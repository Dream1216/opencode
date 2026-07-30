import { createHash, timingSafeEqual } from "node:crypto"
import { Cause, Effect, Exit } from "effect"
import type { Sql } from "postgres"
import { createSecretManager } from "../../security/secret-manager"
import { setTenantContext } from "./client"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  AuthenticationError,
  Service,
  layerFromEnv,
  type SignedRequest,
} from "./worker-queue-admin"

export type WorkerQueueLiveIntegrationResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
  readonly provider: "oidc" | "better-auth"
  readonly secretManagerScheme: "aws-sm" | "file"
  readonly identityEndpointClass: "remote" | "loopback"
}

export type WorkerQueueLiveIntegrationPolicy = {
  readonly provider: "oidc" | "better-auth"
  readonly credentialRef: string
  readonly secretManagerScheme: "aws-sm" | "file"
  readonly identityEndpointClass: "remote" | "loopback"
  readonly cloudRequired: boolean
}

export function liveIntegrationPolicyFromEnv(env: NodeJS.ProcessEnv): WorkerQueueLiveIntegrationPolicy {
  const provider = identityProvider(env.OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER)
  const credentialRef = required(
    env.OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF,
    "OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF",
  )
  const secretManagerScheme = secretScheme(credentialRef)
  const allowedSchemes = new Set(
    (env.OPENCODE_P4_41_LIVE_SECRET_SCHEMES ?? "aws-sm,file")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  )
  if (
    (secretManagerScheme !== "aws-sm" && secretManagerScheme !== "file") ||
    !allowedSchemes.has(secretManagerScheme)
  ) {
    throw new Error("P4.41 live credentials must come from an allowed aws-sm:// or file:// Secret Manager reference")
  }
  const endpoint = new URL(
    required(
      provider === "oidc"
        ? env.OPENCODE_WORKER_QUEUE_OIDC_ISSUER
        : env.OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL,
      provider === "oidc"
        ? "OPENCODE_WORKER_QUEUE_OIDC_ISSUER"
        : "OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL",
    ),
  )
  const loopback =
    endpoint.hostname === "127.0.0.1" ||
    endpoint.hostname === "::1" ||
    endpoint.hostname === "localhost"
  const identityEndpointClass =
    endpoint.protocol === "https:" && !loopback ? "remote" as const : "loopback" as const
  const cloudRequired = env.OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED === "1"
  if (
    cloudRequired &&
    (provider !== "oidc" || secretManagerScheme !== "aws-sm" || identityEndpointClass !== "remote")
  ) {
    throw new Error("P4.42 cloud integration requires remote HTTPS OIDC and aws-sm:// credentials")
  }
  return { provider, credentialRef, secretManagerScheme, identityEndpointClass, cloudRequired }
}

export async function runWorkerQueueLiveIntegration(
  sql: Sql,
  input: {
    readonly url: string
    readonly env?: NodeJS.ProcessEnv
  },
): Promise<WorkerQueueLiveIntegrationResult> {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const sourceEnv = input.env ?? process.env
  const policy = liveIntegrationPolicyFromEnv(sourceEnv)
  const provider = policy.provider
  const actorID = required(sourceEnv.OPENCODE_P4_41_LIVE_ACTOR_ID, "OPENCODE_P4_41_LIVE_ACTOR_ID")
  const secretManager = createSecretManager({ env: sourceEnv, cacheTtlMs: 0 })
  const firstCredential = await secretManager.resolve(policy.credentialRef, { refresh: true })
  const secondCredential = await secretManager.resolve(policy.credentialRef, { refresh: true })
  if (!equalDigest(firstCredential, secondCredential)) {
    throw new Error("Live Secret Manager returned inconsistent identity credentials")
  }
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_live_${suffix}`
  const teamID = `team_worker_live_${suffix}`
  const checks: string[] = []
  await seed(sql, tenantID, teamID, actorID)
  try {
    const env = {
      ...sourceEnv,
      OPENCODE_DATABASE_BACKEND: "postgres-alpha",
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_TENANT_ID: tenantID,
      OPENCODE_TEAM_ID: teamID,
      OPENCODE_ACTOR_ID: "p4-41-live-verifier",
      OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED: "1",
      OPENCODE_WORKER_QUEUE_IDENTITY_MODE: provider,
      OPENCODE_WORKER_QUEUE_RATE_LIMITS: JSON.stringify({
        observe: { limit: 20, windowMs: 60_000 },
        recover: { limit: 20, windowMs: 60_000 },
        mutate: { limit: 20, windowMs: 60_000 },
        approval: { limit: 20, windowMs: 60_000 },
        "break-glass": { limit: 2, windowMs: 60_000 },
      }),
    }
    await Effect.runPromise(
      Effect.gen(function* () {
        const admin = yield* Service
        const principal = yield* admin.authenticate(externalRequest(provider, actorID, firstCredential, "allow"))
        if (
          principal.actorID !== actorID ||
          principal.identityProvider !== provider ||
          principal.teamRole !== "viewer"
        ) {
          return yield* Effect.die("Live identity was not bound to server-derived PostgreSQL membership")
        }
        const mismatched = yield* admin
          .authenticate(externalRequest(provider, `${actorID}:mismatch`, secondCredential, "mismatch"))
          .pipe(Effect.exit)
        expectFailure(mismatched, AuthenticationError)
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )
    const outcomes = await readAuditOutcomes(sql, tenantID)
    if (!outcomes.includes("allow") || !outcomes.includes("deny")) {
      throw new Error("Live identity allow and mismatch denial were not independently audited")
    }
    checks.push(`${provider}-live-identity-verified`)
    checks.push(`${policy.secretManagerScheme}-secret-manager-live-refresh-verified`)
    checks.push(`${policy.identityEndpointClass}-identity-endpoint-policy-verified`)
    checks.push("live-identity-bound-to-postgres-membership")
    checks.push("live-identity-mismatch-denied-and-audited")
    return {
      status: "ok",
      checks,
      provider,
      secretManagerScheme: policy.secretManagerScheme,
      identityEndpointClass: policy.identityEndpointClass,
    }
  } finally {
    await cleanup(sql, tenantID)
  }
}

function externalRequest(
  provider: "oidc" | "better-auth",
  actorID: string,
  credential: string,
  nonce: string,
): SignedRequest {
  return {
    method: "GET",
    target: "/experimental/worker-queue/readiness",
    actorID,
    timestamp: Date.now(),
    nonce: `p4_41_${nonce}_${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    signature: "",
    identityProvider: provider,
    ...(provider === "oidc" ? { identityToken: credential } : { sessionCookie: credential }),
    body: "",
  }
}

function expectFailure<A>(exit: Exit.Exit<A, never>, expected: abstract new (...args: any[]) => Error) {
  if (Exit.isSuccess(exit)) throw new Error(`Expected ${expected.name}, received success`)
  const error = Cause.squash(exit.cause)
  if (!(error instanceof expected)) {
    throw new Error(`Expected ${expected.name}, received ${error instanceof Error ? error.name : String(error)}`)
  }
}

async function seed(sql: Sql, tenantID: string, teamID: string, actorID: string) {
  const now = Date.now()
  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values (${tenantID}, ${tenantID}, ${now}, ${now})
  `
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`
      insert into tenant_member (tenant_id, actor_id, role, time_created)
      values (${tenantID}, ${actorID}, 'viewer', ${now})
    `
    await tx`
      insert into team_member (tenant_id, team_id, actor_id, role, time_created)
      values (${tenantID}, ${teamID}, ${actorID}, 'viewer', ${now})
    `
  })
}

async function readAuditOutcomes(sql: Sql, tenantID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    const rows = await tx<{ outcome: string }[]>`
      select distinct outcome from worker_queue_identity_audit
    `
    return rows.map((row) => row.outcome)
  })
}

async function cleanup(sql: Sql, tenantID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`delete from worker_queue_identity_audit`
    await tx`delete from worker_queue_api_rate_limit`
    await tx`delete from worker_queue_api_nonce`
    await tx`delete from team_member`
    await tx`delete from tenant_member`
  })
  await sql`delete from tenant where id = ${tenantID}`
}

function identityProvider(value: string | undefined) {
  if (value !== "oidc" && value !== "better-auth") {
    throw new Error("OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER must be oidc or better-auth")
  }
  return value
}

function secretScheme(reference: string) {
  const index = reference.indexOf("://")
  return index < 0 ? "" : reference.slice(0, index)
}

function equalDigest(left: string, right: string) {
  const a = createHash("sha256").update(left).digest()
  const b = createHash("sha256").update(right).digest()
  return timingSafeEqual(a, b)
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
