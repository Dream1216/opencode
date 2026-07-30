import type { Sql } from "postgres"
import { DatabaseBackend } from "../backend"
import { makeClient } from "./client"
import { assertRlsReady } from "./migration"
import {
  databaseFingerprint,
  loadAndVerifyReleaseProof,
  postgresSchemaHash,
} from "./release-proof"
import { postgresSaasReleaseProofIssues } from "./release-gate"

export type AlphaStartupEvaluation = {
  readonly allowed: boolean
  readonly reasons: readonly string[]
}

export type AlphaStartupResult = {
  readonly status: "ready"
  readonly mode: "sqlite-primary-postgres-alpha-sidecar"
  readonly role: string
  readonly schema: string
  readonly proof:
    | { readonly status: "not-required" }
    | { readonly status: "verified"; readonly digest: string; readonly generatedAt: number; readonly expiresAt: number }
  readonly capabilities: {
    readonly eventV2Facade: true
    readonly sessionProjectionDryRun: true
    readonly sessionProjectionShadow: true
    readonly eventReplicationOutbox: boolean
    readonly projector: false
    readonly stream: false
    readonly sessionProjection: false
  }
}

type RuntimeRoleRow = {
  role_name: string
  schema_name: string | null
  role_is_superuser: boolean
  role_bypasses_rls: boolean
}

export const alphaCapabilities = {
  eventV2Facade: true,
  sessionProjectionDryRun: true,
  sessionProjectionShadow: true,
  projector: false,
  stream: false,
  sessionProjection: false,
} as const

export function evaluateAlphaStartup(
  config: DatabaseBackend.PostgresAlphaConfig,
  env: NodeJS.ProcessEnv = process.env,
): AlphaStartupEvaluation {
  const reasons: string[] = []
  if (config.url === undefined || config.url.trim() === "") reasons.push("OPENCODE_DATABASE_URL is required")
  if (!config.requireRls) reasons.push("OPENCODE_POSTGRES_REQUIRE_RLS must not disable RLS")
  if (config.tenantID === undefined || config.tenantID.trim() === "") reasons.push("OPENCODE_TENANT_ID is required")
  if (config.actorID === undefined || config.actorID.trim() === "") reasons.push("OPENCODE_ACTOR_ID is required")
  if (enabled(env.OPENCODE_SAAS_RELEASE) && !config.dualWriteEnabled) {
    reasons.push("SaaS PostgreSQL alpha startup requires OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED=1")
  }
  if (config.projectorEnabled) reasons.push("PostgreSQL alpha projector integration is unsupported")
  if (config.streamEnabled) reasons.push("PostgreSQL alpha stream subscription is unsupported")
  if (config.sessionProjectionEnabled) reasons.push("PostgreSQL alpha production session projection is unsupported")
  return { allowed: reasons.length === 0, reasons }
}

export async function assertAlphaStartup(
  config: DatabaseBackend.PostgresAlphaConfig,
  env: NodeJS.ProcessEnv = process.env,
): Promise<AlphaStartupResult> {
  const evaluation = evaluateAlphaStartup(config, env)
  if (!evaluation.allowed) throw new Error(`PostgreSQL alpha startup blocked: ${evaluation.reasons.join("; ")}`)

  const sql = makeClient({
    url: config.url!,
    max: 1,
    connectTimeoutSeconds: Number(env.OPENCODE_POSTGRES_CONNECT_TIMEOUT_SECONDS ?? 10),
    idleTimeoutSeconds: Number(env.OPENCODE_POSTGRES_IDLE_TIMEOUT_SECONDS ?? 5),
  })
  try {
    await assertRlsReady(sql)
    const role = await runtimeRole(sql)
    if (role.role_is_superuser) throw new Error(`PostgreSQL alpha startup blocked: runtime role ${role.role_name} is superuser`)
    if (role.role_bypasses_rls) {
      throw new Error(`PostgreSQL alpha startup blocked: runtime role ${role.role_name} has BYPASSRLS`)
    }
    const proof = await verifyStartupProof(config, role, env)
    return {
      status: "ready",
      mode: "sqlite-primary-postgres-alpha-sidecar",
      role: role.role_name,
      schema: role.schema_name ?? "public",
      proof,
      capabilities: {
        ...alphaCapabilities,
        eventReplicationOutbox: config.dualWriteEnabled,
      },
    }
  } finally {
    await sql.end({ timeout: 5 })
  }
}

async function verifyStartupProof(
  config: DatabaseBackend.PostgresAlphaConfig,
  role: RuntimeRoleRow,
  env: NodeJS.ProcessEnv,
): Promise<AlphaStartupResult["proof"]> {
  if (!enabled(env.OPENCODE_SAAS_RELEASE)) return { status: "not-required" }
  const path = required(env.OPENCODE_POSTGRES_RELEASE_PROOF_PATH, "OPENCODE_POSTGRES_RELEASE_PROOF_PATH")
  const key = required(env.OPENCODE_POSTGRES_RELEASE_PROOF_KEY, "OPENCODE_POSTGRES_RELEASE_PROOF_KEY")
  const environment = required(env.OPENCODE_RELEASE_ENV, "OPENCODE_RELEASE_ENV")
  const buildID = required(env.OPENCODE_BUILD_ID, "OPENCODE_BUILD_ID")
  const artifact = await loadAndVerifyReleaseProof(path, {
    environment,
    buildID,
    databaseFingerprint: databaseFingerprint(config.url!),
    databaseSchema: role.schema_name ?? "public",
    databaseRole: role.role_name,
    schemaHash: postgresSchemaHash(),
    key,
  })
  const saasIssues = postgresSaasReleaseProofIssues(artifact.payload.proof)
  if (saasIssues.length > 0) {
    throw new Error(`PostgreSQL alpha startup blocked: ${saasIssues.join("; ")}`)
  }
  return {
    status: "verified",
    digest: artifact.digest,
    generatedAt: artifact.payload.generatedAt,
    expiresAt: artifact.payload.expiresAt,
  }
}

async function runtimeRole(sql: Sql) {
  const rows = await sql<RuntimeRoleRow[]>`
    select
      current_user as role_name,
      current_schema() as schema_name,
      r.rolsuper as role_is_superuser,
      r.rolbypassrls as role_bypasses_rls
    from pg_roles r
    where r.rolname = current_user
  `
  const role = rows[0]
  if (role === undefined) throw new Error("PostgreSQL alpha startup blocked: current runtime role was not found")
  return role
}

function enabled(value: string | undefined) {
  return value === "1" || value === "true"
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`PostgreSQL alpha startup blocked: ${name} is required`)
  return value
}
