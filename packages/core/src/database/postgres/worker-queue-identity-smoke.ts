import { createServer, type RequestListener } from "node:http"
import { generateKeyPairSync, sign as cryptoSign } from "node:crypto"
import { Cause, Effect, Exit } from "effect"
import type { Sql } from "postgres"
import { createSecretManager } from "../../security/secret-manager"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext } from "./client"
import {
  AuthenticationError,
  AuthorizationError,
  Service,
  layerFromEnv,
  signRequest,
  type SignedRequest,
} from "./worker-queue-admin"
import {
  identityAdapterFromEnv,
  WorkerQueueIdentityError,
} from "./worker-queue-identity"

export async function runWorkerQueueIdentitySmoke(sql: Sql, input: { readonly url: string }) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const checks: string[] = []
  await verifySecretManagerAndRotation(checks)

  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_identity_${suffix}`
  const teamID = `team_worker_identity_${suffix}`
  const actors = {
    oidc: `oidc_${suffix}`,
    betterAuth: `better_auth_${suffix}`,
    nonmember: `nonmember_${suffix}`,
  }
  const audience = "opencode-worker-queue"
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 })
  const kid = `kid_${suffix}`
  const jwk = publicKey.export({ format: "jwk" })
  let issuer = ""
  const idp = await testServer((request, response) => {
    response.setHeader("content-type", "application/json")
    if (request.url === "/.well-known/openid-configuration") {
      return response.end(JSON.stringify({ issuer, jwks_uri: `${issuer}/jwks` }))
    }
    if (request.url === "/jwks") {
      return response.end(JSON.stringify({ keys: [{ ...jwk, kid, alg: "RS256", use: "sig" }] }))
    }
    response.statusCode = 404
    response.end()
  })
  issuer = idp.url
  const betterAuth = await testServer((request, response) => {
    const cookie = request.headers.cookie
    const actorID = cookie === "better-auth.session_token=valid" ? actors.betterAuth : undefined
    response.setHeader("content-type", "application/json")
    if (actorID === undefined) {
      response.statusCode = 401
      return response.end(JSON.stringify({ error: "unauthorized" }))
    }
    response.end(
      JSON.stringify({
        user: { id: actorID, role: "user" },
        session: { expiresAt: new Date(Date.now() + 60_000).toISOString() },
      }),
    )
  })
  await seed(sql, tenantID, teamID, [actors.oidc, actors.betterAuth])
  try {
    const env = {
      ...process.env,
      OPENCODE_DATABASE_BACKEND: "postgres-alpha",
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_TENANT_ID: tenantID,
      OPENCODE_TEAM_ID: teamID,
      OPENCODE_ACTOR_ID: "identity-system",
      OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED: "1",
      OPENCODE_WORKER_QUEUE_IDENTITY_MODE: "hybrid",
      OPENCODE_WORKER_QUEUE_OIDC_ISSUER: issuer,
      OPENCODE_WORKER_QUEUE_OIDC_AUDIENCE: audience,
      OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL: betterAuth.url,
    }
    const oidcToken = jwt(privateKey, kid, {
      iss: issuer,
      aud: audience,
      sub: actors.oidc,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 60,
    })
    await Effect.runPromise(
      Effect.gen(function* () {
        const admin = yield* Service
        const oidcRequest = externalRequest("oidc", "oidc-member", { identityToken: oidcToken })
        const oidc = yield* admin.authenticate(oidcRequest)
        if (oidc.actorID !== actors.oidc || oidc.identityProvider !== "oidc" || oidc.teamRole !== "viewer") {
          return yield* Effect.die("OIDC identity was not bound to server-derived membership")
        }
        checks.push("oidc-jwks-token-and-membership-verified")
        const replay = yield* admin.authenticate(oidcRequest).pipe(Effect.exit)
        expectFailure(replay, AuthenticationError)
        checks.push("external-identity-nonce-replay-rejected")

        const better = yield* admin.authenticate(
          externalRequest("better-auth", "better-auth-member", {
            sessionCookie: "better-auth.session_token=valid",
          }),
        )
        if (
          better.actorID !== actors.betterAuth ||
          better.identityProvider !== "better-auth" ||
          better.teamRole !== "viewer"
        ) {
          return yield* Effect.die("Better Auth identity was not bound to server-derived membership")
        }
        checks.push("better-auth-session-and-membership-verified")

        const wrongAudience = jwt(privateKey, kid, {
          iss: issuer,
          aud: "wrong-audience",
          sub: actors.oidc,
          exp: Math.floor(Date.now() / 1000) + 60,
        })
        const invalid = yield* admin
          .authenticate(externalRequest("oidc", "wrong-audience", { identityToken: wrongAudience }))
          .pipe(Effect.exit)
        expectFailure(invalid, AuthenticationError)
        checks.push("oidc-invalid-audience-rejected")

        const nonmemberToken = jwt(privateKey, kid, {
          iss: issuer,
          aud: audience,
          sub: actors.nonmember,
          exp: Math.floor(Date.now() / 1000) + 60,
        })
        const nonmember = yield* admin
          .authenticate(externalRequest("oidc", "nonmember", { identityToken: nonmemberToken }))
          .pipe(Effect.exit)
        expectFailure(nonmember, AuthorizationError)
        checks.push("external-identity-nonmember-rejected")
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )
    return { status: "ok" as const, checks }
  } finally {
    await cleanup(sql, tenantID)
    await idp.close()
    await betterAuth.close()
  }
}

async function verifySecretManagerAndRotation(checks: string[]) {
  const env: NodeJS.ProcessEnv = {
    KEYRING: "",
    ACTIVE_SECRET: "active-secret-0123456789",
    RETIRING_SECRET: "retiring-secret-0123456789",
    NEXT_SECRET: "next-secret-0123456789",
  }
  const secretManager = createSecretManager({
    env,
    cacheTtlMs: 0,
    fetchAwsSecret: async () => JSON.stringify({ value: "aws-secret-0123456789" }),
  })
  if (
    (await secretManager.resolve("env://ACTIVE_SECRET")) !== env.ACTIVE_SECRET ||
    (await secretManager.resolve("aws-sm://us-east-1/opencode-key#value")) !== "aws-secret-0123456789"
  ) {
    throw new Error("Secret Manager adapters did not resolve secrets")
  }
  checks.push("secret-manager-env-and-aws-adapters-verified")
  const actorID = "rotating-actor"
  env.KEYRING = JSON.stringify(
    keyring(actorID, "v2", {
      v1: { secretRef: "env://RETIRING_SECRET", status: "retiring", notAfter: Date.now() + 60_000 },
      v2: { secretRef: "env://ACTIVE_SECRET", status: "active" },
    }),
  )
  const adapter = await identityAdapterFromEnv(
    {
      OPENCODE_WORKER_QUEUE_IDENTITY_MODE: "hmac",
      OPENCODE_WORKER_QUEUE_ADMIN_KEYRING_REF: "env://KEYRING",
    },
    secretManager,
  )
  await adapter.authenticate(signed(actorID, "v2", env.ACTIVE_SECRET!, "active-key"))
  await adapter.authenticate(signed(actorID, "v1", env.RETIRING_SECRET!, "retiring-key"))
  checks.push("actor-active-and-retiring-keys-accepted")
  env.KEYRING = JSON.stringify(
    keyring(actorID, "v2", {
      v1: { secretRef: "env://RETIRING_SECRET", status: "revoked" },
      v2: { secretRef: "env://ACTIVE_SECRET", status: "active" },
    }),
  )
  await expectIdentityRejected(() => adapter.authenticate(signed(actorID, "v1", env.RETIRING_SECRET!, "revoked-key")))
  checks.push("actor-revoked-key-rejected")
  env.KEYRING = JSON.stringify(
    keyring(actorID, "v3", {
      v2: { secretRef: "env://ACTIVE_SECRET", status: "retiring", notAfter: Date.now() + 60_000 },
      v3: { secretRef: "env://NEXT_SECRET", status: "active" },
    }),
  )
  const rotated = await adapter.authenticate(signed(actorID, "v3", env.NEXT_SECRET!, "rotated-key"))
  if (rotated.keyID !== "v3") throw new Error("Actor keyring did not refresh to the new active key")
  checks.push("actor-keyring-hot-rotation-verified")
}

function keyring(
  actorID: string,
  activeKeyID: string,
  keys: Record<string, { secretRef: string; status: string; notAfter?: number }>,
) {
  return { version: 1, actors: { [actorID]: { activeKeyID, keys } } }
}

function signed(actorID: string, keyID: string, secret: string, nonce: string): SignedRequest {
  const request = {
    method: "GET",
    target: "/experimental/worker-queue/readiness",
    actorID,
    timestamp: Date.now(),
    nonce: `${nonce}-0123456789`,
    keyID,
    body: "",
  }
  return { ...request, signature: signRequest({ ...request, secret }) }
}

function externalRequest(
  provider: "oidc" | "better-auth",
  nonce: string,
  credential: { identityToken?: string; sessionCookie?: string },
): SignedRequest {
  return {
    method: "GET",
    target: "/experimental/worker-queue/readiness",
    actorID: "",
    timestamp: Date.now(),
    nonce: `${nonce}-${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    signature: "",
    identityProvider: provider,
    ...credential,
    body: "",
  }
}

function jwt(
  privateKey: ReturnType<typeof generateKeyPairSync>["privateKey"],
  kid: string,
  payload: Record<string, unknown>,
) {
  const header = Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT", kid })).toString("base64url")
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url")
  const input = `${header}.${body}`
  return `${input}.${cryptoSign("RSA-SHA256", Buffer.from(input), privateKey).toString("base64url")}`
}

function expectFailure<A>(exit: Exit.Exit<A, never>, expected: abstract new (...args: any[]) => Error) {
  if (Exit.isSuccess(exit)) throw new Error(`Expected ${expected.name}, received success`)
  const error = Cause.squash(exit.cause)
  if (!(error instanceof expected)) throw new Error(`Expected ${expected.name}`)
}

async function expectIdentityRejected(run: () => Promise<unknown>) {
  try {
    await run()
  } catch (error) {
    if (error instanceof WorkerQueueIdentityError) return
    throw error
  }
  throw new Error("Expected identity operation to be rejected")
}

async function seed(sql: Sql, tenantID: string, teamID: string, actorIDs: readonly string[]) {
  const now = Date.now()
  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values (${tenantID}, ${tenantID}, ${now}, ${now})
    on conflict (id) do nothing
  `
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    for (const actorID of actorIDs) {
      await tx`
        insert into tenant_member (tenant_id, actor_id, role, time_created)
        values (${tenantID}, ${actorID}, 'viewer', ${now})
      `
      await tx`
        insert into team_member (tenant_id, team_id, actor_id, role, time_created)
        values (${tenantID}, ${teamID}, ${actorID}, 'viewer', ${now})
      `
    }
  })
}

async function cleanup(sql: Sql, tenantID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`delete from worker_queue_api_nonce`
    await tx`delete from team_member`
    await tx`delete from tenant_member`
  })
  await sql`delete from tenant where id = ${tenantID}`
}

async function testServer(listener: RequestListener) {
  const server = createServer(listener)
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject)
      resolve()
    })
  })
  const address = server.address()
  if (address === null || typeof address === "string") throw new Error("Identity smoke server did not bind TCP")
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error === undefined ? resolve() : reject(error)))
        server.closeAllConnections()
      }),
  }
}
