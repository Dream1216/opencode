import { createServer, type RequestListener } from "node:http"
import { Cause, Effect, Exit } from "effect"
import type { Sql } from "postgres"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext } from "./client"
import {
  claimNextWorkerJob,
  enqueueWorkerJob,
  failWorkerJobClaim,
} from "./worker-job"
import {
  AuthenticationError,
  AuthorizationError,
  RateLimitError,
  Service,
  layerFromEnv,
  signRequest,
  type SignedRequest,
} from "./worker-queue-admin"

export async function runWorkerQueueAdminGovernanceSmoke(
  sql: Sql,
  input: { readonly url: string },
) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_governance_${suffix}`
  const teamID = `team_worker_governance_${suffix}`
  const actors = {
    viewer: `viewer_${suffix}`,
    operator: `operator_${suffix}`,
    adminA: `admin_a_${suffix}`,
    adminB: `admin_b_${suffix}`,
    owner: `owner_${suffix}`,
  }
  const roles = {
    [actors.viewer]: "viewer",
    [actors.operator]: "operator",
    [actors.adminA]: "admin",
    [actors.adminB]: "admin",
    [actors.owner]: "owner",
  }
  const secrets = Object.fromEntries(
    Object.values(actors).map((actor) => [actor, `governance-secret-${actor}-0123456789`]),
  )
  const breakGlassToken = `break-glass-second-factor-${suffix}-0123456789`
  const runApproval = `ses_governance_approval_${suffix}`
  const runTimeout = `ses_governance_timeout_${suffix}`
  const runBreakGlass = `ses_governance_break_glass_${suffix}`
  const checks: string[] = []
  const betterAuth = await testServer((request, response) => {
    response.setHeader("content-type", "application/json")
    if (request.headers.cookie !== "better-auth.session_token=owner") {
      response.statusCode = 401
      return response.end(JSON.stringify({ error: "unauthorized" }))
    }
    response.end(
      JSON.stringify({
        user: { id: actors.owner, role: "admin" },
        session: { expiresAt: new Date(Date.now() + 60_000).toISOString() },
      }),
    )
  })
  await seed(sql, tenantID, teamID, roles)
  const failedApproval = await failedJob(sql, tenantID, teamID, runApproval)
  const failedTimeout = await failedJob(sql, tenantID, teamID, runTimeout)
  const failedBreakGlass = await failedJob(sql, tenantID, teamID, runBreakGlass)
  try {
    const env = {
      ...process.env,
      OPENCODE_DATABASE_BACKEND: "postgres-alpha",
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_TENANT_ID: tenantID,
      OPENCODE_TEAM_ID: teamID,
      OPENCODE_ACTOR_ID: actors.operator,
      OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED: "1",
      OPENCODE_WORKER_QUEUE_IDENTITY_MODE: "hybrid",
      OPENCODE_WORKER_QUEUE_ADMIN_KEYS: JSON.stringify(secrets),
      OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL: betterAuth.url,
      OPENCODE_WORKER_QUEUE_BREAK_GLASS_ENABLED: "1",
      OPENCODE_WORKER_QUEUE_BREAK_GLASS_SECRET_REF: "env://BREAK_GLASS_SECOND_FACTOR",
      BREAK_GLASS_SECOND_FACTOR: breakGlassToken,
      OPENCODE_WORKER_QUEUE_RATE_LIMITS: JSON.stringify({
        observe: { limit: 2, windowMs: 60_000 },
        recover: { limit: 50, windowMs: 60_000 },
        mutate: { limit: 50, windowMs: 60_000 },
        approval: { limit: 50, windowMs: 60_000 },
        "break-glass": { limit: 10, windowMs: 60_000 },
      }),
    }
    await Effect.runPromise(
      Effect.gen(function* () {
        const admin = yield* Service
        yield* admin.authenticate(signed(secrets, actors.viewer, "GET", "/experimental/worker-queue/readiness", "rate-1"))
        yield* admin.authenticate(signed(secrets, actors.viewer, "GET", "/experimental/worker-queue/readiness", "rate-2"))
        const throttled = yield* admin
          .authenticate(signed(secrets, actors.viewer, "GET", "/experimental/worker-queue/readiness", "rate-3"))
          .pipe(Effect.exit)
        expectFailure(throttled, RateLimitError)
        checks.push("postgres-distributed-rate-limit-enforced")
        const invalid = signed(
          secrets,
          actors.operator,
          "GET",
          "/experimental/worker-queue/recoverable",
          "invalid-signature",
        )
        const denied = yield* admin
          .authenticate({ ...invalid, signature: "0".repeat(64) })
          .pipe(Effect.exit)
        expectFailure(denied, AuthenticationError)

        const operator = yield* admin.authenticate(
          signed(secrets, actors.operator, "POST", "/experimental/worker-queue/actions/requeue", "operator"),
        )
        const action = yield* admin.requestRequeue(operator, {
          runID: runApproval,
          expectedGeneration: failedApproval.requestedGeneration,
          expectedClaimToken: failedApproval.claimToken,
        })
        const adminA = yield* admin.authenticate(
          signed(
            secrets,
            actors.adminA,
            "POST",
            `/experimental/worker-queue/actions/${action.id}/approve`,
            "admin-a",
          ),
        )
        const first = yield* admin.approve(adminA, action.id)
        if (first.approvalCount !== 1 || first.status !== "pending") {
          return yield* Effect.die("First approval was not pending")
        }
        const revoked = yield* admin.revokeApproval(
          adminA,
          action.id,
          actors.adminA,
          "Approver withdrew consent after incident review",
        )
        if (revoked.approvalCount !== 0 || revoked.status !== "pending") {
          return yield* Effect.die("Approval revocation did not update active approval state")
        }
        yield* admin.approve(adminA, action.id)
        const adminB = yield* admin.authenticate(
          signed(
            secrets,
            actors.adminB,
            "POST",
            `/experimental/worker-queue/actions/${action.id}/approve`,
            "admin-b",
          ),
        )
        const executed = yield* admin.approve(adminB, action.id)
        if (executed.status !== "executed" || executed.approvalCount !== 2) {
          return yield* Effect.die("Re-approved action did not execute")
        }
        checks.push("approval-revoke-reapprove-lifecycle-verified")

        const timeoutAction = yield* admin.requestRequeue(operator, {
          runID: runTimeout,
          expectedGeneration: failedTimeout.requestedGeneration,
          expectedClaimToken: failedTimeout.claimToken,
        })
        yield* Effect.promise(() => forceExpired(sql, tenantID, timeoutAction.id))
        const expired = yield* admin.action(adminA, timeoutAction.id)
        if (expired.status !== "expired") return yield* Effect.die("Expired approval action remained pending")
        checks.push("approval-timeout-transition-audited")

        const hmacOwner = yield* admin.authenticate(
          signed(
            secrets,
            actors.owner,
            "POST",
            "/experimental/worker-queue/actions/break-glass/requeue",
            "hmac-owner",
          ),
        )
        const hmacRejected = yield* admin
          .breakGlassRequeue(hmacOwner, {
            runID: runBreakGlass,
            expectedGeneration: failedBreakGlass.requestedGeneration,
            expectedClaimToken: failedBreakGlass.claimToken,
            incidentID: `incident_hmac_${suffix}`,
            reason: "Emergency recovery requested with insufficient identity assurance",
            token: breakGlassToken,
          })
          .pipe(Effect.exit)
        expectFailure(hmacRejected, AuthorizationError)
        const externalOwner = yield* admin.authenticate(
          externalOwnerRequest("break-glass-owner", "better-auth.session_token=owner"),
        )
        const invalidFactor = yield* admin
          .breakGlassRequeue(externalOwner, {
            runID: runBreakGlass,
            expectedGeneration: failedBreakGlass.requestedGeneration,
            expectedClaimToken: failedBreakGlass.claimToken,
            incidentID: `incident_invalid_${suffix}`,
            reason: "Emergency recovery requested during a declared production incident",
            token: "invalid-second-factor",
          })
          .pipe(Effect.exit)
        expectFailure(invalidFactor, AuthenticationError)
        const emergency = yield* admin.breakGlassRequeue(externalOwner, {
          runID: runBreakGlass,
          expectedGeneration: failedBreakGlass.requestedGeneration,
          expectedClaimToken: failedBreakGlass.claimToken,
          incidentID: `incident_valid_${suffix}`,
          reason: "Emergency recovery requested during a declared production incident",
          token: breakGlassToken,
        })
        if (emergency.status !== "pending") return yield* Effect.die("Break-glass did not requeue the failed job")
        checks.push("break-glass-owner-external-identity-and-second-factor-enforced")
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )
    const evidence = await readEvidence(sql, tenantID)
    if (!evidence.identityOutcomes.includes("allow") || !evidence.identityOutcomes.includes("deny") || !evidence.identityOutcomes.includes("throttle")) {
      throw new Error("Identity audit did not capture allow, deny, and throttle outcomes")
    }
    checks.push("complete-identity-audit-outcomes-recorded")
    if (!evidence.approvalEvents.includes("approve") || !evidence.approvalEvents.includes("revoke") || !evidence.approvalEvents.includes("expire")) {
      throw new Error("Approval lifecycle append-only evidence is incomplete")
    }
    checks.push("approval-append-only-evidence-recorded")
    if (evidence.breakGlassCount !== 1 || evidence.breakGlassAuditCount < 3) {
      throw new Error("Break-glass execution and denied attempts were not fully audited")
    }
    checks.push("break-glass-execution-and-denials-audited")
    return { status: "ok" as const, checks }
  } finally {
    await cleanup(sql, tenantID)
    await betterAuth.close()
  }
}

async function failedJob(sql: Sql, tenantID: string, teamID: string, runID: string) {
  const tenant = { tenantID, teamID, actorID: "governance-fixture" }
  await enqueueWorkerJob(sql, { tenant, runID, reason: "wake" })
  const claim = await claimNextWorkerJob(sql, {
    tenant,
    runID,
    ownerID: "governance-fixture",
    claimMs: 5_000,
  })
  if (claim === undefined) throw new Error("Governance fixture was not claimed")
  return await failWorkerJobClaim(sql, {
    claim,
    error: "governance fixture terminal failure",
    maxAttempts: 1,
    retryDelayMs: 0,
  })
}

function signed(
  secrets: Record<string, string>,
  actorID: string,
  method: string,
  target: string,
  nonce: string,
): SignedRequest {
  const request = {
    method,
    target,
    actorID,
    timestamp: Date.now(),
    nonce: `${nonce}-${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    body: "",
  }
  return { ...request, signature: signRequest({ ...request, secret: secrets[actorID]! }) }
}

function externalOwnerRequest(nonce: string, cookie: string): SignedRequest {
  return {
    method: "POST",
    target: "/experimental/worker-queue/actions/break-glass/requeue",
    actorID: "",
    timestamp: Date.now(),
    nonce: `${nonce}-${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    signature: "",
    identityProvider: "better-auth",
    sessionCookie: cookie,
    body: "",
  }
}

function expectFailure<A>(exit: Exit.Exit<A, never>, expected: abstract new (...args: any[]) => Error) {
  if (Exit.isSuccess(exit)) throw new Error(`Expected ${expected.name}, received success`)
  const error = Cause.squash(exit.cause)
  if (!(error instanceof expected)) {
    const detail = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
    throw new Error(`Expected ${expected.name}, received ${detail}`)
  }
}

async function seed(
  sql: Sql,
  tenantID: string,
  teamID: string,
  roles: Record<string, string>,
) {
  const now = Date.now()
  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values (${tenantID}, ${tenantID}, ${now}, ${now})
  `
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    for (const [actorID, role] of Object.entries(roles)) {
      await tx`
        insert into tenant_member (tenant_id, actor_id, role, time_created)
        values (${tenantID}, ${actorID}, ${role}, ${now})
      `
      await tx`
        insert into team_member (tenant_id, team_id, actor_id, role, time_created)
        values (${tenantID}, ${teamID}, ${actorID}, ${role}, ${now})
      `
    }
  })
}

async function forceExpired(sql: Sql, tenantID: string, actionID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`
      update worker_queue_action
      set time_expires = ${Date.now() - 1}
      where id = ${actionID}
    `
  })
}

async function readEvidence(sql: Sql, tenantID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    const identities = await tx<{ outcome: string }[]>`
      select distinct outcome from worker_queue_identity_audit
    `
    const approvals = await tx<{ event: string }[]>`
      select distinct event from worker_queue_action_approval_event
    `
    const breakGlass = await tx<{ count: string | number }[]>`
      select count(*) as count from worker_queue_break_glass
    `
    const breakGlassAudit = await tx<{ count: string | number }[]>`
      select count(*) as count
      from audit_event
      where action in ('worker_queue.break_glass.attempt', 'worker_queue.break_glass.requeue')
    `
    return {
      identityOutcomes: identities.map((row) => row.outcome),
      approvalEvents: approvals.map((row) => row.event),
      breakGlassCount: Number(breakGlass[0]?.count ?? 0),
      breakGlassAuditCount: Number(breakGlassAudit[0]?.count ?? 0),
    }
  })
}

async function cleanup(sql: Sql, tenantID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`delete from worker_queue_break_glass`
    await tx`delete from worker_queue_action_approval_event`
    await tx`delete from worker_queue_identity_audit`
    await tx`delete from worker_queue_api_rate_limit`
    await tx`delete from worker_queue_api_nonce`
    await tx`delete from worker_queue_action_approval`
    await tx`delete from worker_queue_action`
    await tx`delete from worker_job`
    await tx`delete from audit_event`
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
  if (address === null || typeof address === "string") throw new Error("Governance smoke server did not bind TCP")
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error === undefined ? resolve() : reject(error)))
        server.closeAllConnections()
      }),
  }
}
