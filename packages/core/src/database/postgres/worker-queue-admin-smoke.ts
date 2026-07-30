import { Cause, Effect, Exit } from "effect"
import type { Sql } from "postgres"
import { setTenantContext } from "./client"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  claimNextWorkerJob,
  cleanupWorkerJob,
  completeWorkerJobClaim,
  enqueueWorkerJob,
  failWorkerJobClaim,
  readWorkerJob,
} from "./worker-job"
import {
  AuthenticationError,
  AuthorizationError,
  Service,
  layerFromEnv,
  signRequest,
  type SignedRequest,
} from "./worker-queue-admin"
import { workerQueueTelemetrySnapshot } from "./worker-queue-telemetry"

export async function runWorkerQueueAdminSmoke(sql: Sql, input: { readonly url: string }) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_queue_admin_${suffix}`
  const teamID = `team_worker_queue_admin_${suffix}`
  const actors = {
    viewer: `viewer_${suffix}`,
    operator: `operator_${suffix}`,
    adminA: `admin_a_${suffix}`,
    adminB: `admin_b_${suffix}`,
    intruder: `intruder_${suffix}`,
  }
  const secrets = Object.fromEntries(
    Object.values(actors).map((actor) => [actor, `secret-${actor}-0123456789`]),
  )
  const runID = `ses_worker_queue_admin_${suffix}`
  const checks: string[] = []
  await seed(sql, tenantID, teamID, actors)
  try {
    const tenant = { tenantID, actorID: actors.operator, teamID }
    await enqueueWorkerJob(sql, { tenant, runID, reason: "wake" })
    const claim = await claimNextWorkerJob(sql, {
      tenant,
      runID,
      ownerID: "worker-queue-admin-fixture",
      claimMs: 5_000,
    })
    if (claim === undefined) throw new Error("Worker queue admin fixture was not claimed")
    const failed = await failWorkerJobClaim(sql, {
      claim,
      error: "worker queue admin recovery fixture",
      maxAttempts: 1,
      retryDelayMs: 0,
    })
    const env = {
      ...process.env,
      OPENCODE_DATABASE_BACKEND: "postgres-alpha",
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_TENANT_ID: tenantID,
      OPENCODE_TEAM_ID: teamID,
      OPENCODE_ACTOR_ID: actors.operator,
      OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED: "1",
      OPENCODE_WORKER_QUEUE_ADMIN_KEYS: JSON.stringify(secrets),
      OPENCODE_WORKER_QUEUE_REQUIRED_APPROVALS: "2",
    }
    await Effect.runPromise(
      Effect.gen(function* () {
        const admin = yield* Service
        if (!admin.enabled) return yield* Effect.die("Worker queue admin should be enabled")
        const viewer = yield* admin.authenticate(
          signed(secrets, actors.viewer, "GET", "/experimental/worker-queue/readiness", "", "viewer-ready"),
        )
        const readiness = yield* admin.readiness(viewer)
        if (!readiness.degraded || readiness.metrics.failed !== 1) {
          return yield* Effect.die("Worker queue admin readiness did not expose failed state")
        }
        checks.push("signed-member-readiness-authorized")
        const viewerDenied = yield* admin
          .requestRequeue(viewer, {
            runID,
            expectedGeneration: failed.requestedGeneration,
            expectedClaimToken: failed.claimToken,
          })
          .pipe(Effect.exit)
        expectFailure(viewerDenied, AuthorizationError)
        checks.push("viewer-requeue-forbidden")
        const badSignature = signed(
          secrets,
          actors.operator,
          "POST",
          "/experimental/worker-queue/actions/requeue",
          "{}",
          "bad-signature",
        )
        const invalid = yield* admin.authenticate({ ...badSignature, signature: "0".repeat(64) }).pipe(Effect.exit)
        expectFailure(invalid, AuthenticationError)
        checks.push("invalid-signature-rejected")
        const operatorRequest = signed(
          secrets,
          actors.operator,
          "POST",
          "/experimental/worker-queue/actions/requeue",
          "{}",
          "operator-request",
        )
        const operator = yield* admin.authenticate(operatorRequest)
        const action = yield* admin.requestRequeue(operator, {
          runID,
          expectedGeneration: failed.requestedGeneration,
          expectedClaimToken: failed.claimToken,
          requestID: `request-${suffix}`,
        })
        if (action.status !== "pending" || action.approvalCount !== 0) {
          return yield* Effect.die("Operator action should await two privileged approvals")
        }
        checks.push("operator-created-pending-action")
        const replay = yield* admin.authenticate(operatorRequest).pipe(Effect.exit)
        expectFailure(replay, AuthenticationError)
        checks.push("signed-request-replay-rejected")
        const adminA = yield* admin.authenticate(
          signed(secrets, actors.adminA, "POST", `/experimental/worker-queue/actions/${action.id}/approve`, "", "admin-a"),
        )
        const firstApproval = yield* admin.approve(adminA, action.id)
        if (firstApproval.status !== "pending" || firstApproval.approvalCount !== 1) {
          return yield* Effect.die("First admin approval should keep the action pending")
        }
        const adminB = yield* admin.authenticate(
          signed(secrets, actors.adminB, "POST", `/experimental/worker-queue/actions/${action.id}/approve`, "", "admin-b"),
        )
        const executed = yield* admin.approve(adminB, action.id)
        if (executed.status !== "executed" || executed.approvalCount !== 2) {
          return yield* Effect.die("Second distinct admin approval did not execute requeue")
        }
        checks.push("two-person-approval-executed")
        const visible = yield* admin.action(operator, action.id)
        if (visible.status !== "executed") return yield* Effect.die("Executed action was not readable")
        const prometheus = yield* admin.prometheus(viewer)
        if (
          !prometheus.includes("opencode_worker_queue_jobs") ||
          !prometheus.includes(`tenant_id="${tenantID}"`) ||
          prometheus.includes(actors.viewer)
        ) {
          return yield* Effect.die("Prometheus output is missing queue metrics or leaked actor identity")
        }
        if (workerQueueTelemetrySnapshot({ tenantID, teamID }) === undefined) {
          return yield* Effect.die("OpenTelemetry queue snapshot was not recorded")
        }
        checks.push("otel-prometheus-metrics-ready")
        const intruder = yield* admin
          .authenticate(
            signed(secrets, actors.intruder, "GET", "/experimental/worker-queue/readiness", "", "intruder"),
          )
          .pipe(Effect.exit)
        expectFailure(intruder, AuthorizationError)
        checks.push("non-member-signed-actor-rejected")
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )
    const requeued = await readWorkerJob(sql, tenant, runID)
    if (requeued?.status !== "pending" || requeued.requestedGeneration !== failed.requestedGeneration + 1) {
      throw new Error("Approved worker queue action did not requeue the next generation")
    }
    const recovered = await claimNextWorkerJob(sql, {
      tenant,
      runID,
      ownerID: "worker-queue-admin-recovered",
      claimMs: 5_000,
    })
    if (recovered === undefined) throw new Error("Approved worker queue action was not claimable")
    await completeWorkerJobClaim(sql, recovered)
    const auditCount = await countAudit(sql, tenantID, runID)
    if (auditCount < 4) throw new Error(`Expected request, approvals, and requeue audits, got ${auditCount}`)
    checks.push("approval-and-requeue-audited")
    checks.push("approved-generation-completed")
    return { status: "ok" as const, checks }
  } finally {
    await cleanup(sql, tenantID, runID)
  }
}

function signed(
  secrets: Record<string, string>,
  actorID: string,
  method: string,
  target: string,
  body: string,
  nonceSeed: string,
): SignedRequest {
  const request = {
    method,
    target,
    actorID,
    timestamp: Date.now(),
    nonce: `${nonceSeed}-${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    body,
  }
  return { ...request, signature: signRequest({ ...request, secret: secrets[actorID]! }) }
}

function expectFailure<A>(exit: Exit.Exit<A, never>, expected: abstract new (...args: any[]) => Error) {
  if (Exit.isSuccess(exit)) {
    throw new Error(`Expected ${expected.name}, received success`)
  }
  const actual = Cause.squash(exit.cause)
  if (!(actual instanceof expected)) {
    const detail = actual instanceof Error ? `${actual.name}: ${actual.message}` : String(actual)
    throw new Error(`Expected ${expected.name}, received ${detail}`)
  }
}

async function seed(sql: Sql, tenantID: string, teamID: string, actors: Record<string, string>) {
  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values (${tenantID}, ${tenantID}, ${Date.now()}, ${Date.now()})
    on conflict (id) do nothing
  `
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    for (const actorID of Object.values(actors)) {
      if (actorID === actors.intruder) continue
      const role = actorID === actors.viewer ? "viewer" : actorID === actors.operator ? "operator" : "admin"
      await tx`
        insert into tenant_member (tenant_id, actor_id, role, time_created)
        values (${tenantID}, ${actorID}, ${role}, ${Date.now()})
        on conflict (tenant_id, actor_id) do nothing
      `
      await tx`
        insert into team_member (tenant_id, team_id, actor_id, role, time_created)
        values (${tenantID}, ${teamID}, ${actorID}, ${role}, ${Date.now()})
        on conflict (tenant_id, team_id, actor_id) do nothing
      `
    }
  })
}

async function countAudit(sql: Sql, tenantID: string, runID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    const rows = await tx<{ count: string | number }[]>`
      select count(*) as count
      from audit_event
      where resource_type = 'worker_queue_action'
         or (resource_type = 'worker_job' and resource_id = ${runID})
    `
    return Number(rows[0]?.count ?? 0)
  })
}

async function cleanup(sql: Sql, tenantID: string, runID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    await tx`delete from worker_queue_action`
    await tx`delete from worker_queue_api_nonce`
    await tx`delete from audit_event where resource_type in ('worker_queue_action', 'worker_job')`
    await tx`delete from worker_job where run_id = ${runID}`
    await tx`delete from team_member`
    await tx`delete from tenant_member`
  })
  await sql`delete from tenant where id = ${tenantID}`
}
