import { Effect } from "effect"
import { fileURLToPath } from "node:url"
import type { Sql } from "postgres"
import { setTenantContext } from "./client"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  claimNextWorkerJob,
  enqueueWorkerJob,
  failWorkerJobClaim,
} from "./worker-job"
import {
  Service,
  layerFromEnv,
  signRequest,
  type SignedRequest,
} from "./worker-queue-admin"
import { runWorkerJobChaos } from "./worker-job-chaos"
import { runWorkerLeaseChaos } from "./worker-lease-chaos"

type ChildResult =
  | { readonly outcome: "allow" | "throttle" | "conflict" }
  | {
      readonly outcome: "approved"
      readonly status: "pending" | "executed" | "rejected" | "expired"
      readonly approvalCount: number
    }
  | { readonly outcome: "error"; readonly error: string }

export type WorkerQueueMultiInstanceSoakResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
  readonly rateLimitProcesses: number
  readonly rateLimitAllowed: number
  readonly approvalProcesses: number
  readonly takeoverRounds: number
}

const childScript = fileURLToPath(new URL("../../../script/postgres-worker-queue-admin-child.ts", import.meta.url))

export async function runWorkerQueueMultiInstanceSoak(
  sql: Sql,
  input: {
    readonly url: string
    readonly env?: NodeJS.ProcessEnv
  },
): Promise<WorkerQueueMultiInstanceSoakResult> {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const sourceEnv = input.env ?? process.env
  const rateLimitProcesses = integer(sourceEnv.OPENCODE_P4_41_SOAK_CONCURRENCY, 12, 4, 64)
  const takeoverRounds = integer(sourceEnv.OPENCODE_P4_41_SOAK_ROUNDS, 2, 1, 10)
  const rateLimit = Math.max(2, Math.floor(rateLimitProcesses / 2))
  const approvalCopies = Math.max(2, Math.min(8, Math.floor(rateLimitProcesses / 2)))
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_soak_${suffix}`
  const teamID = `team_worker_soak_${suffix}`
  const actors = {
    viewer: `viewer_${suffix}`,
    operator: `operator_${suffix}`,
    adminA: `admin_a_${suffix}`,
    adminB: `admin_b_${suffix}`,
  }
  const roles = {
    [actors.viewer]: "viewer",
    [actors.operator]: "operator",
    [actors.adminA]: "admin",
    [actors.adminB]: "admin",
  }
  const secrets = Object.fromEntries(
    Object.values(actors).map((actor) => [actor, `p4-41-secret-${actor}-0123456789`]),
  )
  const runID = `ses_worker_soak_${suffix}`
  const checks: string[] = []
  await seed(sql, tenantID, teamID, roles)
  const failed = await failedJob(sql, tenantID, teamID, runID)
  const env = {
    ...sourceEnv,
    OPENCODE_DATABASE_BACKEND: "postgres-alpha",
    OPENCODE_DATABASE_URL: input.url,
    OPENCODE_TENANT_ID: tenantID,
    OPENCODE_TEAM_ID: teamID,
    OPENCODE_ACTOR_ID: actors.operator,
    OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED: "1",
    OPENCODE_WORKER_QUEUE_IDENTITY_MODE: "hmac",
    OPENCODE_WORKER_QUEUE_ADMIN_KEYS: JSON.stringify(secrets),
    OPENCODE_WORKER_QUEUE_REQUIRED_APPROVALS: "2",
    OPENCODE_WORKER_QUEUE_RATE_LIMITS: JSON.stringify({
      observe: { limit: rateLimit, windowMs: 60_000 },
      recover: { limit: 100, windowMs: 60_000 },
      mutate: { limit: 100, windowMs: 60_000 },
      approval: { limit: 100, windowMs: 60_000 },
      "break-glass": { limit: 10, windowMs: 60_000 },
    }),
  }
  try {
    const rateStart = Date.now() + 1_000
    const rateResults = await Promise.all(
      Array.from({ length: rateLimitProcesses }, () =>
        runChild({
          env,
          operation: "authenticate",
          actorID: actors.viewer,
          actorSecret: secrets[actors.viewer]!,
          target: "/experimental/worker-queue/readiness",
          startAt: rateStart,
        }),
      ),
    )
    assertNoChildErrors(rateResults)
    const allowed = rateResults.filter((result) => result.outcome === "allow").length
    const throttled = rateResults.filter((result) => result.outcome === "throttle").length
    if (allowed !== rateLimit || throttled !== rateLimitProcesses - rateLimit) {
      throw new Error(
        `Distributed rate limit mismatch: allowed=${allowed}, throttled=${throttled}, expected=${rateLimit}/${rateLimitProcesses - rateLimit}`,
      )
    }
    checks.push("multi-process-rate-limit-single-postgres-budget")

    const action = await Effect.runPromise(
      Effect.gen(function* () {
        const admin = yield* Service
        const operator = yield* admin.authenticate(
          signed(
            actors.operator,
            secrets[actors.operator]!,
            "POST",
            "/experimental/worker-queue/actions/requeue",
          ),
        )
        return yield* admin.requestRequeue(operator, {
          runID,
          expectedGeneration: failed.requestedGeneration,
          expectedClaimToken: failed.claimToken,
          requestID: `p4_41_${suffix}`,
        })
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )
    const approvalStart = Date.now() + 1_000
    const approvalActors = [
      ...Array.from({ length: approvalCopies }, () => actors.adminA),
      ...Array.from({ length: approvalCopies }, () => actors.adminB),
    ]
    const approvalResults = await Promise.all(
      approvalActors.map((actorID) =>
        runChild({
          env,
          operation: "approve",
          actorID,
          actorSecret: secrets[actorID]!,
          target: `/experimental/worker-queue/actions/${action.id}/approve`,
          actionID: action.id,
          startAt: approvalStart,
        }),
      ),
    )
    assertNoChildErrors(approvalResults)
    if (approvalResults.filter((result) => result.outcome === "approved" && result.status === "executed").length !== 1) {
      throw new Error("Concurrent approvals did not produce exactly one executing process")
    }
    const evidence = await readEvidence(sql, tenantID, actors.viewer, action.id, runID)
    if (
      evidence.rateAllowed !== rateLimit ||
      evidence.rateThrottled !== rateLimitProcesses - rateLimit ||
      evidence.actionStatus !== "executed" ||
      evidence.activeApprovals !== 2 ||
      evidence.approvalEvents !== 2 ||
      evidence.requestedGeneration !== failed.requestedGeneration + 1 ||
      evidence.actionAuditCount !== 3 ||
      evidence.requeueAuditCount !== 1
    ) {
      throw new Error(`Concurrent governance evidence mismatch: ${JSON.stringify(evidence)}`)
    }
    checks.push("multi-process-rate-limit-audit-exact")
    checks.push("approval-contention-single-execution")
    checks.push("approval-contention-append-only-evidence-exact")

    for (let round = 0; round < takeoverRounds; round++) {
      const lease = await runWorkerLeaseChaos(sql, { url: input.url })
      const job = await runWorkerJobChaos(sql, { url: input.url })
      if (lease.effectCount !== 1 || job.completedGeneration !== 2) {
        throw new Error(`Takeover soak round ${round + 1} did not preserve exactly-once invariants`)
      }
    }
    checks.push("lease-kill-takeover-soak-passed")
    checks.push("job-kill-takeover-soak-passed")
    checks.push("stale-effects-and-claims-fenced-every-round")
    return {
      status: "ok",
      checks,
      rateLimitProcesses,
      rateLimitAllowed: allowed,
      approvalProcesses: approvalActors.length,
      takeoverRounds,
    }
  } finally {
    await cleanup(sql, tenantID)
  }
}

async function runChild(input: {
  readonly env: NodeJS.ProcessEnv
  readonly operation: "authenticate" | "approve"
  readonly actorID: string
  readonly actorSecret: string
  readonly target: string
  readonly startAt: number
  readonly actionID?: string
}) {
  const child = Bun.spawn({
    cmd: [process.execPath, childScript],
    env: {
      ...input.env,
      OPENCODE_P4_41_CHILD_OPERATION: input.operation,
      OPENCODE_P4_41_CHILD_ACTOR_ID: input.actorID,
      OPENCODE_P4_41_CHILD_ACTOR_SECRET: input.actorSecret,
      OPENCODE_P4_41_CHILD_TARGET: input.target,
      OPENCODE_P4_41_CHILD_START_AT: String(input.startAt),
      ...(input.actionID === undefined ? {} : { OPENCODE_P4_41_CHILD_ACTION_ID: input.actionID }),
    },
    stdout: "pipe",
    stderr: "pipe",
  })
  const timeout = setTimeout(() => child.kill(9), 45_000)
  try {
    const [exit, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    if (exit !== 0) throw new Error(`P4.41 child exited ${exit}: ${stderr.slice(0, 1_000)}`)
    const result = JSON.parse(stdout.trim()) as ChildResult
    return result
  } finally {
    clearTimeout(timeout)
  }
}

function assertNoChildErrors(results: readonly ChildResult[]) {
  const failed = results.find((result) => result.outcome === "error")
  if (failed?.outcome === "error") throw new Error(`P4.41 child failed with ${failed.error}`)
}

async function failedJob(sql: Sql, tenantID: string, teamID: string, runID: string) {
  const tenant = { tenantID, teamID, actorID: "p4-41-fixture" }
  await enqueueWorkerJob(sql, { tenant, runID, reason: "wake" })
  const claim = await claimNextWorkerJob(sql, {
    tenant,
    runID,
    ownerID: "p4-41-fixture",
    claimMs: 5_000,
  })
  if (claim === undefined) throw new Error("P4.41 worker job fixture was not claimed")
  return await failWorkerJobClaim(sql, {
    claim,
    error: "P4.41 approval contention fixture",
    maxAttempts: 1,
    retryDelayMs: 0,
  })
}

async function seed(sql: Sql, tenantID: string, teamID: string, roles: Record<string, string>) {
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

async function readEvidence(sql: Sql, tenantID: string, rateActorID: string, actionID: string, runID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, { tenantID })
    const rate = await tx<{ outcome: string; count: string | number }[]>`
      select outcome, count(*) as count
      from worker_queue_identity_audit
      where actor_id = ${rateActorID}
      group by outcome
    `
    const actions = await tx<{ status: string }[]>`
      select status from worker_queue_action where id = ${actionID}
    `
    const active = await tx<{ count: string | number }[]>`
      select count(*) as count from worker_queue_action_approval where action_id = ${actionID}
    `
    const events = await tx<{ count: string | number }[]>`
      select count(*) as count
      from worker_queue_action_approval_event
      where action_id = ${actionID} and event = 'approve'
    `
    const jobs = await tx<{ requested_generation: string | number }[]>`
      select requested_generation from worker_job where run_id = ${runID}
    `
    const audits = await tx<{ count: string | number }[]>`
      select count(*) as count from audit_event where resource_id = ${actionID}
    `
    const requeues = await tx<{ count: string | number }[]>`
      select count(*) as count
      from audit_event
      where action = 'worker_job.requeue'
        and resource_id = ${runID}
        and request_id = ${actionID}
    `
    const byOutcome = new Map(rate.map((row) => [row.outcome, Number(row.count)]))
    return {
      rateAllowed: byOutcome.get("allow") ?? 0,
      rateThrottled: byOutcome.get("throttle") ?? 0,
      actionStatus: actions[0]?.status,
      activeApprovals: Number(active[0]?.count ?? 0),
      approvalEvents: Number(events[0]?.count ?? 0),
      requestedGeneration: Number(jobs[0]?.requested_generation ?? 0),
      actionAuditCount: Number(audits[0]?.count ?? 0),
      requeueAuditCount: Number(requeues[0]?.count ?? 0),
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

function signed(actorID: string, secret: string, method: string, target: string): SignedRequest {
  const request = {
    method,
    target,
    actorID,
    timestamp: Date.now(),
    nonce: `p4_41_${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    body: "",
  }
  return { ...request, signature: signRequest({ ...request, secret }) }
}

function integer(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < min || result > max) {
    throw new Error(`Expected an integer between ${min} and ${max}`)
  }
  return result
}
