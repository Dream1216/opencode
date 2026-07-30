import type { Sql, TransactionSql } from "postgres"
import { randomUUID } from "node:crypto"
import { setTenantContext, type TenantContext } from "./client"

export type WorkerJobReason = "wake" | "resume" | "recovery"
export type WorkerJobStatus = "pending" | "running" | "completed" | "failed" | "cancelled"

export type WorkerJob = {
  readonly tenantID: string
  readonly runID: string
  readonly requestedGeneration: number
  readonly claimedGeneration: number
  readonly completedGeneration: number
  readonly reason: WorkerJobReason
  readonly status: WorkerJobStatus
  readonly availableAt: number
  readonly claimOwner?: string
  readonly claimToken: number
  readonly claimExpiresAt?: number
  readonly attempts: number
  readonly lastError?: string
  readonly timeCreated: number
  readonly timeUpdated: number
  readonly timeCompleted?: number
}

export type WorkerJobClaim = WorkerJob & {
  readonly status: "running"
  readonly claimOwner: string
  readonly claimExpiresAt: number
}

type JobRow = {
  tenant_id: string
  run_id: string
  requested_generation: string | number
  claimed_generation: string | number
  completed_generation: string | number
  reason: WorkerJobReason
  status: WorkerJobStatus
  available_at: string | number
  claim_owner: string | null
  claim_token: string | number
  claim_expires_at: string | number | null
  attempts: number
  last_error: string | null
  time_created: string | number
  time_updated: string | number
  time_completed: string | number | null
}

export class WorkerJobClaimRejectedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WorkerJobClaimRejectedError"
  }
}

export class WorkerJobRequeueRejectedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WorkerJobRequeueRejectedError"
  }
}

export type WorkerQueueMetrics = {
  readonly pending: number
  readonly running: number
  readonly completed: number
  readonly failed: number
  readonly cancelled: number
  readonly expiredRunning: number
  readonly oldestPendingAgeMs: number
}

export type WorkerJobRequeueInput = {
  readonly tenant: TenantContext & { readonly actorID: string }
  readonly runID: string
  readonly expectedGeneration: number
  readonly expectedClaimToken: number
  readonly requestID?: string
}

export async function enqueueWorkerJob(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly runID: string
    readonly reason: Exclude<WorkerJobReason, "recovery">
  },
): Promise<WorkerJob> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const now = await databaseNow(tx)
    const rows = await tx<JobRow[]>`
      insert into worker_job (
        tenant_id, run_id, requested_generation, claimed_generation, completed_generation,
        reason, status, available_at, claim_token, attempts, time_created, time_updated
      )
      values (
        ${input.tenant.tenantID}, ${input.runID}, 1, 0, 0,
        ${input.reason}, 'pending', ${now}, 0, 0, ${now}, ${now}
      )
      on conflict (tenant_id, run_id) do update
      set requested_generation = worker_job.requested_generation + 1,
          reason = excluded.reason,
          status = case
            when worker_job.status = 'running' and worker_job.claim_expires_at > ${now} then 'running'
            else 'pending'
          end,
          available_at = ${now},
          last_error = null,
          time_updated = ${now},
          time_completed = null
      returning *
    `
    return fromRow(rows[0]!)
  })
}

export async function claimNextWorkerJob(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly ownerID: string
    readonly claimMs: number
    readonly runID?: string
  },
): Promise<WorkerJobClaim | undefined> {
  if (input.claimMs <= 0) throw new Error("Worker job claim duration must be positive")
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const now = await databaseNow(tx)
    const candidates = await tx<{ run_id: string }[]>`
      select run_id
      from worker_job
      where (
        (status = 'pending' and available_at <= ${now})
        or (status = 'running' and claim_expires_at <= ${now})
      )
        and (${input.runID ?? null}::text is null or run_id = ${input.runID ?? null})
      order by available_at, time_created, run_id
      for update skip locked
      limit 1
    `
    const candidate = candidates[0]
    if (candidate === undefined) return undefined
    const rows = await tx<JobRow[]>`
      update worker_job
      set claimed_generation = requested_generation,
          reason = case when status = 'running' then 'recovery' else reason end,
          status = 'running',
          claim_owner = ${input.ownerID},
          claim_token = claim_token + 1,
          claim_expires_at = ${now + input.claimMs},
          attempts = attempts + 1,
          time_updated = ${now}
      where run_id = ${candidate.run_id}
      returning *
    `
    return asClaim(fromRow(rows[0]!))
  })
}

export async function heartbeatWorkerJobClaim(
  sql: Sql,
  claim: WorkerJobClaim,
  claimMs: number,
): Promise<WorkerJobClaim> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenantOf(claim))
    const now = await databaseNow(tx)
    const rows = await tx<JobRow[]>`
      update worker_job
      set claim_expires_at = ${now + claimMs},
          time_updated = ${now}
      where run_id = ${claim.runID}
        and status = 'running'
        and claim_owner = ${claim.claimOwner}
        and claim_token = ${claim.claimToken}
        and claim_expires_at > ${now}
      returning *
    `
    if (rows[0] === undefined) throw rejected(claim, "heartbeat")
    return asClaim(fromRow(rows[0]))
  })
}

export async function completeWorkerJobClaim(sql: Sql, claim: WorkerJobClaim): Promise<WorkerJob> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenantOf(claim))
    const now = await databaseNow(tx)
    const current = await lockedClaim(tx, claim, now, "complete")
    const pending = current.requestedGeneration > current.claimedGeneration
    const rows = await tx<JobRow[]>`
      update worker_job
      set completed_generation = greatest(completed_generation, claimed_generation),
          status = ${pending ? "pending" : "completed"},
          available_at = ${now},
          claim_owner = null,
          claim_expires_at = null,
          last_error = null,
          time_updated = ${now},
          time_completed = ${pending ? null : now}
      where run_id = ${claim.runID}
      returning *
    `
    return fromRow(rows[0]!)
  })
}

export async function failWorkerJobClaim(
  sql: Sql,
  input: {
    readonly claim: WorkerJobClaim
    readonly error: string
    readonly maxAttempts: number
    readonly retryDelayMs: number
  },
): Promise<WorkerJob> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenantOf(input.claim))
    const now = await databaseNow(tx)
    const current = await lockedClaim(tx, input.claim, now, "fail")
    const terminal = current.attempts >= input.maxAttempts
    const rows = await tx<JobRow[]>`
      update worker_job
      set status = ${terminal ? "failed" : "pending"},
          available_at = ${terminal ? now : now + input.retryDelayMs},
          claim_owner = null,
          claim_expires_at = null,
          last_error = ${input.error.slice(0, 4000)},
          time_updated = ${now},
          time_completed = ${terminal ? now : null}
      where run_id = ${input.claim.runID}
      returning *
    `
    return fromRow(rows[0]!)
  })
}

export async function cancelWorkerJob(sql: Sql, tenant: TenantContext, runID: string): Promise<WorkerJob | undefined> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const now = await databaseNow(tx)
    const rows = await tx<JobRow[]>`
      update worker_job
      set status = 'cancelled',
          claim_owner = null,
          claim_expires_at = null,
          time_updated = ${now},
          time_completed = ${now}
      where run_id = ${runID}
        and status in ('pending', 'running')
      returning *
    `
    return rows[0] === undefined ? undefined : fromRow(rows[0])
  })
}

export async function readWorkerJob(sql: Sql, tenant: TenantContext, runID: string): Promise<WorkerJob | undefined> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const rows = await tx<JobRow[]>`
      select *
      from worker_job
      where run_id = ${runID}
    `
    return rows[0] === undefined ? undefined : fromRow(rows[0])
  })
}

export async function listRecoverableWorkerJobs(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly limit?: number
  },
): Promise<readonly WorkerJob[]> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const limit = Math.min(500, Math.max(1, Math.floor(input.limit ?? 100)))
    const rows = await tx<JobRow[]>`
      select *
      from worker_job
      where status in ('failed', 'cancelled')
      order by time_updated, run_id
      limit ${limit}
    `
    return rows.map(fromRow)
  })
}

export async function readWorkerQueueMetrics(sql: Sql, tenant: TenantContext): Promise<WorkerQueueMetrics> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const now = await databaseNow(tx)
    const rows = await tx<
      {
        pending: string | number
        running: string | number
        completed: string | number
        failed: string | number
        cancelled: string | number
        expired_running: string | number
        oldest_pending_at: string | number | null
      }[]
    >`
      select
        count(*) filter (where status = 'pending') as pending,
        count(*) filter (where status = 'running') as running,
        count(*) filter (where status = 'completed') as completed,
        count(*) filter (where status = 'failed') as failed,
        count(*) filter (where status = 'cancelled') as cancelled,
        count(*) filter (
          where status = 'running' and claim_expires_at <= ${now}
        ) as expired_running,
        min(available_at) filter (where status = 'pending') as oldest_pending_at
      from worker_job
    `
    const row = rows[0]!
    return {
      pending: Number(row.pending),
      running: Number(row.running),
      completed: Number(row.completed),
      failed: Number(row.failed),
      cancelled: Number(row.cancelled),
      expiredRunning: Number(row.expired_running),
      oldestPendingAgeMs:
        row.oldest_pending_at === null ? 0 : Math.max(0, now - Number(row.oldest_pending_at)),
    }
  })
}

export async function requeueWorkerJob(
  sql: Sql,
  input: WorkerJobRequeueInput,
): Promise<WorkerJob> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    return await requeueWorkerJobInTransaction(tx, input)
  })
}

export async function requeueWorkerJobInTransaction(
  tx: TransactionSql,
  input: WorkerJobRequeueInput,
): Promise<WorkerJob> {
  const now = await databaseNow(tx)
  const rows = await tx<JobRow[]>`
      select *
      from worker_job
      where run_id = ${input.runID}
      for update
    `
  const current = rows[0] === undefined ? undefined : fromRow(rows[0])
  if (
    current === undefined ||
    (current.status !== "failed" && current.status !== "cancelled") ||
    current.requestedGeneration !== input.expectedGeneration ||
    current.claimToken !== input.expectedClaimToken
  ) {
    throw new WorkerJobRequeueRejectedError(
      `Worker job ${input.runID} cannot be requeued with generation ${input.expectedGeneration} and claim ${input.expectedClaimToken}`,
    )
  }
  const updated = await tx<JobRow[]>`
      update worker_job
      set requested_generation = requested_generation + 1,
          reason = 'recovery',
          status = 'pending',
          available_at = ${now},
          claim_owner = null,
          claim_expires_at = null,
          attempts = 0,
          last_error = null,
          time_updated = ${now},
          time_completed = null
      where run_id = ${input.runID}
      returning *
    `
  const job = fromRow(updated[0]!)
  await tx`
      insert into audit_event (
        id, tenant_id, actor_id, action, resource_type, resource_id,
        outcome, request_id, metadata, time_created
      )
      values (
        ${`audit_worker_job_${randomUUID()}`},
        ${input.tenant.tenantID},
        ${input.tenant.actorID},
        'worker_job.requeue',
        'worker_job',
        ${input.runID},
        'allow',
        ${input.requestID ?? null},
        ${tx.json({
          previousStatus: current.status,
          previousGeneration: current.requestedGeneration,
          previousClaimToken: current.claimToken,
          requestedGeneration: job.requestedGeneration,
        })},
        ${now}
      )
    `
  return job
}

export async function cleanupWorkerJob(sql: Sql, tenant: TenantContext, runID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from worker_job where run_id = ${runID}`
    await tx`
      delete from audit_event
      where resource_type = 'worker_job'
        and resource_id = ${runID}
    `
  })
}

async function lockedClaim(
  sql: TransactionSql,
  claim: WorkerJobClaim,
  now: number,
  action: string,
): Promise<WorkerJobClaim> {
  const rows = await sql<JobRow[]>`
    select *
    from worker_job
    where run_id = ${claim.runID}
    for update
  `
  const current = rows[0] === undefined ? undefined : fromRow(rows[0])
  if (
    current === undefined ||
    current.status !== "running" ||
    current.claimOwner !== claim.claimOwner ||
    current.claimToken !== claim.claimToken ||
    current.claimExpiresAt === undefined ||
    current.claimExpiresAt <= now
  ) {
    throw rejected(claim, action)
  }
  return asClaim(current)
}

async function databaseNow(sql: TransactionSql) {
  const rows = await sql<{ now_ms: string | number }[]>`
    select floor(extract(epoch from clock_timestamp()) * 1000)::bigint as now_ms
  `
  return Number(rows[0]!.now_ms)
}

function tenantOf(job: WorkerJob): TenantContext {
  return { tenantID: job.tenantID }
}

function fromRow(row: JobRow): WorkerJob {
  return {
    tenantID: row.tenant_id,
    runID: row.run_id,
    requestedGeneration: Number(row.requested_generation),
    claimedGeneration: Number(row.claimed_generation),
    completedGeneration: Number(row.completed_generation),
    reason: row.reason,
    status: row.status,
    availableAt: Number(row.available_at),
    ...(row.claim_owner === null ? {} : { claimOwner: row.claim_owner }),
    claimToken: Number(row.claim_token),
    ...(row.claim_expires_at === null ? {} : { claimExpiresAt: Number(row.claim_expires_at) }),
    attempts: row.attempts,
    ...(row.last_error === null ? {} : { lastError: row.last_error }),
    timeCreated: Number(row.time_created),
    timeUpdated: Number(row.time_updated),
    ...(row.time_completed === null ? {} : { timeCompleted: Number(row.time_completed) }),
  }
}

function asClaim(job: WorkerJob): WorkerJobClaim {
  if (job.status !== "running" || job.claimOwner === undefined || job.claimExpiresAt === undefined) {
    throw new Error(`Worker job ${job.runID} is not an active claim`)
  }
  return job as WorkerJobClaim
}

function rejected(claim: WorkerJobClaim, action: string) {
  return new WorkerJobClaimRejectedError(
    `Worker job ${claim.runID} claim ${claim.claimToken} owned by ${claim.claimOwner} cannot ${action}`,
  )
}
