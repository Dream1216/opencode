import type { Sql } from "postgres"
import { isDeepStrictEqual } from "node:util"
import { setTenantContext, type TenantContext } from "./client"

export type WorkerLeaseStatus = "active" | "released" | "completed"

export type WorkerLease = {
  readonly tenantID: string
  readonly runID: string
  readonly ownerID: string
  readonly fencingToken: number
  readonly status: WorkerLeaseStatus
  readonly leaseExpiresAt: number
  readonly heartbeatAt: number
  readonly timeAcquired: number
  readonly timeReleased?: number
  readonly timeCompleted?: number
}

export type AcquireWorkerLeaseResult = {
  readonly acquired: boolean
  readonly lease: WorkerLease
}

export type WorkerFence = {
  readonly tenant: TenantContext
  readonly runID: string
  readonly ownerID: string
  readonly fencingToken: number
}

type LeaseRow = {
  tenant_id: string
  run_id: string
  owner_id: string
  fencing_token: string | number
  status: WorkerLeaseStatus
  lease_expires_at: string | number
  heartbeat_at: string | number
  time_acquired: string | number
  time_released: string | number | null
  time_completed: string | number | null
}

type EffectRow = {
  owner_id: string
  fencing_token: string | number
  payload: Record<string, unknown>
  time_committed: string | number
}

export class WorkerFenceRejectedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WorkerFenceRejectedError"
  }
}

export async function acquireWorkerLease(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly runID: string
    readonly ownerID: string
    readonly leaseMs: number
  },
): Promise<AcquireWorkerLeaseResult> {
  if (input.leaseMs <= 0) throw new Error("Worker lease duration must be positive")
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const now = await databaseNow(tx)
    const rows = await tx<LeaseRow[]>`
      select *
      from worker_lease
      where run_id = ${input.runID}
      for update
    `
    const current = rows[0]
    if (current === undefined) {
      const inserted = await tx<LeaseRow[]>`
        insert into worker_lease (
          tenant_id, run_id, owner_id, fencing_token, status,
          lease_expires_at, heartbeat_at, time_acquired
        )
        values (
          ${input.tenant.tenantID}, ${input.runID}, ${input.ownerID}, 1, 'active',
          ${now + input.leaseMs}, ${now}, ${now}
        )
        returning *
      `
      return { acquired: true, lease: fromRow(inserted[0]!) }
    }
    const lease = fromRow(current)
    if (lease.status === "active" && lease.leaseExpiresAt > now) {
      return { acquired: lease.ownerID === input.ownerID, lease }
    }
    const next = await tx<LeaseRow[]>`
      update worker_lease
      set owner_id = ${input.ownerID},
          fencing_token = fencing_token + 1,
          status = 'active',
          lease_expires_at = ${now + input.leaseMs},
          heartbeat_at = ${now},
          time_acquired = ${now},
          time_released = null,
          time_completed = null
      where run_id = ${input.runID}
      returning *
    `
    return { acquired: true, lease: fromRow(next[0]!) }
  })
}

export async function heartbeatWorkerLease(
  sql: Sql,
  fence: WorkerFence,
  leaseMs: number,
): Promise<WorkerLease> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, fence.tenant)
    const now = await databaseNow(tx)
    const rows = await tx<LeaseRow[]>`
      update worker_lease
      set heartbeat_at = ${now},
          lease_expires_at = ${now + leaseMs}
      where run_id = ${fence.runID}
        and owner_id = ${fence.ownerID}
        and fencing_token = ${fence.fencingToken}
        and status = 'active'
        and lease_expires_at > ${now}
      returning *
    `
    const row = rows[0]
    if (row === undefined) throw rejected(fence, "heartbeat")
    return fromRow(row)
  })
}

export async function releaseWorkerLease(sql: Sql, fence: WorkerFence): Promise<WorkerLease> {
  return await transitionWorkerLease(sql, fence, "released")
}

export async function completeWorkerLease(sql: Sql, fence: WorkerFence): Promise<WorkerLease> {
  return await transitionWorkerLease(sql, fence, "completed")
}

export async function commitWorkerEffect(
  sql: Sql,
  input: WorkerFence & {
    readonly effectKey: string
    readonly payload: Record<string, unknown>
  },
) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const now = await databaseNow(tx)
    const leaseRows = await tx<LeaseRow[]>`
      select *
      from worker_lease
      where run_id = ${input.runID}
      for update
    `
    const lease = leaseRows[0]
    assertFence(lease, input, now, "commit effect")
    const existing = await tx<EffectRow[]>`
      select owner_id, fencing_token, payload, time_committed
      from worker_effect
      where run_id = ${input.runID}
        and effect_key = ${input.effectKey}
    `
    const row = existing[0]
    if (row !== undefined) {
      if (!isDeepStrictEqual(row.payload, input.payload)) {
        throw new WorkerFenceRejectedError(
          `Worker effect ${input.effectKey} already exists with a different payload for run ${input.runID}`,
        )
      }
      return {
        committed: false,
        fencingToken: Number(row.fencing_token),
        timeCommitted: Number(row.time_committed),
      }
    }
    const inserted = await tx<{ fencing_token: string | number; time_committed: string | number }[]>`
      insert into worker_effect (
        tenant_id, run_id, effect_key, owner_id, fencing_token, payload, time_committed
      )
      values (
        ${input.tenant.tenantID}, ${input.runID}, ${input.effectKey}, ${input.ownerID},
        ${input.fencingToken}, ${tx.json(input.payload as any)}, ${now}
      )
      returning fencing_token, time_committed
    `
    return {
      committed: true,
      fencingToken: Number(inserted[0]!.fencing_token),
      timeCommitted: Number(inserted[0]!.time_committed),
    }
  })
}

export async function readWorkerLease(sql: Sql, tenant: TenantContext, runID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const rows = await tx<LeaseRow[]>`
      select *
      from worker_lease
      where run_id = ${runID}
    `
    return rows[0] === undefined ? undefined : fromRow(rows[0])
  })
}

export async function assertWorkerFence(sql: Sql, fence: WorkerFence): Promise<WorkerLease> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, fence.tenant)
    const now = await databaseNow(tx)
    const rows = await tx<LeaseRow[]>`
      select *
      from worker_lease
      where run_id = ${fence.runID}
    `
    const row = rows[0]
    assertFence(row, fence, now, "execution boundary")
    return fromRow(row)
  })
}

export async function countWorkerEffects(sql: Sql, tenant: TenantContext, runID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const rows = await tx<{ count: string | number }[]>`
      select count(*) as count
      from worker_effect
      where run_id = ${runID}
    `
    return Number(rows[0]?.count ?? 0)
  })
}

export async function cleanupWorkerRun(sql: Sql, tenant: TenantContext, runID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from worker_effect where run_id = ${runID}`
    await tx`delete from worker_lease where run_id = ${runID}`
  })
}

async function transitionWorkerLease(sql: Sql, fence: WorkerFence, status: "released" | "completed") {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, fence.tenant)
    const now = await databaseNow(tx)
    const rows =
      status === "released"
        ? await tx<LeaseRow[]>`
            update worker_lease
            set status = 'released',
                time_released = ${now},
                lease_expires_at = ${now}
            where run_id = ${fence.runID}
              and owner_id = ${fence.ownerID}
              and fencing_token = ${fence.fencingToken}
              and status = 'active'
              and lease_expires_at > ${now}
            returning *
          `
        : await tx<LeaseRow[]>`
            update worker_lease
            set status = 'completed',
                time_completed = ${now},
                lease_expires_at = ${now}
            where run_id = ${fence.runID}
              and owner_id = ${fence.ownerID}
              and fencing_token = ${fence.fencingToken}
              and status = 'active'
              and lease_expires_at > ${now}
            returning *
          `
    const row = rows[0]
    if (row === undefined) throw rejected(fence, status)
    return fromRow(row)
  })
}

async function databaseNow(sql: { unsafe: Sql["unsafe"] }) {
  const rows = await sql.unsafe<{ now: string | number }[]>(
    "select floor(extract(epoch from clock_timestamp()) * 1000)::bigint as now",
  )
  return Number(rows[0]!.now)
}

function assertFence(
  row: LeaseRow | undefined,
  fence: WorkerFence,
  now: number,
  operation: string,
): asserts row is LeaseRow {
  if (
    row === undefined ||
    row.status !== "active" ||
    row.owner_id !== fence.ownerID ||
    Number(row.fencing_token) !== fence.fencingToken ||
    Number(row.lease_expires_at) <= now
  ) {
    throw rejected(fence, operation)
  }
}

function rejected(fence: WorkerFence, operation: string) {
  return new WorkerFenceRejectedError(
    `Worker fence rejected ${operation} for run ${fence.runID}, owner ${fence.ownerID}, token ${fence.fencingToken}`,
  )
}

function fromRow(row: LeaseRow): WorkerLease {
  return {
    tenantID: row.tenant_id,
    runID: row.run_id,
    ownerID: row.owner_id,
    fencingToken: Number(row.fencing_token),
    status: row.status,
    leaseExpiresAt: Number(row.lease_expires_at),
    heartbeatAt: Number(row.heartbeat_at),
    timeAcquired: Number(row.time_acquired),
    ...(row.time_released === null ? {} : { timeReleased: Number(row.time_released) }),
    ...(row.time_completed === null ? {} : { timeCompleted: Number(row.time_completed) }),
  }
}
