import type { Sql, TransactionSql } from "postgres"
import { makeClient, setTenantContext, type TenantContext } from "./client"

export type WorkspaceRecoveryOwnershipLease = {
  readonly tenantID: string
  readonly workspaceID: string
  readonly workspaceDirectory: string
  readonly ownerID: string
  readonly epoch: number
  readonly status: "active" | "released"
  readonly leaseExpiresAt: number
  readonly heartbeatAt: number
  readonly timeAcquired: number
  readonly timeUpdated: number
  readonly timeReleased?: number
}

export type WorkspaceRecoveryOwnershipAcquireResult =
  | {
      readonly acquired: true
      readonly disposition: "acquired" | "renewed" | "takeover"
      readonly lease: WorkspaceRecoveryOwnershipLease
    }
  | {
      readonly acquired: false
      readonly disposition: "contended"
      readonly current: WorkspaceRecoveryOwnershipLease
    }

export type WorkspaceRecoveryOwnershipTarget = {
  readonly tenant: TenantContext
  readonly workspaceID: string
  readonly workspaceDirectory: string
  readonly ownerID: string
  readonly leaseMs: number
}

export type WorkspaceRecoveryOwnershipStore = {
  acquire(input: WorkspaceRecoveryOwnershipTarget): Promise<WorkspaceRecoveryOwnershipAcquireResult>
  renew(
    lease: WorkspaceRecoveryOwnershipLease,
    leaseMs: number,
  ): Promise<WorkspaceRecoveryOwnershipLease | undefined>
  release(lease: WorkspaceRecoveryOwnershipLease): Promise<boolean>
  read(
    tenant: TenantContext,
    workspaceID: string,
  ): Promise<WorkspaceRecoveryOwnershipLease | undefined>
  destroy(tenant: TenantContext, workspaceID: string): Promise<void>
  close(): Promise<void>
}

type OwnershipRow = {
  tenant_id: string
  workspace_id: string
  workspace_directory: string
  owner_id: string
  epoch: string | number
  status: "active" | "released"
  lease_expires_at: string | number
  heartbeat_at: string | number
  time_acquired: string | number
  time_updated: string | number
  time_released: string | number | null
}

export function makePostgresWorkspaceRecoveryOwnershipStore(options: {
  readonly url: string
  readonly max?: number
}): WorkspaceRecoveryOwnershipStore {
  const sql = makeClient({ url: options.url, max: options.max ?? 2 })
  let closed = false
  const open = () => {
    if (closed) throw new Error("workspace recovery ownership store is closed")
  }

  return {
    async acquire(input) {
      open()
      validateTarget(input)
      return await sql.begin(async (tx) => {
        await setTenantContext(tx, input.tenant)
        const now = await databaseNow(tx)
        const rows = await tx<OwnershipRow[]>`
          select *
          from workspace_recovery_owner
          where tenant_id = ${input.tenant.tenantID}
            and workspace_id = ${input.workspaceID}
          for update
        `
        const current = rows[0] === undefined ? undefined : fromRow(rows[0])
        if (current === undefined) {
          const inserted = await tx<OwnershipRow[]>`
            insert into workspace_recovery_owner (
              tenant_id, workspace_id, workspace_directory, owner_id, epoch, status,
              lease_expires_at, heartbeat_at, time_acquired, time_updated, time_released
            )
            values (
              ${input.tenant.tenantID}, ${input.workspaceID}, ${input.workspaceDirectory},
              ${input.ownerID}, 1, 'active', ${now + input.leaseMs}, ${now}, ${now}, ${now}, null
            )
            returning *
          `
          return {
            acquired: true,
            disposition: "acquired",
            lease: fromRow(inserted[0]!),
          }
        }
        if (current.workspaceDirectory !== input.workspaceDirectory) {
          throw new Error(`workspace recovery ownership collision for ${input.workspaceID}`)
        }
        if (
          current.status === "active" &&
          current.leaseExpiresAt > now &&
          current.ownerID !== input.ownerID
        ) {
          return {
            acquired: false,
            disposition: "contended",
            current,
          }
        }
        const liveRenewal =
          current.status === "active" &&
          current.leaseExpiresAt > now &&
          current.ownerID === input.ownerID
        const epoch = liveRenewal ? current.epoch : current.epoch + 1
        const updated = await tx<OwnershipRow[]>`
          update workspace_recovery_owner
          set owner_id = ${input.ownerID},
              epoch = ${epoch},
              status = 'active',
              lease_expires_at = ${now + input.leaseMs},
              heartbeat_at = ${now},
              time_acquired = ${liveRenewal ? current.timeAcquired : now},
              time_updated = ${now},
              time_released = null
          where tenant_id = ${input.tenant.tenantID}
            and workspace_id = ${input.workspaceID}
          returning *
        `
        return {
          acquired: true,
          disposition: liveRenewal ? "renewed" : "takeover",
          lease: fromRow(updated[0]!),
        }
      })
    },
    async renew(lease, leaseMs) {
      open()
      positiveLease(leaseMs)
      return await sql.begin(async (tx) => {
        await setTenantContext(tx, { tenantID: lease.tenantID })
        const now = await databaseNow(tx)
        const rows = await tx<OwnershipRow[]>`
          update workspace_recovery_owner
          set lease_expires_at = ${now + leaseMs},
              heartbeat_at = ${now},
              time_updated = ${now}
          where tenant_id = ${lease.tenantID}
            and workspace_id = ${lease.workspaceID}
            and workspace_directory = ${lease.workspaceDirectory}
            and owner_id = ${lease.ownerID}
            and epoch = ${lease.epoch}
            and status = 'active'
            and lease_expires_at > ${now}
          returning *
        `
        return rows[0] === undefined ? undefined : fromRow(rows[0])
      })
    },
    async release(lease) {
      open()
      return await sql.begin(async (tx) => {
        await setTenantContext(tx, { tenantID: lease.tenantID })
        const now = await databaseNow(tx)
        const rows = await tx<OwnershipRow[]>`
          update workspace_recovery_owner
          set status = 'released',
              lease_expires_at = ${now},
              heartbeat_at = ${now},
              time_updated = ${now},
              time_released = ${now}
          where tenant_id = ${lease.tenantID}
            and workspace_id = ${lease.workspaceID}
            and owner_id = ${lease.ownerID}
            and epoch = ${lease.epoch}
            and status = 'active'
          returning *
        `
        return rows[0] !== undefined
      })
    },
    async read(tenant, workspaceID) {
      open()
      return await sql.begin(async (tx) => {
        await setTenantContext(tx, tenant)
        const rows = await tx<OwnershipRow[]>`
          select *
          from workspace_recovery_owner
          where tenant_id = ${tenant.tenantID}
            and workspace_id = ${workspaceID}
        `
        return rows[0] === undefined ? undefined : fromRow(rows[0])
      })
    },
    async destroy(tenant, workspaceID) {
      open()
      await sql.begin(async (tx) => {
        await setTenantContext(tx, tenant)
        await tx`
          delete from workspace_recovery_owner
          where tenant_id = ${tenant.tenantID}
            and workspace_id = ${workspaceID}
        `
      })
    },
    async close() {
      if (closed) return
      closed = true
      await sql.end({ timeout: 5 })
    },
  }
}

export function makeMemoryWorkspaceRecoveryOwnershipStore(options?: {
  readonly now?: () => number
}): WorkspaceRecoveryOwnershipStore {
  const now = options?.now ?? Date.now
  const rows = new Map<string, WorkspaceRecoveryOwnershipLease>()
  let closed = false
  const open = () => {
    if (closed) throw new Error("workspace recovery ownership store is closed")
  }

  return {
    async acquire(input) {
      open()
      validateTarget(input)
      const time = now()
      const key = ownershipKey(input.tenant.tenantID, input.workspaceID)
      const current = rows.get(key)
      if (current === undefined) {
        const lease = makeLease(input, 1, time)
        rows.set(key, lease)
        return { acquired: true, disposition: "acquired", lease }
      }
      if (current.workspaceDirectory !== input.workspaceDirectory) {
        throw new Error(`workspace recovery ownership collision for ${input.workspaceID}`)
      }
      if (current.status === "active" && current.leaseExpiresAt > time && current.ownerID !== input.ownerID) {
        return { acquired: false, disposition: "contended", current: { ...current } }
      }
      const liveRenewal =
        current.status === "active" &&
        current.leaseExpiresAt > time &&
        current.ownerID === input.ownerID
      const lease = makeLease(input, liveRenewal ? current.epoch : current.epoch + 1, time, {
        timeAcquired: liveRenewal ? current.timeAcquired : time,
      })
      rows.set(key, lease)
      return {
        acquired: true,
        disposition: liveRenewal ? "renewed" : "takeover",
        lease: { ...lease },
      }
    },
    async renew(lease, leaseMs) {
      open()
      positiveLease(leaseMs)
      const time = now()
      const key = ownershipKey(lease.tenantID, lease.workspaceID)
      const current = rows.get(key)
      if (
        current === undefined ||
        current.status !== "active" ||
        current.ownerID !== lease.ownerID ||
        current.epoch !== lease.epoch ||
        current.leaseExpiresAt <= time
      ) {
        return
      }
      const renewed = {
        ...current,
        leaseExpiresAt: time + leaseMs,
        heartbeatAt: time,
        timeUpdated: time,
      }
      rows.set(key, renewed)
      return { ...renewed }
    },
    async release(lease) {
      open()
      const time = now()
      const key = ownershipKey(lease.tenantID, lease.workspaceID)
      const current = rows.get(key)
      if (
        current === undefined ||
        current.status !== "active" ||
        current.ownerID !== lease.ownerID ||
        current.epoch !== lease.epoch
      ) {
        return false
      }
      rows.set(key, {
        ...current,
        status: "released",
        leaseExpiresAt: time,
        heartbeatAt: time,
        timeUpdated: time,
        timeReleased: time,
      })
      return true
    },
    async read(tenant, workspaceID) {
      open()
      const current = rows.get(ownershipKey(tenant.tenantID, workspaceID))
      return current === undefined ? undefined : { ...current }
    },
    async destroy(tenant, workspaceID) {
      open()
      rows.delete(ownershipKey(tenant.tenantID, workspaceID))
    },
    async close() {
      closed = true
      rows.clear()
    },
  }
}

function validateTarget(input: WorkspaceRecoveryOwnershipTarget) {
  if (!input.tenant.tenantID.trim()) throw new Error("workspace recovery ownership requires tenantID")
  if (!input.workspaceID.trim()) throw new Error("workspace recovery ownership requires workspaceID")
  if (!input.workspaceDirectory.trim()) {
    throw new Error("workspace recovery ownership requires workspaceDirectory")
  }
  if (!input.ownerID.trim()) throw new Error("workspace recovery ownership requires ownerID")
  positiveLease(input.leaseMs)
}

function positiveLease(leaseMs: number) {
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0) {
    throw new Error("workspace recovery ownership lease duration must be a positive integer")
  }
}

function makeLease(
  input: WorkspaceRecoveryOwnershipTarget,
  epoch: number,
  now: number,
  options?: { readonly timeAcquired?: number },
): WorkspaceRecoveryOwnershipLease {
  return {
    tenantID: input.tenant.tenantID,
    workspaceID: input.workspaceID,
    workspaceDirectory: input.workspaceDirectory,
    ownerID: input.ownerID,
    epoch,
    status: "active",
    leaseExpiresAt: now + input.leaseMs,
    heartbeatAt: now,
    timeAcquired: options?.timeAcquired ?? now,
    timeUpdated: now,
  }
}

function fromRow(row: OwnershipRow): WorkspaceRecoveryOwnershipLease {
  return {
    tenantID: row.tenant_id,
    workspaceID: row.workspace_id,
    workspaceDirectory: row.workspace_directory,
    ownerID: row.owner_id,
    epoch: Number(row.epoch),
    status: row.status,
    leaseExpiresAt: Number(row.lease_expires_at),
    heartbeatAt: Number(row.heartbeat_at),
    timeAcquired: Number(row.time_acquired),
    timeUpdated: Number(row.time_updated),
    ...(row.time_released === null ? {} : { timeReleased: Number(row.time_released) }),
  }
}

function ownershipKey(tenantID: string, workspaceID: string) {
  return `${tenantID}\u0000${workspaceID}`
}

async function databaseNow(sql: Sql | TransactionSql) {
  const rows = await sql<{ now_ms: string | number }[]>`
    select floor(extract(epoch from clock_timestamp()) * 1000)::bigint as now_ms
  `
  return Number(rows[0]!.now_ms)
}
