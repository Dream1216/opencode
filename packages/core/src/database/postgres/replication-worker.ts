import { Effect } from "effect"
import { and, asc, eq, inArray, isNull, lte, or, sql as drizzleSql } from "drizzle-orm"
import type { Sql } from "postgres"
import { isDeepStrictEqual } from "node:util"
import { Database } from "../database"
import {
  PostgresReplicationOutboxTable,
  type PostgresReplicationOutboxOperation,
} from "../../event/sql"
import { claim as claimAggregate, readAggregate, remove as removeAggregate, replay } from "./event-store"
import { makeClient } from "./client"
import { DatabaseBackend } from "../backend"

export type OutboxRow = typeof PostgresReplicationOutboxTable.$inferSelect

export type DrainOptions = {
  readonly workerID?: string
  readonly batchSize?: number
  readonly leaseMs?: number
  readonly retryDelayMs?: number
  readonly maxAttempts?: number
  readonly dispatch?: (row: OutboxRow) => Promise<void>
}

export type ReplicationMetrics = {
  readonly pending: number
  readonly applied: number
  readonly deadLetter: number
  readonly leased: number
  readonly oldestPendingLagMs: number
}

export async function drainReplicationOutbox(
  db: Database.Interface["db"],
  pg: Sql,
  options: DrainOptions = {},
) {
  const workerID = options.workerID ?? `pg-replication-${process.pid}`
  const rows = await claimBatch(db, workerID, options.batchSize ?? 100, options.leaseMs ?? 30_000)
  let applied = 0
  let retried = 0
  let deadLetter = 0
  for (const row of rows) {
    try {
      await (options.dispatch ?? ((item) => dispatchOutboxRow(pg, item)))(row)
      await markApplied(db, row.sequence, workerID)
      applied++
    } catch (error) {
      const dead = row.attempts + 1 >= (options.maxAttempts ?? 10)
      await markFailed(
        db,
        row.sequence,
        workerID,
        row.attempts + 1,
        dead,
        error instanceof Error ? error.message : String(error),
        options.retryDelayMs ?? 1_000,
      )
      if (dead) deadLetter++
      else retried++
    }
  }
  return {
    status: "ok" as const,
    claimed: rows.length,
    applied,
    retried,
    deadLetter,
    metrics: await replicationMetrics(db),
  }
}

export async function dispatchOutboxRow(pg: Sql, row: OutboxRow) {
  const tenant = { tenantID: row.tenant_id, actorID: row.actor_id }
  switch (row.operation as PostgresReplicationOutboxOperation) {
    case "append":
      if (row.event_id === null || row.seq === null || row.type === null || row.data === null) {
        throw new Error(`Invalid append outbox row ${row.id}`)
      }
      await replay(pg, {
        tenant,
        id: row.event_id,
        aggregateID: row.aggregate_id,
        seq: row.seq,
        type: row.type,
        data: row.data,
        ownerID: row.owner_id ?? undefined,
      })
      return
    case "claim":
      if (row.owner_id === null) throw new Error(`Invalid claim outbox row ${row.id}`)
      await claimAggregate(pg, tenant, row.aggregate_id, row.owner_id)
      return
    case "remove":
      await removeAggregate(pg, tenant, row.aggregate_id)
      return
  }
}

export async function replicationMetrics(db: Database.Interface["db"]): Promise<ReplicationMetrics> {
  const now = Date.now()
  const rows = await Effect.runPromise(
    db.all<{
      pending: number
      applied: number
      dead_letter: number
      leased: number
      oldest_pending: number | null
    }>(drizzleSql`
      select
        sum(case when status = 'pending' then 1 else 0 end) as pending,
        sum(case when status = 'applied' then 1 else 0 end) as applied,
        sum(case when status = 'dead_letter' then 1 else 0 end) as dead_letter,
        sum(case when status = 'pending' and lease_expires_at > ${now} then 1 else 0 end) as leased,
        min(case when status = 'pending' then time_created else null end) as oldest_pending
      from postgres_replication_outbox
    `),
  )
  const row = rows[0]
  return {
    pending: Number(row?.pending ?? 0),
    applied: Number(row?.applied ?? 0),
    deadLetter: Number(row?.dead_letter ?? 0),
    leased: Number(row?.leased ?? 0),
    oldestPendingLagMs: row?.oldest_pending == null ? 0 : Math.max(0, now - Number(row.oldest_pending)),
  }
}

export async function reconcileReplicationOutbox(
  db: Database.Interface["db"],
  pg: Sql,
  input: { readonly limit?: number; readonly aggregateIDs?: readonly string[] } = {},
) {
  const filters = [eq(PostgresReplicationOutboxTable.status, "applied"), eq(PostgresReplicationOutboxTable.operation, "append")]
  if (input.aggregateIDs !== undefined && input.aggregateIDs.length > 0) {
    filters.push(inArray(PostgresReplicationOutboxTable.aggregate_id, input.aggregateIDs))
  }
  const rows = await Effect.runPromise(
    db
      .select()
      .from(PostgresReplicationOutboxTable)
      .where(and(...filters))
      .orderBy(asc(PostgresReplicationOutboxTable.sequence))
      .limit(input.limit ?? 1_000)
      .all(),
  )
  const groups = new Map<string, OutboxRow[]>()
  for (const row of rows) {
    const key = `${row.tenant_id}\u0000${row.aggregate_id}`
    groups.set(key, [...(groups.get(key) ?? []), row])
  }
  const divergences: string[] = []
  for (const group of groups.values()) {
    const first = group[0]!
    const events = await readAggregate(pg, {
      tenant: { tenantID: first.tenant_id, actorID: first.actor_id },
      aggregateID: first.aggregate_id,
      limit: Math.max(group.length + 10, 100),
    })
    const bySeq = new Map(events.map((event) => [event.seq, event]))
    for (const row of group) {
      const stored = row.seq === null ? undefined : bySeq.get(row.seq)
      if (
        stored === undefined ||
        stored.id !== row.event_id ||
        stored.type !== row.type ||
        !isDeepStrictEqual(stored.data, row.data)
      ) {
        divergences.push(`${row.tenant_id}:${row.aggregate_id}:${row.seq ?? "none"}`)
      }
    }
  }
  return {
    status: divergences.length === 0 ? ("ok" as const) : ("diverged" as const),
    compared: rows.length,
    divergences,
    metrics: await replicationMetrics(db),
  }
}

export async function runReplicationWorkerOnce(env: NodeJS.ProcessEnv = process.env) {
  const config = DatabaseBackend.fromEnv(Database.path, env)
  if (config.type !== "postgres-alpha") throw new Error("Replication worker requires postgres-alpha backend")
  if (!config.dualWriteEnabled) throw new Error("Replication worker requires OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED=1")
  if (config.url === undefined) throw new Error("Replication worker requires OPENCODE_DATABASE_URL")
  const pg = makeClient({ url: config.url, max: 1 })
  try {
    return await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        return yield* Effect.tryPromise(() => drainReplicationOutbox(db, pg))
      }).pipe(Effect.provide(Database.layerFromBackend(config)), Effect.scoped),
    )
  } finally {
    await pg.end({ timeout: 5 })
  }
}

async function claimBatch(db: Database.Interface["db"], workerID: string, limit: number, leaseMs: number) {
  const now = Date.now()
  return await Effect.runPromise(
    db.transaction(
      () =>
        Effect.gen(function* () {
          const rows = yield* db
            .select()
            .from(PostgresReplicationOutboxTable)
            .where(
              and(
                eq(PostgresReplicationOutboxTable.status, "pending"),
                lte(PostgresReplicationOutboxTable.next_attempt_at, now),
                or(
                  isNull(PostgresReplicationOutboxTable.lease_expires_at),
                  lte(PostgresReplicationOutboxTable.lease_expires_at, now),
                ),
              ),
            )
            .orderBy(asc(PostgresReplicationOutboxTable.sequence))
            .limit(limit)
            .all()
          for (const row of rows) {
            yield* db
              .update(PostgresReplicationOutboxTable)
              .set({ lease_owner: workerID, lease_expires_at: now + leaseMs, time_updated: now })
              .where(eq(PostgresReplicationOutboxTable.sequence, row.sequence))
              .run()
          }
          return rows.map((row) => ({ ...row, lease_owner: workerID, lease_expires_at: now + leaseMs }))
        }),
      { behavior: "immediate" },
    ),
  )
}

async function markApplied(db: Database.Interface["db"], sequence: number, workerID: string) {
  const now = Date.now()
  await Effect.runPromise(
    db
      .update(PostgresReplicationOutboxTable)
      .set({
        status: "applied",
        lease_owner: null,
        lease_expires_at: null,
        last_error: null,
        time_updated: now,
        time_applied: now,
      })
      .where(
        and(
          eq(PostgresReplicationOutboxTable.sequence, sequence),
          eq(PostgresReplicationOutboxTable.lease_owner, workerID),
        ),
      )
      .run(),
  )
}

async function markFailed(
  db: Database.Interface["db"],
  sequence: number,
  workerID: string,
  attempts: number,
  dead: boolean,
  error: string,
  retryDelayMs: number,
) {
  const now = Date.now()
  await Effect.runPromise(
    db
      .update(PostgresReplicationOutboxTable)
      .set({
        status: dead ? "dead_letter" : "pending",
        attempts,
        next_attempt_at: dead ? now : now + retryDelayMs,
        lease_owner: null,
        lease_expires_at: null,
        last_error: error.slice(0, 4_000),
        time_updated: now,
      })
      .where(
        and(
          eq(PostgresReplicationOutboxTable.sequence, sequence),
          eq(PostgresReplicationOutboxTable.lease_owner, workerID),
        ),
      )
      .run(),
  )
}
