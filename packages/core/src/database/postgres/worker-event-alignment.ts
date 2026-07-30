import { DateTime, Effect, Schema } from "effect"
import type { Sql } from "postgres"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import type { Definition } from "@opencode-ai/schema/event"
import { SessionEvent } from "../../session/event"
import { EventV2 } from "../../event"
import { Database } from "../database"
import { AppNodeBuilder } from "../../effect/app-node-builder"
import { LayerNode } from "../../effect/layer-node"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext, type TenantContext } from "./client"
import { make } from "./event-v2-facade"

export type WorkerAlignmentResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly replayAggregateID: string
  readonly checks: readonly string[]
  readonly timeline: readonly NormalizedWorkerEvent[]
  readonly replayTimeline: readonly NormalizedWorkerEvent[]
}

type WorkloadItem = {
  readonly definition: Definition
  readonly data: Record<string, unknown>
  readonly id: string
}

type NormalizedWorkerEvent = {
  readonly seq: number
  readonly type: string
  readonly reason?: string
  readonly phase?: string
  readonly force?: boolean
  readonly leaseOwner?: string
  readonly errorType?: string
}

const tenant = {
  tenantID: "tenant_worker_event_alignment",
  actorID: "actor_worker_event_alignment",
} satisfies TenantContext

export async function runWorkerEventAlignment(sql: Sql): Promise<WorkerAlignmentResult> {
  const checks: string[] = []
  const aggregateID = `ses_pg_worker_event_alignment_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const replayAggregateID = `${aggregateID}_replay`

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("pg-migrations-applied")
  checks.push("pg-rls-ready")

  const sqlite = await runSqliteWorkerWorkload(aggregateID)
  checks.push("sqlite-worker-workload-complete")

  const pg = await runPostgresWorkerWorkload(sql, aggregateID)
  checks.push("postgres-worker-workload-complete")

  assertComparable(sqlite.timeline, pg.timeline, "worker lifecycle publish timeline")
  checks.push("worker-publish-timeline-parity")

  const serialized = workerWorkload(replayAggregateID).map(serialize)
  const sqliteReplay = await runSqliteReplayWorkload(replayAggregateID, serialized)
  checks.push("sqlite-worker-replay-complete")

  const pgReplay = await runPostgresReplayWorkload(sql, replayAggregateID, serialized)
  checks.push("postgres-worker-replay-complete")

  assertComparable(sqliteReplay, pgReplay, "worker lifecycle replay timeline")
  checks.push("worker-replay-timeline-parity")

  await cleanupPostgres(sql, aggregateID)
  await cleanupPostgres(sql, replayAggregateID)
  checks.push("postgres-cleanup-complete")

  return { status: "ok", aggregateID, replayAggregateID, checks, timeline: sqlite.timeline, replayTimeline: sqliteReplay }
}

async function runSqliteWorkerWorkload(aggregateID: string) {
  return await withSqliteEvents(async (events, db) => {
    for (const item of workerWorkload(aggregateID)) {
      await publishSqlite(events, item)
    }
    const page = await Effect.runPromise(
      EventV2.readAggregate(db, { aggregateID, limit: 100, manifest: SessionDurable }),
    )
    return { timeline: normalizeEvents(page.events) }
  })
}

async function runPostgresWorkerWorkload(sql: Sql, aggregateID: string) {
  const facade = make({ sql, tenant, ownerID: "worker-alignment-publisher" })
  for (const item of workerWorkload(aggregateID)) {
    await facade.publish(item.definition, item.data as any, { id: item.id as any, ownerID: "worker-alignment-publisher" })
  }
  const page = await facade.readAggregate({ aggregateID, limit: 100, manifest: SessionDurable })
  return { timeline: normalizeEvents(page.events) }
}

async function runSqliteReplayWorkload(aggregateID: string, serialized: readonly EventV2.SerializedEvent[]) {
  return await withSqliteEvents(async (events, db) => {
    for (const event of serialized) {
      await Effect.runPromise(events.replay(event, { ownerID: "worker-alignment-replay" }))
    }
    await Effect.runPromise(events.replay(serialized[0]!, { ownerID: "worker-alignment-replay" }))
    await Effect.runPromise(events.claim(aggregateID, "claimed-worker-owner"))
    await expectEffectRejected(
      events.replay(serializedWorkerEvent(aggregateID, 99, SessionEvent.Worker.Completed, {}), {
        ownerID: "wrong-worker-owner",
        strictOwner: true,
      }),
      "SQLite worker strict owner mismatch should be rejected",
    )
    await Effect.runPromise(
      events.replay(serializedWorkerEvent(aggregateID, serialized.length, SessionEvent.Worker.Completed, {}), {
        ownerID: "claimed-worker-owner",
        strictOwner: true,
      }),
    )
    const page = await Effect.runPromise(
      EventV2.readAggregate(db, { aggregateID, limit: 100, manifest: SessionDurable }),
    )
    return normalizeEvents(page.events)
  })
}

async function runPostgresReplayWorkload(sql: Sql, aggregateID: string, serialized: readonly EventV2.SerializedEvent[]) {
  const facade = make({ sql, tenant, ownerID: "worker-alignment-replay" })
  for (const event of serialized) {
    await facade.replay(event, { ownerID: "worker-alignment-replay" })
  }
  await facade.replay(serialized[0]!, { ownerID: "worker-alignment-replay" })
  await facade.claim(aggregateID, "claimed-worker-owner")
  await expectPromiseRejected(
    facade.replay(serializedWorkerEvent(aggregateID, 99, SessionEvent.Worker.Completed, {}), {
      ownerID: "wrong-worker-owner",
      strictOwner: true,
    }),
    "PostgreSQL worker strict owner mismatch should be rejected",
  )
  await facade.replay(serializedWorkerEvent(aggregateID, serialized.length, SessionEvent.Worker.Completed, {}), {
    ownerID: "claimed-worker-owner",
    strictOwner: true,
  })
  const page = await facade.readAggregate({ aggregateID, limit: 100, manifest: SessionDurable })
  return normalizeEvents(page.events)
}

async function withSqliteEvents<T>(
  run: (events: EventV2.Interface, db: Database.Interface["db"]) => Promise<T>,
): Promise<T> {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-worker-event-alignment-"))
  const filename = path.join(temporary, "opencode.sqlite")
  try {
    const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(filename)],
    ])
    return await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        const { db } = yield* Database.Service
        return yield* Effect.promise(() => run(events, db))
      }).pipe(Effect.scoped, Effect.provide(layer)),
    )
  } finally {
    await fs.rm(temporary, { recursive: true, force: true })
  }
}

async function publishSqlite(events: EventV2.Interface, item: WorkloadItem) {
  await Effect.runPromise(events.publish(item.definition, item.data as any, { id: item.id as any }))
}

function workerWorkload(aggregateID: string): readonly WorkloadItem[] {
  return [
    item(aggregateID, 0, SessionEvent.Worker.Scheduled, { reason: "wake" }),
    item(aggregateID, 1, SessionEvent.Worker.Resumed, {}),
    item(aggregateID, 2, SessionEvent.Worker.Lease, { phase: "acquired", lease: lease("a") }),
    item(aggregateID, 3, SessionEvent.Worker.Started, { force: false }),
    item(aggregateID, 4, SessionEvent.Worker.Lease, { phase: "heartbeat", lease: lease("a") }),
    item(aggregateID, 5, SessionEvent.Worker.StopRequested, {}),
    item(aggregateID, 6, SessionEvent.Worker.Stopped, { reason: "interrupted" }),
    item(aggregateID, 7, SessionEvent.Worker.Lease, { phase: "released", reason: "stopped", lease: lease("a") }),
    item(aggregateID, 8, SessionEvent.Worker.Scheduled, { reason: "resume" }),
    item(aggregateID, 9, SessionEvent.Worker.Lease, { phase: "acquired", lease: lease("b") }),
    item(aggregateID, 10, SessionEvent.Worker.Started, { force: true }),
    item(aggregateID, 11, SessionEvent.Worker.Completed, {}),
    item(aggregateID, 12, SessionEvent.Worker.Lease, { phase: "released", reason: "completed", lease: lease("b") }),
  ]
}

function item(aggregateID: string, index: number, definition: Definition, extra: Record<string, unknown>): WorkloadItem {
  return {
    definition,
    id: eventID(aggregateID, index),
    data: {
      sessionID: aggregateID,
      timestamp: DateTime.makeUnsafe(index),
      ...extra,
    },
  }
}

function lease(suffix: string) {
  return {
    leaseID: `wlease_worker_alignment_${suffix}`,
    ownerID: `worker-owner-${suffix}`,
    ttlMs: 15000,
  }
}

function eventID(aggregateID: string, seq: number | string) {
  return `evt_${aggregateID}_worker_${seq}`
}

function serialize(item: WorkloadItem): EventV2.SerializedEvent {
  const durable = item.definition.durable
  if (durable === undefined) throw new Error(`Worker workload definition is not durable: ${item.definition.type}`)
  return {
    id: item.id as any,
    type: EventV2.versionedType(item.definition.type, durable.version),
    aggregateID: String(item.data.sessionID),
    seq: Number(item.id.split("_").at(-1)),
    data: Schema.encodeUnknownSync(item.definition.data)(item.data as any) as Record<string, unknown>,
  }
}

function serializedWorkerEvent(
  aggregateID: string,
  seq: number,
  definition: Definition,
  extra: Record<string, unknown>,
): EventV2.SerializedEvent {
  const durable = definition.durable
  if (durable === undefined) throw new Error(`Worker workload definition is not durable: ${definition.type}`)
  const data = {
    sessionID: aggregateID,
    timestamp: DateTime.makeUnsafe(seq),
    ...extra,
  }
  return {
    id: eventID(aggregateID, seq) as any,
    type: EventV2.versionedType(definition.type, durable.version),
    aggregateID,
    seq,
    data: Schema.encodeUnknownSync(definition.data)(data as any) as Record<string, unknown>,
  }
}

function normalizeEvents(events: readonly unknown[]): readonly NormalizedWorkerEvent[] {
  return events.map((event) => {
    const payload = event as {
      readonly type: string
      readonly durable?: { readonly seq?: number }
      readonly data: {
        readonly reason?: unknown
        readonly phase?: unknown
        readonly force?: unknown
        readonly lease?: { readonly ownerID?: unknown }
        readonly error?: { readonly type?: unknown }
      }
    }
    return {
      seq: payload.durable?.seq ?? -1,
      type: payload.type,
      ...(payload.data.reason === undefined ? {} : { reason: String(payload.data.reason) }),
      ...(payload.data.phase === undefined ? {} : { phase: String(payload.data.phase) }),
      ...(payload.data.force === undefined ? {} : { force: Boolean(payload.data.force) }),
      ...(payload.data.lease?.ownerID === undefined ? {} : { leaseOwner: String(payload.data.lease.ownerID) }),
      ...(payload.data.error?.type === undefined ? {} : { errorType: String(payload.data.error.type) }),
    } satisfies NormalizedWorkerEvent
  })
}

function assertComparable(left: readonly NormalizedWorkerEvent[], right: readonly NormalizedWorkerEvent[], label: string) {
  const a = JSON.stringify(left)
  const b = JSON.stringify(right)
  if (a !== b) throw new Error(`${label} mismatch. sqlite=${a} postgres=${b}`)
}

async function cleanupPostgres(sql: Sql, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
  })
}

async function expectPromiseRejected(input: Promise<unknown>, message: string) {
  try {
    await input
  } catch {
    return
  }
  throw new Error(message)
}

async function expectEffectRejected(effect: Effect.Effect<unknown>, message: string) {
  await Effect.runPromise(
    Effect.gen(function* () {
      let rejected = false
      yield* effect.pipe(
        Effect.catchCauseIf(
          () => true,
          () =>
            Effect.sync(() => {
              rejected = true
            }),
        ),
      )
      if (!rejected) yield* Effect.die(new Error(message))
    }),
  )
}
