import { DateTime, Effect, Schema } from "effect"
import type { Sql } from "postgres"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { EventV2 } from "../../event"
import { Database } from "../database"
import { AppNodeBuilder } from "../../effect/app-node-builder"
import { LayerNode } from "../../effect/layer-node"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext, type TenantContext } from "./client"
import { make, unsupportedLiveEventCapabilityMessage } from "./event-v2-facade"

export type FacadeContractResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly checks: readonly string[]
  readonly events: readonly NormalizedEvent[]
}

type NormalizedEvent = {
  readonly seq: number
  readonly type: string
  readonly action: string
  readonly resourceID: string
  readonly index: number
}

const tenant = {
  tenantID: "tenant_event_v2_facade_contract",
  actorID: "actor_event_v2_facade_contract",
} satisfies TenantContext

export async function runEventV2FacadeContract(sql: Sql): Promise<FacadeContractResult> {
  const checks: string[] = []
  const aggregateID = `ses_pg_event_v2_facade_contract_${Date.now()}_${Math.random().toString(36).slice(2)}`

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("pg-migrations-applied")
  checks.push("pg-rls-ready")

  const sqlite = await runSqliteContract(aggregateID)
  checks.push(...sqlite.checks.map((check) => `sqlite-${check}`))

  const postgres = await runPostgresContract(sql, aggregateID)
  checks.push(...postgres.checks.map((check) => `postgres-${check}`))

  assertComparable(sqlite.events, postgres.events)
  checks.push("sqlite-postgres-contract-parity")

  await cleanupPostgres(sql, aggregateID)
  checks.push("postgres-cleanup-complete")

  return { status: "ok", aggregateID, checks, events: sqlite.events }
}

async function runSqliteContract(aggregateID: string) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-event-v2-facade-contract-"))
  const filename = path.join(temporary, "opencode.sqlite")
  try {
    const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(filename)],
    ])
    return await Effect.runPromise(
      Effect.gen(function* () {
        const checks: string[] = []
        const events = yield* EventV2.Service
        const { db } = yield* Database.Service

        const first = yield* events.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 0) as any, {
          id: eventID(aggregateID, 0) as any,
        })
        const second = yield* events.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 1) as any, {
          id: eventID(aggregateID, 1) as any,
        })
        assert(first.durable?.seq === 0, "SQLite publish should allocate seq 0")
        assert(second.durable?.seq === 1, "SQLite publish should allocate seq 1")
        checks.push("publish-seq-compatible")

        const page = yield* EventV2.readAggregate(db, { aggregateID, limit: 10, manifest: SessionDurable })
        assert(page.events.length === 2, "SQLite readAggregate should return two events")
        assert(page.hasMore === false, "SQLite readAggregate should not report hasMore")
        checks.push("readAggregate-compatible")

        const after = yield* EventV2.readAggregate(db, { aggregateID, after: 0, limit: 1, manifest: SessionDurable })
        assert(after.events.length === 1, "SQLite readAggregate after boundary should return one event")
        assert(after.events[0]?.durable?.seq === 1, "SQLite readAggregate after boundary should return seq 1")
        checks.push("readAggregate-after-boundary-compatible")

        const replayed = serializedAudit(aggregateID, 2)
        yield* events.replay(replayed, { ownerID: "event-v2-facade-contract" })
        yield* events.replay(replayed, { ownerID: "event-v2-facade-contract" })
        const afterReplay = yield* EventV2.readAggregate(db, { aggregateID, limit: 10, manifest: SessionDurable })
        assert(
          normalizeEvents(afterReplay.events).map((event) => event.seq).join(",") === "0,1,2",
          "SQLite replay should append once and remain idempotent",
        )
        checks.push("replay-idempotent-compatible")

        yield* expectEffectRejected(
          events.replay({ ...serializedAudit(aggregateID, 99), id: eventID(aggregateID, "conflict") as any, seq: 2 }),
          "SQLite divergent replay should be rejected",
        )
        checks.push("replay-conflict-rejected")

        yield* events.claim(aggregateID, "claimed-owner")
        yield* expectEffectRejected(
          events.replay(serializedAudit(aggregateID, 3), { ownerID: "wrong-owner", strictOwner: true }),
          "SQLite strict owner mismatch should be rejected",
        )
        yield* events.replay(serializedAudit(aggregateID, 3), { ownerID: "claimed-owner", strictOwner: true })
        checks.push("claim-compatible")

        const finalPage = yield* EventV2.readAggregate(db, { aggregateID, limit: 10, manifest: SessionDurable })
        const finalEvents = normalizeEvents(finalPage.events)
        assert(finalEvents.map((event) => event.seq).join(",") === "0,1,2,3", "SQLite final events should be contiguous")

        yield* events.remove(aggregateID)
        const afterRemove = yield* EventV2.readAggregate(db, { aggregateID, limit: 10, manifest: SessionDurable })
        assert(afterRemove.events.length === 0, "SQLite remove should clear aggregate events")
        checks.push("remove-compatible")

        return { checks, events: finalEvents }
      }).pipe(Effect.scoped, Effect.provide(layer)),
    )
  } finally {
    await fs.rm(temporary, { recursive: true, force: true })
  }
}

async function runPostgresContract(sql: Sql, aggregateID: string) {
  const checks: string[] = []
  const facade = make({ sql, tenant, ownerID: "event-v2-facade-contract" })

  const first = await facade.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 0) as any, {
    id: eventID(aggregateID, 0) as any,
  })
  const second = await facade.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 1) as any, {
    id: eventID(aggregateID, 1) as any,
  })
  assert(first.durable?.seq === 0, "PostgreSQL publish should allocate seq 0")
  assert(second.durable?.seq === 1, "PostgreSQL publish should allocate seq 1")
  checks.push("publish-seq-compatible")

  const page = await facade.readAggregate({ aggregateID, limit: 10, manifest: SessionDurable })
  assert(page.events.length === 2, "PostgreSQL readAggregate should return two events")
  assert(page.hasMore === false, "PostgreSQL readAggregate should not report hasMore")
  checks.push("readAggregate-compatible")

  const after = await facade.readAggregate({ aggregateID, after: 0, limit: 1, manifest: SessionDurable })
  assert(after.events.length === 1, "PostgreSQL readAggregate after boundary should return one event")
  assert(after.events[0]?.durable?.seq === 1, "PostgreSQL readAggregate after boundary should return seq 1")
  checks.push("readAggregate-after-boundary-compatible")

  const replayed = serializedAudit(aggregateID, 2)
  await facade.replay(replayed, { ownerID: "event-v2-facade-contract" })
  await facade.replay(replayed, { ownerID: "event-v2-facade-contract" })
  const afterReplay = await facade.readAggregate({ aggregateID, limit: 10, manifest: SessionDurable })
  assert(
    normalizeEvents(afterReplay.events).map((event) => event.seq).join(",") === "0,1,2",
    "PostgreSQL replay should append once and remain idempotent",
  )
  checks.push("replay-idempotent-compatible")

  await expectPromiseRejected(
    facade.replay({ ...serializedAudit(aggregateID, 99), id: eventID(aggregateID, "conflict") as any, seq: 2 }),
    "PostgreSQL divergent replay should be rejected",
  )
  checks.push("replay-conflict-rejected")

  await facade.claim(aggregateID, "claimed-owner")
  await expectPromiseRejected(
    facade.replay(serializedAudit(aggregateID, 3), { ownerID: "wrong-owner", strictOwner: true }),
    "PostgreSQL strict owner mismatch should be rejected",
  )
  await facade.replay(serializedAudit(aggregateID, 3), { ownerID: "claimed-owner", strictOwner: true })
  checks.push("claim-compatible")

  assertUnsupported(() => facade.subscribe())
  assertUnsupported(() => facade.all())
  assertUnsupported(() => facade.durable())
  assertUnsupported(() => facade.listen())
  assertUnsupported(() => facade.project())
  checks.push("live-event-capabilities-fail-closed")

  const finalPage = await facade.readAggregate({ aggregateID, limit: 10, manifest: SessionDurable })
  const finalEvents = normalizeEvents(finalPage.events)
  assert(finalEvents.map((event) => event.seq).join(",") === "0,1,2,3", "PostgreSQL final events should be contiguous")

  await facade.remove(aggregateID)
  const afterRemove = await facade.readAggregate({ aggregateID, limit: 10, manifest: SessionDurable })
  assert(afterRemove.events.length === 0, "PostgreSQL remove should clear aggregate events")
  checks.push("remove-compatible")

  return { checks, events: finalEvents }
}

function auditData(aggregateID: string, index: number) {
  return {
    sessionID: aggregateID,
    timestamp: DateTime.makeUnsafe(0),
    action: `event-v2-facade-contract.${index}`,
    resource: { type: "session", id: aggregateID },
    outcome: "success",
    tenant: {
      tenantID: tenant.tenantID,
      actorID: tenant.actorID,
      source: "env",
      mode: "multi-tenant",
    },
    metadata: { index },
  }
}

function serializedAudit(aggregateID: string, index: number): EventV2.SerializedEvent {
  return {
    id: eventID(aggregateID, index) as any,
    type: EventV2.versionedType(SessionEvent.Audit.Recorded.type, 1),
    aggregateID,
    seq: index,
    data: Schema.encodeUnknownSync(SessionEvent.Audit.Recorded.data)(auditData(aggregateID, index) as any) as Record<
      string,
      unknown
    >,
  }
}

function eventID(aggregateID: string, suffix: number | string) {
  return `evt_${aggregateID}_${suffix}`
}

function normalizeEvents(events: readonly unknown[]) {
  return events.map((event) => {
    const payload = event as {
      readonly durable?: { readonly seq?: number }
      readonly type: string
      readonly data: {
        readonly action?: unknown
        readonly resource?: { readonly id?: unknown }
        readonly metadata?: { readonly index?: unknown }
      }
    }
    return {
      seq: payload.durable?.seq ?? -1,
      type: payload.type,
      action: String(payload.data.action),
      resourceID: String(payload.data.resource?.id),
      index: Number(payload.data.metadata?.index),
    } satisfies NormalizedEvent
  })
}

function assertComparable(sqliteEvents: readonly NormalizedEvent[], pgEvents: readonly NormalizedEvent[]) {
  const sqlite = JSON.stringify(sqliteEvents)
  const pg = JSON.stringify(pgEvents)
  if (sqlite !== pg) throw new Error(`EventV2 facade contract mismatch. sqlite=${sqlite} postgres=${pg}`)
}

async function cleanupPostgres(sql: Sql, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
  })
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function expectEffectRejected(effect: Effect.Effect<unknown>, message: string) {
  return Effect.gen(function* () {
    let rejected = false
    yield* effect.pipe(Effect.catchCauseIf(() => true, () => Effect.sync(() => {
      rejected = true
    })))
    if (!rejected) yield* Effect.die(new Error(message))
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

function assertUnsupported(call: () => unknown) {
  try {
    call()
  } catch (error) {
    if (error instanceof Error && error.message === unsupportedLiveEventCapabilityMessage) return
    throw error
  }
  throw new Error("PostgreSQL EventV2 facade live capability should fail closed")
}
