import type { Sql } from "postgres"
import { DateTime, Schema } from "effect"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { EventV2 } from "../../event"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext, type TenantContext } from "./client"
import { make } from "./event-v2-facade"

export type FacadeSmokeResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly checks: readonly string[]
}

const tenant = {
  tenantID: "tenant_event_v2_facade_smoke",
  actorID: "actor_event_v2_facade_smoke",
} satisfies TenantContext

export async function runEventV2FacadeSmoke(sql: Sql): Promise<FacadeSmokeResult> {
  const checks: string[] = []
  const aggregateID = `ses_pg_event_v2_facade_smoke_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const facade = make({ sql, tenant, ownerID: "event-v2-facade-smoke" })

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("pg-migrations-applied")
  checks.push("pg-rls-ready")

  const first = await facade.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 0) as any, {
    id: eventID(aggregateID, 0) as any,
  })
  const second = await facade.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, 1) as any, {
    id: eventID(aggregateID, 1) as any,
  })
  assert(first.durable?.seq === 0, "first facade publish should allocate seq 0")
  assert(second.durable?.seq === 1, "second facade publish should allocate seq 1")
  checks.push("publish-seq-compatible")

  const page = await facade.readAggregate({
    aggregateID,
    limit: 10,
    manifest: SessionDurable,
  })
  assert(page.hasMore === false, "readAggregate should not report hasMore for two events")
  assert(page.events.length === 2, "readAggregate should return two events")
  assert(page.events.map((event) => event.durable?.seq).join(",") === "0,1", "readAggregate should preserve seq order")
  assert(page.events.map((event) => event.type).every((type) => type === SessionEvent.Audit.Recorded.type), "readAggregate should decode logical event type")
  checks.push("readAggregate-compatible")

  const after = await facade.readAggregate({
    aggregateID,
    after: 0,
    limit: 1,
    manifest: SessionDurable,
  })
  assert(after.events.length === 1, "readAggregate after boundary should return one event")
  assert(after.events[0]?.durable?.seq === 1, "readAggregate after boundary should return seq 1")
  checks.push("readAggregate-after-boundary-compatible")

  const replayed = serializedAudit(aggregateID, 2)
  await facade.replay(replayed, { ownerID: "event-v2-facade-smoke" })
  await facade.replay(replayed, { ownerID: "event-v2-facade-smoke" })
  const afterReplay = await facade.readAggregate({
    aggregateID,
    limit: 10,
    manifest: SessionDurable,
  })
  assert(
    afterReplay.events.map((event) => event.durable?.seq).join(",") === "0,1,2",
    "replay should append once and remain idempotent",
  )
  checks.push("replay-compatible")

  await expectRejected(
    facade.replay({ ...serializedAudit(aggregateID, 99), id: eventID(aggregateID, "conflict") as any, seq: 2 }),
    "divergent replay should be rejected",
  )
  checks.push("replay-conflict-rejected")

  await facade.claim(aggregateID, "claimed-owner")
  await expectRejected(
    facade.replay(serializedAudit(aggregateID, 3), { ownerID: "wrong-owner", strictOwner: true }),
    "strict owner mismatch should be rejected",
  )
  await facade.replay(serializedAudit(aggregateID, 3), { ownerID: "claimed-owner", strictOwner: true })
  checks.push("claim-compatible")

  await facade.remove(aggregateID)
  const afterRemove = await facade.readAggregate({
    aggregateID,
    limit: 10,
    manifest: SessionDurable,
  })
  assert(afterRemove.events.length === 0, "remove should clear aggregate events")
  checks.push("remove-compatible")

  return { status: "ok", aggregateID, checks }
}

function auditData(aggregateID: string, index: number) {
  return {
    sessionID: aggregateID,
    timestamp: DateTime.makeUnsafe(0),
    action: `event-v2-facade-smoke.${index}`,
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

async function cleanup(sql: Sql, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
  })
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function expectRejected(input: Promise<unknown>, message: string) {
  try {
    await input
  } catch {
    return
  }
  throw new Error(message)
}
