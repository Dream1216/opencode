import type { Sql } from "postgres"
import { applyMigrations, assertRlsReady } from "./migration"
import { append, latestSequence, readAggregate } from "./event-store"
import type { TenantContext } from "./client"

export type EventStoreSmokeResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly checks: readonly string[]
}

export async function runEventStoreSmoke(sql: Sql): Promise<EventStoreSmokeResult> {
  const checks: string[] = []
  const aggregateID = `pg_event_store_smoke_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantA = { tenantID: "tenant_event_store_smoke_a", actorID: "actor_event_store_smoke_a" } satisfies TenantContext
  const tenantB = { tenantID: "tenant_event_store_smoke_b", actorID: "actor_event_store_smoke_b" } satisfies TenantContext

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("migrations-applied")
  checks.push("rls-ready")

  const first = await append(sql, {
    tenant: tenantA,
    id: `${aggregateID}_a_0`,
    aggregateID,
    type: "session.next.audit.recorded.1",
    data: { sessionID: aggregateID, action: "event-store-smoke.first" },
    ownerID: "owner-a",
  })
  const second = await append(sql, {
    tenant: tenantA,
    id: `${aggregateID}_a_1`,
    aggregateID,
    type: "session.next.audit.recorded.1",
    data: { sessionID: aggregateID, action: "event-store-smoke.second" },
    ownerID: "owner-a",
  })
  assert(first.seq === 0 && second.seq === 1, "serial append must allocate contiguous seq")
  checks.push("serial-append-contiguous")

  const concurrent = await Promise.all(
    Array.from({ length: 8 }, (_, index) =>
      append(sql, {
        tenant: tenantA,
        id: `${aggregateID}_a_concurrent_${index}`,
        aggregateID,
        type: "session.next.audit.recorded.1",
        data: { sessionID: aggregateID, action: "event-store-smoke.concurrent", index },
        ownerID: "owner-a",
      }),
    ),
  )
  const concurrentSeq = concurrent.map((event) => event.seq).sort((a, b) => a - b)
  assert(JSON.stringify(concurrentSeq) === JSON.stringify([2, 3, 4, 5, 6, 7, 8, 9]), "concurrent append seq must be gap-free")
  checks.push("concurrent-append-gap-free")

  const tenantAEvents = await readAggregate(sql, { tenant: tenantA, aggregateID, limit: 20 })
  assert(tenantAEvents.length === 10, "tenant A should read 10 own events")
  assert(tenantAEvents.map((event) => event.seq).join(",") === "0,1,2,3,4,5,6,7,8,9", "tenant A read must be ordered")
  checks.push("read-ordered")

  const tenantBFirst = await append(sql, {
    tenant: tenantB,
    id: `${aggregateID}_b_0`,
    aggregateID,
    type: "session.next.audit.recorded.1",
    data: { sessionID: aggregateID, action: "event-store-smoke.tenant-b" },
    ownerID: "owner-b",
  })
  assert(tenantBFirst.seq === 0, "tenant B sequence must be independent")
  const tenantBEvents = await readAggregate(sql, { tenant: tenantB, aggregateID, limit: 20 })
  assert(tenantBEvents.length === 1 && tenantBEvents[0]?.seq === 0, "tenant B should only read own event")
  checks.push("tenant-sequence-isolated")

  assert((await latestSequence(sql, tenantA, aggregateID)) === 9, "tenant A latest seq should be 9")
  assert((await latestSequence(sql, tenantB, aggregateID)) === 0, "tenant B latest seq should be 0")
  checks.push("latest-sequence-isolated")

  const missingTenantRows = await sql<{ id: string }[]>`
    select id
    from event
    where aggregate_id = ${aggregateID}
  `
  assert(missingTenantRows.length === 0, "missing tenant context must not read events")
  checks.push("missing-tenant-read-fails-closed")

  await cleanup(sql, tenantA, aggregateID)
  await cleanup(sql, tenantB, aggregateID)
  checks.push("cleanup-completed")

  return { status: "ok", aggregateID, checks }
}

async function cleanup(sql: Sql, tenant: TenantContext, aggregateID: string) {
  await sql.begin(async (tx) => {
    await tx`select set_config('opencode.tenant_id', ${tenant.tenantID}, true)`
    await tx`select set_config('opencode.actor_id', ${tenant.actorID ?? ""}, true)`
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
  })
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}
