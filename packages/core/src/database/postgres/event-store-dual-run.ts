import { Effect, DateTime, Stream } from "effect"
import type { Sql } from "postgres"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { EventV2 } from "../../event"
import { Database } from "../database"
import { AppNodeBuilder } from "../../effect/app-node-builder"
import { LayerNode } from "../../effect/layer-node"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { append, readAggregate, type StoredEvent } from "./event-store"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext, type TenantContext } from "./client"

export type DualRunResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly checks: readonly string[]
  readonly events: readonly NormalizedEvent[]
}

type WorkloadEvent = {
  readonly id: string
  readonly action: string
  readonly index: number
}

type NormalizedEvent = {
  readonly seq: number
  readonly type: string
  readonly action: string
  readonly resourceID: string
  readonly index: number
}

const tenant = {
  tenantID: "tenant_event_store_dual_run",
  actorID: "actor_event_store_dual_run",
} satisfies TenantContext

export async function runEventStoreDualRun(sql: Sql): Promise<DualRunResult> {
  const checks: string[] = []
  const aggregateID = `ses_pg_event_store_dual_run_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const workload = Array.from({ length: 5 }, (_, index) => ({
    id: `evt_${aggregateID}_event_${index}`,
    action: `dual-run.event.${index}`,
    index,
  }))

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("pg-migrations-applied")
  checks.push("pg-rls-ready")

  const sqliteEvents = await runSqliteWorkload(aggregateID, workload)
  checks.push("sqlite-workload-complete")

  const pgEvents = await runPostgresWorkload(sql, aggregateID, workload)
  checks.push("postgres-workload-complete")

  assertComparable(sqliteEvents, pgEvents)
  checks.push("seq-order-type-data-parity")

  await cleanupPostgres(sql, aggregateID)
  checks.push("postgres-cleanup-complete")

  return { status: "ok", aggregateID, checks, events: sqliteEvents }
}

async function runSqliteWorkload(aggregateID: string, workload: readonly WorkloadEvent[]) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-event-store-dual-run-"))
  const filename = path.join(temporary, "opencode.sqlite")
  try {
    const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(filename)],
    ])
    return await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        for (const item of workload) {
          yield* events.publish(SessionEvent.Audit.Recorded, auditData(aggregateID, item) as any, { id: item.id as any })
        }
        const stored = Array.from(
          yield* events.durable({ aggregateID }).pipe(Stream.take(workload.length), Stream.runCollect),
        )
        return stored.map((event) =>
          normalize({
            seq: event.durable?.seq ?? -1,
            type: event.type,
            data: event.data as Record<string, unknown>,
          }),
        )
      }).pipe(Effect.scoped, Effect.provide(layer)),
    )
  } finally {
    await fs.rm(temporary, { recursive: true, force: true })
  }
}

async function runPostgresWorkload(sql: Sql, aggregateID: string, workload: readonly WorkloadEvent[]) {
  for (const item of workload) {
    await append(sql, {
      tenant,
      id: item.id,
      aggregateID,
      type: EventV2.versionedType(SessionEvent.Audit.Recorded.type, 1),
      data: auditData(aggregateID, item),
      ownerID: "event-store-dual-run",
    })
  }
  const stored = await readAggregate(sql, { tenant, aggregateID, limit: workload.length })
  return stored.map(normalizeStored)
}

function auditData(aggregateID: string, item: WorkloadEvent) {
  return {
    sessionID: aggregateID,
    timestamp: DateTime.makeUnsafe(0),
    action: item.action,
    resource: { type: "session", id: aggregateID },
    outcome: "success",
    tenant: {
      tenantID: tenant.tenantID,
      actorID: tenant.actorID,
      source: "env",
      mode: "multi-tenant",
    },
    metadata: { index: item.index },
  }
}

function normalizeStored(event: StoredEvent) {
  return normalize({
    seq: event.seq,
    type: event.type === EventV2.versionedType(SessionEvent.Audit.Recorded.type, 1) ? SessionEvent.Audit.Recorded.type : event.type,
    data: event.data,
  })
}

function normalize(input: { readonly seq: number; readonly type: string; readonly data: Record<string, unknown> }) {
  const resource = input.data.resource as { readonly id?: string } | undefined
  const metadata = input.data.metadata as { readonly index?: number } | undefined
  return {
    seq: input.seq,
    type: input.type,
    action: String(input.data.action),
    resourceID: String(resource?.id),
    index: Number(metadata?.index),
  } satisfies NormalizedEvent
}

function assertComparable(sqliteEvents: readonly NormalizedEvent[], pgEvents: readonly NormalizedEvent[]) {
  const sqlite = JSON.stringify(sqliteEvents)
  const pg = JSON.stringify(pgEvents)
  if (sqlite !== pg) throw new Error(`EventStore dual-run mismatch. sqlite=${sqlite} postgres=${pg}`)
}

async function cleanupPostgres(sql: Sql, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
  })
}
