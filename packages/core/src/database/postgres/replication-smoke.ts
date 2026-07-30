import { DateTime, Effect } from "effect"
import type { Sql } from "postgres"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { EventV2 } from "../../event"
import { Database } from "../database"
import { AppNodeBuilder } from "../../effect/app-node-builder"
import { LayerNode } from "../../effect/layer-node"
import { applyMigrations } from "./migration"
import { latestSequence, remove } from "./event-store"
import {
  dispatchOutboxRow,
  drainReplicationOutbox,
  reconcileReplicationOutbox,
  type OutboxRow,
} from "./replication-worker"

export async function runReplicationSmoke(pg: Sql) {
  const checks: string[] = []
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-pg-replication-"))
  const filename = path.join(temporary, "opencode.sqlite")
  const prefix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const appendAggregate = `ses_pg_replication_append_${prefix}`
  const lifecycleAggregate = `ses_pg_replication_lifecycle_${prefix}`
  const failureAggregate = `ses_pg_replication_failure_${prefix}`
  const tenant = { tenantID: "tenant_pg_replication_smoke", actorID: "actor_pg_replication_smoke" }
  const restore = setReplicationEnv(tenant)
  await applyMigrations(pg)
  try {
    const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(filename)],
    ])
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        const { db } = yield* Database.Service
        yield* events.publish(SessionEvent.Audit.Recorded, auditData(appendAggregate, tenant, 0) as any, {
          id: eventID(appendAggregate, 0) as any,
        })
        yield* events.publish(SessionEvent.Audit.Recorded, auditData(appendAggregate, tenant, 1) as any, {
          id: eventID(appendAggregate, 1) as any,
        })
        yield* events.publish(SessionEvent.Audit.Recorded, auditData(lifecycleAggregate, tenant, 0) as any, {
          id: eventID(lifecycleAggregate, 0) as any,
        })
        yield* events.claim(lifecycleAggregate, "replication-smoke-owner")
        yield* events.remove(lifecycleAggregate)
        yield* events.publish(SessionEvent.Audit.Recorded, auditData(failureAggregate, tenant, 0) as any, {
          id: eventID(failureAggregate, 0) as any,
        })
        checks.push("sqlite-transactional-outbox-written")

        const first = yield* Effect.tryPromise(() =>
          drainReplicationOutbox(db, pg, {
            batchSize: 5,
            dispatch: (row) =>
              row.aggregate_id === failureAggregate
                ? Promise.reject(new Error("forced replication failure"))
                : dispatchOutboxRow(pg, row),
          }),
        )
        if (first.applied !== 5) throw new Error(`Expected five applied replication operations, got ${first.applied}`)
        checks.push("append-claim-remove-drained")

        const second = yield* Effect.tryPromise(() =>
          drainReplicationOutbox(db, pg, {
            batchSize: 10,
            maxAttempts: 1,
            dispatch: (row: OutboxRow) =>
              row.aggregate_id === failureAggregate
                ? Promise.reject(new Error("forced dead letter"))
                : dispatchOutboxRow(pg, row),
          }),
        )
        if (second.deadLetter !== 1) throw new Error("Forced replication failure did not enter dead-letter state")
        checks.push("dead-letter-recorded")

        const reconciliation = yield* Effect.tryPromise(() =>
          reconcileReplicationOutbox(db, pg, { aggregateIDs: [appendAggregate] }),
        )
        if (reconciliation.status !== "ok" || reconciliation.compared !== 2) {
          throw new Error(`Replication reconciliation failed: ${reconciliation.divergences.join(", ")}`)
        }
        checks.push("sqlite-postgres-reconciliation-passed")
        if (reconciliation.metrics.deadLetter !== 1) throw new Error("Replication metrics did not report dead letter")
        checks.push("lag-and-dead-letter-metrics-ready")
        return reconciliation.metrics
      }).pipe(Effect.scoped, Effect.provide(layer)),
    )
    if ((await latestSequence(pg, tenant, lifecycleAggregate)) !== -1) {
      throw new Error("Replicated remove operation did not clear the PostgreSQL aggregate")
    }
    checks.push("replicated-remove-verified")
    await remove(pg, tenant, appendAggregate)
    await remove(pg, tenant, lifecycleAggregate)
    await remove(pg, tenant, failureAggregate)
    return { status: "ok" as const, checks, metrics: result }
  } finally {
    restore()
    await fs.rm(temporary, { recursive: true, force: true })
  }
}

function auditData(
  aggregateID: string,
  tenant: { readonly tenantID: string; readonly actorID: string },
  index: number,
) {
  return {
    sessionID: aggregateID,
    timestamp: DateTime.makeUnsafe(index),
    action: `postgres-replication-smoke.${index}`,
    resource: { type: "session", id: aggregateID },
    outcome: "success",
    tenant: { ...tenant, source: "env", mode: "multi-tenant" },
    metadata: { index },
  }
}

function eventID(aggregateID: string, index: number) {
  return `evt_${aggregateID}_${index}`
}

function setReplicationEnv(tenant: { readonly tenantID: string; readonly actorID: string }) {
  const keys = [
    "OPENCODE_DATABASE_BACKEND",
    "OPENCODE_TENANT_ID",
    "OPENCODE_ACTOR_ID",
    "OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED",
  ] as const
  const previous = new Map(keys.map((key) => [key, process.env[key]]))
  process.env.OPENCODE_DATABASE_BACKEND = "postgres-alpha"
  process.env.OPENCODE_TENANT_ID = tenant.tenantID
  process.env.OPENCODE_ACTOR_ID = tenant.actorID
  process.env.OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED = "1"
  return () => {
    for (const key of keys) {
      const value = previous.get(key)
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}
