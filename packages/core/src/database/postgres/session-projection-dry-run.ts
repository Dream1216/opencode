import { DateTime, Effect } from "effect"
import type { Sql } from "postgres"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { Prompt } from "@opencode-ai/schema/prompt"
import { SessionEvent } from "../../session/event"
import { SessionMessage } from "../../session/message"
import { EventV2 } from "../../event"
import { Database } from "../database"
import { AppNodeBuilder } from "../../effect/app-node-builder"
import { LayerNode } from "../../effect/layer-node"
import { applyMigrations, assertRlsReady } from "./migration"
import { setTenantContext, type TenantContext } from "./client"
import { make } from "./event-v2-facade"
import {
  dryRunSessionProjection,
  readShadowProjection,
  removeShadowProjection,
  writeShadowProjection,
  type SessionProjectionSummary,
} from "./session-projection-shadow"

export type SessionProjectionDryRunResult = {
  readonly status: "ok"
  readonly aggregateID: string
  readonly checks: readonly string[]
  readonly projection: SessionProjectionSummary
}

const tenant = {
  tenantID: "tenant_session_projection_dry_run",
  actorID: "actor_session_projection_dry_run",
} satisfies TenantContext

export async function runSessionProjectionDryRun(sql: Sql): Promise<SessionProjectionDryRunResult> {
  const checks: string[] = []
  const aggregateID = `ses_pg_session_projection_dry_run_${Date.now()}_${Math.random().toString(36).slice(2)}`

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("pg-migrations-applied")
  checks.push("pg-rls-ready")

  const sqliteProjection = dryRunSessionProjection(await runSqliteWorkload(aggregateID))
  checks.push("sqlite-dry-run-projected")

  const pgProjection = dryRunSessionProjection(await runPostgresWorkload(sql, aggregateID))
  checks.push("postgres-dry-run-projected")

  assertComparable(sqliteProjection, pgProjection)
  checks.push("sqlite-postgres-dry-run-parity")

  await writeShadowProjection(sql, { tenant, source: "postgres-alpha", projection: pgProjection })
  const stored = await readShadowProjection(sql, { tenant, source: "postgres-alpha", aggregateID })
  assert(stored !== undefined, "shadow projection should be readable")
  assert(stored.projection_hash === pgProjection.hash, "shadow projection hash should match dry-run output")
  assert(Number(stored.message_count) === pgProjection.messageCount, "shadow projection message count should match")
  checks.push("postgres-shadow-written")

  await removeShadowProjection(sql, tenant, aggregateID)
  checks.push("postgres-shadow-cleanup-complete")
  await cleanupPostgres(sql, aggregateID)
  checks.push("postgres-event-cleanup-complete")

  return { status: "ok", aggregateID, checks, projection: pgProjection }
}

async function runSqliteWorkload(aggregateID: string) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-session-projection-dry-run-"))
  const filename = path.join(temporary, "opencode.sqlite")
  try {
    const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node]), [
      [Database.node, Database.layerFromPath(filename)],
    ])
    return await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        for (const item of workload(aggregateID)) {
          yield* events.publish(item.definition, item.data as any, { id: item.id as any })
        }
        const { db } = yield* Database.Service
        const page = yield* EventV2.readAggregate(db, { aggregateID, limit: 20, manifest: SessionDurable })
        return page.events
      }).pipe(Effect.scoped, Effect.provide(layer)),
    )
  } finally {
    await fs.rm(temporary, { recursive: true, force: true })
  }
}

async function runPostgresWorkload(sql: Sql, aggregateID: string) {
  const facade = make({ sql, tenant, ownerID: "session-projection-dry-run" })
  for (const item of workload(aggregateID)) {
    await facade.publish(item.definition, item.data as any, { id: item.id as any })
  }
  const page = await facade.readAggregate({ aggregateID, limit: 20, manifest: SessionDurable })
  return page.events
}

function workload(aggregateID: string) {
  const firstPrompt = Prompt.make({ text: "shadow projection first" })
  const secondPrompt = Prompt.make({ text: "shadow projection second" })
  return [
    {
      definition: SessionEvent.PromptAdmitted,
      id: eventID(aggregateID, 0),
      data: {
        sessionID: aggregateID,
        messageID: SessionMessage.ID.make("msg_shadow_first"),
        timestamp: DateTime.makeUnsafe(0),
        prompt: firstPrompt,
        delivery: "steer",
      },
    },
    {
      definition: SessionEvent.Prompted,
      id: eventID(aggregateID, 1),
      data: {
        sessionID: aggregateID,
        messageID: SessionMessage.ID.make("msg_shadow_first"),
        timestamp: DateTime.makeUnsafe(1),
        prompt: firstPrompt,
        delivery: "steer",
      },
    },
    {
      definition: SessionEvent.PromptAdmitted,
      id: eventID(aggregateID, 2),
      data: {
        sessionID: aggregateID,
        messageID: SessionMessage.ID.make("msg_shadow_second"),
        timestamp: DateTime.makeUnsafe(2),
        prompt: secondPrompt,
        delivery: "queue",
      },
    },
    {
      definition: SessionEvent.Prompted,
      id: eventID(aggregateID, 3),
      data: {
        sessionID: aggregateID,
        messageID: SessionMessage.ID.make("msg_shadow_second"),
        timestamp: DateTime.makeUnsafe(3),
        prompt: secondPrompt,
        delivery: "queue",
      },
    },
  ] as const
}

function eventID(aggregateID: string, index: number) {
  return `evt_${aggregateID}_projection_${index}`
}

function assertComparable(sqlite: SessionProjectionSummary, postgres: SessionProjectionSummary) {
  const left = JSON.stringify({ ...sqlite, hash: undefined })
  const right = JSON.stringify({ ...postgres, hash: undefined })
  if (left !== right) throw new Error(`Session projection dry-run mismatch. sqlite=${left} postgres=${right}`)
}

async function cleanupPostgres(sql: Sql, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`delete from event where aggregate_id = ${aggregateID}`
    await tx`delete from event_sequence where aggregate_id = ${aggregateID}`
    await tx`delete from pg_session_projection_shadow where aggregate_id = ${aggregateID}`
  })
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}
