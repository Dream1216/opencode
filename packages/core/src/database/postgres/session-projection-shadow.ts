import { createHash } from "node:crypto"
import { DateTime, Effect } from "effect"
import type { Sql } from "postgres"
import { SessionEvent } from "../../session/event"
import { SessionMessageUpdater } from "../../session/message-updater"
import type { SessionMessage } from "../../session/message"
import { setTenantContext, type TenantContext } from "./client"

export type NormalizedProjectionMessage = {
  readonly id: string
  readonly type: string
  readonly text?: string
  readonly contentCount?: number
}

export type SessionProjectionSummary = {
  readonly aggregateID: string
  readonly sessionID: string
  readonly lastSeq: number
  readonly eventCount: number
  readonly messageCount: number
  readonly inputAdmittedCount: number
  readonly inputPromotedCount: number
  readonly messages: readonly NormalizedProjectionMessage[]
  readonly hash: string
}

export type ShadowProjectionRow = {
  readonly tenant_id: string
  readonly session_id: string
  readonly aggregate_id: string
  readonly source: string
  readonly last_seq: string | number
  readonly event_count: string | number
  readonly message_count: string | number
  readonly input_admitted_count: string | number
  readonly input_promoted_count: string | number
  readonly projection: Record<string, unknown>
  readonly projection_hash: string
}

export function dryRunSessionProjection(events: readonly SessionEvent.DurableEvent[]): SessionProjectionSummary {
  if (events.length === 0) throw new Error("Session projection dry-run requires at least one event")
  const state: SessionMessageUpdater.MemoryState = { messages: [] }
  let sessionID: string | undefined
  let aggregateID: string | undefined
  let lastSeq = -1
  let inputAdmittedCount = 0
  let inputPromotedCount = 0
  for (const event of events) {
    if (event.durable === undefined) throw new Error(`Session projection dry-run received non-durable event ${event.type}`)
    sessionID = String(event.data.sessionID)
    aggregateID = event.durable.aggregateID
    lastSeq = event.durable.seq
    if (event.type === SessionEvent.PromptAdmitted.type) inputAdmittedCount++
    if (event.type === SessionEvent.Prompted.type) inputPromotedCount++
    Effect.runSync(SessionMessageUpdater.update(SessionMessageUpdater.memory(state), event))
  }
  const messages = state.messages.map(normalizeMessage)
  const withoutHash = {
    aggregateID: aggregateID ?? "",
    sessionID: sessionID ?? "",
    lastSeq,
    eventCount: events.length,
    messageCount: messages.length,
    inputAdmittedCount,
    inputPromotedCount,
    messages,
  }
  return { ...withoutHash, hash: stableHash(withoutHash) }
}

export async function writeShadowProjection(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly source: string
    readonly projection: SessionProjectionSummary
  },
) {
  const now = Date.now()
  await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    await tx`
      insert into pg_session_projection_shadow (
        tenant_id,
        session_id,
        aggregate_id,
        source,
        last_seq,
        event_count,
        message_count,
        input_admitted_count,
        input_promoted_count,
        projection,
        projection_hash,
        time_created,
        time_updated
      )
      values (
        ${input.tenant.tenantID},
        ${input.projection.sessionID},
        ${input.projection.aggregateID},
        ${input.source},
        ${input.projection.lastSeq},
        ${input.projection.eventCount},
        ${input.projection.messageCount},
        ${input.projection.inputAdmittedCount},
        ${input.projection.inputPromotedCount},
        ${tx.json(input.projection as any)},
        ${input.projection.hash},
        ${now},
        ${now}
      )
      on conflict (tenant_id, source, aggregate_id) do update set
        session_id = excluded.session_id,
        last_seq = excluded.last_seq,
        event_count = excluded.event_count,
        message_count = excluded.message_count,
        input_admitted_count = excluded.input_admitted_count,
        input_promoted_count = excluded.input_promoted_count,
        projection = excluded.projection,
        projection_hash = excluded.projection_hash,
        time_updated = excluded.time_updated
    `
  })
}

export async function readShadowProjection(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly source: string
    readonly aggregateID: string
  },
) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const rows = await tx<ShadowProjectionRow[]>`
      select
        tenant_id,
        session_id,
        aggregate_id,
        source,
        last_seq,
        event_count,
        message_count,
        input_admitted_count,
        input_promoted_count,
        projection,
        projection_hash
      from pg_session_projection_shadow
      where source = ${input.source}
        and aggregate_id = ${input.aggregateID}
      limit 1
    `
    return rows[0]
  })
}

export async function removeShadowProjection(sql: Sql, tenant: TenantContext, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`
      delete from pg_session_projection_shadow
      where aggregate_id = ${aggregateID}
    `
  })
}

function normalizeMessage(message: SessionMessage.Message): NormalizedProjectionMessage {
  if (message.type === "user") return { id: message.id, type: message.type, text: message.text }
  if (message.type === "system") return { id: message.id, type: message.type, text: message.text }
  if (message.type === "synthetic") return { id: message.id, type: message.type, text: message.text }
  if (message.type === "assistant") return { id: message.id, type: message.type, contentCount: message.content.length }
  return { id: message.id, type: message.type }
}

function stableHash(input: unknown) {
  return createHash("sha256").update(stableStringify(input)).digest("hex")
}

function stableStringify(input: unknown): string {
  if (input === null || typeof input !== "object") return JSON.stringify(input)
  if (Array.isArray(input)) return `[${input.map(stableStringify).join(",")}]`
  if (DateTime.isDateTime(input)) return JSON.stringify(DateTime.toEpochMillis(input))
  const record = input as Record<string, unknown>
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
    .join(",")}}`
}
