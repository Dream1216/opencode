import type { Sql } from "postgres"
import { setTenantContext, type TenantContext } from "./client"
import type { StoredEvent } from "./event-store"

export type PendingQuestionInput = {
  readonly tenant: TenantContext
  readonly askedType: string
  readonly terminalTypes: readonly string[]
  readonly location: {
    readonly directory: string
    readonly workspaceID?: string
  }
}

type EventRow = {
  readonly id: string
  readonly tenant_id: string
  readonly actor_id: string | null
  readonly aggregate_id: string
  readonly seq: string | number
  readonly type: string
  readonly data: Record<string, unknown>
}

/**
 * Discovers pending Question aggregates from the tenant-scoped PostgreSQL
 * EventStore. The event payload carries Location because EventStore rows do not
 * persist live-event metadata.
 */
export async function readPendingQuestionAskedEvents(
  sql: Sql,
  input: PendingQuestionInput,
): Promise<StoredEvent[]> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const rows = await tx<EventRow[]>`
      select
        asked.id,
        asked.tenant_id,
        asked.actor_id,
        asked.aggregate_id,
        asked.seq,
        asked.type,
        asked.data
      from event asked
      where asked.type = ${input.askedType}
        and asked.data -> 'location' ->> 'directory' = ${input.location.directory}
        and coalesce(asked.data -> 'location' ->> 'workspaceID', '') = ${input.location.workspaceID ?? ""}
        and not exists (
          select 1
          from event terminal
          where terminal.tenant_id = asked.tenant_id
            and terminal.aggregate_id = asked.aggregate_id
            and terminal.type in ${tx(input.terminalTypes)}
        )
      order by asked.aggregate_id asc, asked.seq asc
    `
    return rows.map((row) => ({
      id: row.id,
      tenantID: row.tenant_id,
      ...(row.actor_id === null ? {} : { actorID: row.actor_id }),
      aggregateID: row.aggregate_id,
      seq: Number(row.seq),
      type: row.type,
      data: row.data,
    }))
  })
}
