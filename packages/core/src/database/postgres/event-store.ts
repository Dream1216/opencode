import type { Sql } from "postgres"
import { isDeepStrictEqual } from "node:util"
import { setTenantContext, type TenantContext } from "./client"

export type AppendInput = {
  readonly tenant: TenantContext
  readonly id: string
  readonly aggregateID: string
  readonly type: string
  readonly data: Record<string, unknown>
  readonly ownerID?: string
  readonly expectedSeq?: number
}

export class SequenceConflictError extends Error {
  readonly code = "event_sequence_conflict"

  constructor(
    readonly aggregateID: string,
    readonly expectedSeq: number,
    readonly actualSeq: number,
  ) {
    super(`Event sequence conflict for aggregate ${aggregateID}: expected ${expectedSeq}, got ${actualSeq}`)
    this.name = "PostgresEventStore.SequenceConflictError"
  }
}

export type StoredEvent = {
  readonly id: string
  readonly tenantID: string
  readonly actorID?: string
  readonly aggregateID: string
  readonly seq: number
  readonly type: string
  readonly data: Record<string, unknown>
}

export type ReadInput = {
  readonly tenant: TenantContext
  readonly aggregateID: string
  readonly after?: number
  readonly limit?: number
  readonly types?: readonly string[]
}

export type ReplayInput = {
  readonly tenant: TenantContext
  readonly id: string
  readonly aggregateID: string
  readonly seq: number
  readonly type: string
  readonly data: Record<string, unknown>
  readonly ownerID?: string
  readonly strictOwner?: boolean
}

export type ReplayResult = {
  readonly event: StoredEvent
  readonly committed: boolean
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

export async function append(sql: Sql, input: AppendInput): Promise<StoredEvent> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    await tx`
      insert into event_sequence (tenant_id, aggregate_id, seq, owner_id)
      values (${input.tenant.tenantID}, ${input.aggregateID}, -1, ${input.ownerID ?? null})
      on conflict (tenant_id, aggregate_id) do nothing
    `
    const sequence = await tx<{ seq: string | number; owner_id: string | null }[]>`
      select seq, owner_id
      from event_sequence
      where tenant_id = ${input.tenant.tenantID}
        and aggregate_id = ${input.aggregateID}
      for update
    `
    const current = sequence[0]
    if (current === undefined) throw new Error(`Unable to lock event sequence for aggregate ${input.aggregateID}`)
    const actualSeq = Number(current.seq)
    if (input.expectedSeq !== undefined && input.expectedSeq !== actualSeq) {
      throw new SequenceConflictError(input.aggregateID, input.expectedSeq, actualSeq)
    }
    const seq = actualSeq + 1
    await tx`
      insert into event (id, tenant_id, actor_id, aggregate_id, seq, type, data)
      values (
        ${input.id},
        ${input.tenant.tenantID},
        ${input.tenant.actorID ?? null},
        ${input.aggregateID},
        ${seq},
        ${input.type},
        ${tx.json(input.data as any)}
      )
    `
    await tx`
      update event_sequence
      set seq = ${seq},
          owner_id = ${input.ownerID ?? current.owner_id}
      where tenant_id = ${input.tenant.tenantID}
        and aggregate_id = ${input.aggregateID}
    `
    return {
      id: input.id,
      tenantID: input.tenant.tenantID,
      ...(input.tenant.actorID === undefined ? {} : { actorID: input.tenant.actorID }),
      aggregateID: input.aggregateID,
      seq,
      type: input.type,
      data: input.data,
    }
  })
}

export async function readAggregate(sql: Sql, input: ReadInput): Promise<StoredEvent[]> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    const after = input.after ?? -1
    const limit = input.limit ?? 100
    const rows =
      input.types === undefined || input.types.length === 0
        ? await tx<EventRow[]>`
            select id, tenant_id, actor_id, aggregate_id, seq, type, data
            from event
            where aggregate_id = ${input.aggregateID}
              and seq > ${after}
            order by seq asc
            limit ${limit}
          `
        : await tx<EventRow[]>`
            select id, tenant_id, actor_id, aggregate_id, seq, type, data
            from event
            where aggregate_id = ${input.aggregateID}
              and seq > ${after}
              and type in ${tx(input.types)}
            order by seq asc
            limit ${limit}
          `
    return rows.map(fromRow)
  })
}

export async function latestSequence(sql: Sql, tenant: TenantContext, aggregateID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const rows = await tx<{ seq: string | number }[]>`
      select seq
      from event_sequence
      where aggregate_id = ${aggregateID}
    `
    return rows[0] === undefined ? -1 : Number(rows[0].seq)
  })
}

export async function replay(sql: Sql, input: ReplayInput): Promise<ReplayResult> {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, input.tenant)
    await tx`
      insert into event_sequence (tenant_id, aggregate_id, seq, owner_id)
      values (${input.tenant.tenantID}, ${input.aggregateID}, -1, ${input.ownerID ?? null})
      on conflict (tenant_id, aggregate_id) do nothing
    `
    const sequence = await tx<{ seq: string | number; owner_id: string | null }[]>`
      select seq, owner_id
      from event_sequence
      where tenant_id = ${input.tenant.tenantID}
        and aggregate_id = ${input.aggregateID}
      for update
    `
    const current = sequence[0]
    if (current === undefined) throw new Error(`Unable to lock event sequence for aggregate ${input.aggregateID}`)
    if (input.strictOwner === true && current.owner_id !== null && current.owner_id !== input.ownerID) {
      throw new Error(
        `Replay owner mismatch for aggregate ${input.aggregateID}: expected ${current.owner_id}, got ${input.ownerID ?? "none"}`,
      )
    }
    const latest = Number(current.seq)
    if (input.seq <= latest) {
      const existing = await tx<EventRow[]>`
        select id, tenant_id, actor_id, aggregate_id, seq, type, data
        from event
        where aggregate_id = ${input.aggregateID}
          and seq = ${input.seq}
        limit 1
      `
      const row = existing[0]
      if (
        row !== undefined &&
        row.id === input.id &&
        row.type === input.type &&
        isDeepStrictEqual(row.data, input.data)
      ) {
        return { event: fromRow(row), committed: false }
      }
      throw new Error(`Replay diverged at aggregate ${input.aggregateID} sequence ${input.seq}`)
    }
    if (input.seq !== latest + 1) {
      throw new Error(`Replay sequence mismatch for aggregate ${input.aggregateID}: expected ${latest + 1}, got ${input.seq}`)
    }
    await tx`
      insert into event (id, tenant_id, actor_id, aggregate_id, seq, type, data)
      values (
        ${input.id},
        ${input.tenant.tenantID},
        ${input.tenant.actorID ?? null},
        ${input.aggregateID},
        ${input.seq},
        ${input.type},
        ${tx.json(input.data as any)}
      )
    `
    await tx`
      update event_sequence
      set seq = ${input.seq},
          owner_id = ${input.ownerID ?? current.owner_id}
      where tenant_id = ${input.tenant.tenantID}
        and aggregate_id = ${input.aggregateID}
    `
    return {
      event: {
        id: input.id,
        tenantID: input.tenant.tenantID,
        ...(input.tenant.actorID === undefined ? {} : { actorID: input.tenant.actorID }),
        aggregateID: input.aggregateID,
        seq: input.seq,
        type: input.type,
        data: input.data,
      },
      committed: true,
    }
  })
}

export async function remove(sql: Sql, tenant: TenantContext, aggregateID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`
      delete from event_sequence
      where aggregate_id = ${aggregateID}
    `
  })
}

export async function claim(sql: Sql, tenant: TenantContext, aggregateID: string, ownerID: string) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    await tx`
      insert into event_sequence (tenant_id, aggregate_id, seq, owner_id)
      values (${tenant.tenantID}, ${aggregateID}, -1, ${ownerID})
      on conflict (tenant_id, aggregate_id) do update set owner_id = excluded.owner_id
    `
  })
}

function fromRow(row: EventRow): StoredEvent {
  return {
    id: row.id,
    tenantID: row.tenant_id,
    ...(row.actor_id === null ? {} : { actorID: row.actor_id }),
    aggregateID: row.aggregate_id,
    seq: Number(row.seq),
    type: row.type,
    data: row.data,
  }
}
