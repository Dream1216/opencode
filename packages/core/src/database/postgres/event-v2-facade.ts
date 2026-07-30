import { Schema } from "effect"
import type { Sql } from "postgres"
import type { Data, Definition, Payload } from "@opencode-ai/schema/event"
import { Durable } from "@opencode-ai/schema/durable-event-manifest"
import { EventV2 } from "../../event"
import { append, claim, readAggregate as readPostgresAggregate, remove, replay } from "./event-store"
import type { TenantContext } from "./client"

export type FacadeConfig = {
  readonly sql: Sql
  readonly tenant: TenantContext
  readonly ownerID?: string
}

export type PublishOptions = {
  readonly id?: EventV2.ID
  readonly ownerID?: string
  readonly expectedSeq?: number
}

export type ReplayOptions = {
  readonly ownerID?: string
  readonly strictOwner?: boolean
}

export type ReadAggregateInput<A> = {
  readonly aggregateID: string
  readonly after?: number
  readonly limit: number
  readonly manifest: {
    readonly definitions: ReadonlyMap<string, Definition>
    readonly schema: Schema.Decoder<A, never>
  }
}

export type Facade = {
  readonly publish: <D extends Definition>(
    definition: D,
    data: Data<D>,
    options?: PublishOptions,
  ) => Promise<Payload<D>>
  readonly readAggregate: <A>(
    input: ReadAggregateInput<A>,
  ) => Promise<{ readonly events: readonly A[]; readonly hasMore: boolean }>
  readonly replay: (event: EventV2.SerializedEvent, options?: ReplayOptions) => Promise<void>
  readonly remove: (aggregateID: string) => Promise<void>
  readonly claim: (aggregateID: string, ownerID: string) => Promise<void>
  readonly subscribe: () => never
  readonly all: () => never
  readonly durable: () => never
  readonly listen: () => never
  readonly project: () => never
}

export const unsupportedLiveEventCapabilityMessage =
  "PostgreSQL EventV2 facade alpha only supports publish/readAggregate/replay/remove/claim; projector, stream subscription, and session projection are intentionally unsupported"

export function make(config: FacadeConfig): Facade {
  const unsupportedLiveEventCapability = () => {
    throw new Error(unsupportedLiveEventCapabilityMessage)
  }
  return {
    async publish<D extends Definition>(definition: D, data: Data<D>, options?: PublishOptions) {
      const durable = definition.durable
      if (durable === undefined) throw new Error(`PostgreSQL EventV2 facade only supports durable events: ${definition.type}`)
      const aggregateID = (data as Record<string, unknown>)[durable.aggregate]
      if (typeof aggregateID !== "string") {
        throw new Error(`Expected string aggregate field ${durable.aggregate} for durable event ${definition.type}`)
      }
      const encoded = Schema.encodeUnknownSync(definition.data)(data) as Record<string, unknown>
      const stored = await append(config.sql, {
        tenant: config.tenant,
        id: options?.id ?? EventV2.ID.create(),
        aggregateID,
        type: EventV2.versionedType(definition.type, durable.version),
        data: encoded,
        ownerID: options?.ownerID ?? config.ownerID,
        expectedSeq: options?.expectedSeq,
      })
      return {
        id: stored.id as EventV2.ID,
        type: definition.type,
        durable: { aggregateID, seq: stored.seq, version: durable.version },
        data,
      } as Payload<D>
    },
    async readAggregate<A>(input: ReadAggregateInput<A>) {
      const rows = await readPostgresAggregate(config.sql, {
        tenant: config.tenant,
        aggregateID: input.aggregateID,
        after: input.after,
        limit: input.limit + 1,
        types: Array.from(input.manifest.definitions.keys()),
      })
      const page = rows.slice(0, input.limit)
      const decode = Schema.decodeUnknownSync(input.manifest.schema)
      return {
        events: page.map((event) =>
          decode({
            id: event.id,
            type: input.manifest.definitions.get(event.type)?.type ?? event.type,
            durable: {
              aggregateID: event.aggregateID,
              seq: event.seq,
              version: input.manifest.definitions.get(event.type)?.durable?.version,
            },
            data: event.data,
          }),
        ),
        hasMore: rows.length > input.limit,
      }
    },
    async replay(event: EventV2.SerializedEvent, options?: ReplayOptions) {
      const definition = Durable.get(event.type)
      if (definition?.durable === undefined) throw new Error(`Unknown durable event type ${event.type}`)
      await replay(config.sql, {
        tenant: config.tenant,
        id: event.id,
        aggregateID: event.aggregateID,
        seq: event.seq,
        type: event.type,
        data: event.data,
        ownerID: options?.ownerID,
        strictOwner: options?.strictOwner,
      })
    },
    async remove(aggregateID: string) {
      await remove(config.sql, config.tenant, aggregateID)
    },
    async claim(aggregateID: string, ownerID: string) {
      await claim(config.sql, config.tenant, aggregateID, ownerID)
    },
    subscribe: unsupportedLiveEventCapability,
    all: unsupportedLiveEventCapability,
    durable: unsupportedLiveEventCapability,
    listen: unsupportedLiveEventCapability,
    project: unsupportedLiveEventCapability,
  }
}
