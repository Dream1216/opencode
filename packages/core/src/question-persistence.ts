export * as QuestionPersistence from "./question-persistence"

import { QuestionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { Question } from "@opencode-ai/schema/question"
import { eq } from "drizzle-orm"
import { Context, Effect, Layer, Schema } from "effect"
import { DatabaseBackend } from "./database/backend"
import { Database } from "./database/database"
import { makeClient, type TenantContext } from "./database/postgres/client"
import { readPendingQuestionAskedEvents } from "./database/postgres/question-event-store"
import { EventV2 } from "./event"
import { EventTable } from "./event/sql"
import { makeGlobalNode } from "./effect/app-node"
import { Location } from "./location"

export interface Interface {
  readonly restore: (location: Location.Ref) => Effect.Effect<ReadonlyArray<Question.Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/QuestionPersistence") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const backend = DatabaseBackend.fromEnv(Database.path)
    const postgres =
      backend.type === "postgres-alpha" && backend.url && backend.tenantID
        ? {
            sql: makeClient({ url: backend.url, max: 1 }),
            tenant: {
              tenantID: backend.tenantID,
              ...(backend.actorID === undefined ? {} : { actorID: backend.actorID }),
            } satisfies TenantContext,
          }
        : undefined

    if (postgres)
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise(() => postgres.sql.end({ timeout: 5 })).pipe(Effect.catchCause(() => Effect.void)),
      )

    const askedType = EventV2.versionedType(
      Question.Event.Asked.type,
      Question.Event.Asked.durable!.version,
    )
    const terminalTypes = [Question.Event.Replied, Question.Event.Rejected].map((definition) =>
      EventV2.versionedType(definition.type, definition.durable!.version),
    )

    const restore = Effect.fn("QuestionPersistence.restore")(function* (location: Location.Ref) {
      const rows = yield* db
        .select({ aggregateID: EventTable.aggregate_id })
        .from(EventTable)
        .where(eq(EventTable.type, askedType))
        .all()
        .pipe(Effect.orDie)
      const seen = new Set<string>()
      const pending = new Map<Question.ID, Question.Request>()

      for (const row of rows) {
        seen.add(row.aggregateID)
        const page = yield* EventV2.readAggregate(db, {
          aggregateID: row.aggregateID,
          limit: 10,
          manifest: QuestionDurable,
        })
        if (page.events.at(-1)?.type !== Question.Event.Asked.type) continue
        const asked = page.events.find((event) => event.type === Question.Event.Asked.type)
        if (!asked) continue
        const data = asked.data as Question.AskedData
        if (!sameLocation(data.location, location)) continue
        pending.set(data.id, toRequest(data))
      }

      if (postgres) {
        const recovered = yield* Effect.tryPromise(() =>
          readPendingQuestionAskedEvents(postgres.sql, {
            tenant: postgres.tenant,
            askedType,
            terminalTypes,
            location,
          }),
        ).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Unable to restore pending Questions from PostgreSQL EventStore", cause).pipe(
              Effect.as([] as const),
            ),
          ),
        )
        for (const event of recovered) {
          if (seen.has(event.aggregateID)) continue
          const data = Schema.decodeUnknownSync(Question.Event.Asked.data)(event.data)
          yield* events.replay({
            id: EventV2.ID.make(event.id),
            aggregateID: event.aggregateID,
            seq: event.seq,
            type: event.type,
            data: event.data,
          })
          seen.add(event.aggregateID)
          pending.set(data.id, toRequest(data))
        }
      }

      return Array.from(pending.values())
    })

    return Service.of({ restore })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [Database.node, EventV2.node],
})

function sameLocation(left: Location.Ref, right: Location.Ref) {
  return left.directory === right.directory && left.workspaceID === right.workspaceID
}

function toRequest(data: Question.AskedData): Question.Request {
  return {
    id: data.id,
    sessionID: data.sessionID,
    questions: data.questions,
    ...(data.tool === undefined ? {} : { tool: data.tool }),
  }
}
