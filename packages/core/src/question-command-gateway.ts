export * as QuestionCommandGateway from "./question-command-gateway"

import { QuestionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { Question } from "@opencode-ai/schema/question"
import { Schema } from "effect"
import type { Sql } from "postgres"
import { EventV2 } from "./event"
import { QuestionAggregate } from "./question-aggregate"
import type { TenantContext } from "./database/postgres/client"
import {
  append as appendPostgres,
  readAggregate as readPostgresAggregate,
  SequenceConflictError as PostgresSequenceConflictError,
} from "./database/postgres/event-store"

export interface EventRecord {
  readonly seq: number
  readonly event: QuestionAggregate.StoredEvent
}

export interface Store {
  readonly read: (requestID: Question.ID) => Promise<ReadonlyArray<EventRecord>>
  readonly append: (input: {
    readonly requestID: Question.ID
    readonly expectedSeq: number
    readonly event: QuestionAggregate.StoredEvent
  }) => Promise<EventRecord>
}

export interface Snapshot {
  readonly state: QuestionAggregate.State
  readonly latestSeq: number
  readonly records: ReadonlyArray<EventRecord>
}

export interface CommandResult {
  readonly status: "appended" | "idempotent"
  readonly state: Exclude<QuestionAggregate.State, QuestionAggregate.Empty>
  readonly seq: number
}

export interface Interface {
  readonly load: (requestID: Question.ID) => Promise<Snapshot>
  readonly ask: (data: Question.AskedData) => Promise<CommandResult>
  readonly reply: (input: {
    readonly requestID: Question.ID
    readonly sessionID: Question.Request["sessionID"]
    readonly answers: ReadonlyArray<Question.Answer>
  }) => Promise<CommandResult>
  readonly reject: (input: {
    readonly requestID: Question.ID
    readonly sessionID: Question.Request["sessionID"]
  }) => Promise<CommandResult>
}

export class SequenceConflictError extends Error {
  readonly code = "question_sequence_conflict"

  constructor(
    readonly requestID: Question.ID,
    readonly expectedSeq: number,
    readonly actualSeq: number,
  ) {
    super(`Question sequence conflict for ${requestID}: expected ${expectedSeq}, got ${actualSeq}`)
    this.name = "QuestionCommandGateway.SequenceConflictError"
  }
}

export class HistoryError extends Error {
  readonly code = "question_history_invalid"

  constructor(
    readonly requestID: Question.ID,
    message: string,
  ) {
    super(message)
    this.name = "QuestionCommandGateway.HistoryError"
  }
}

export function make(store: Store, options?: { readonly maxAttempts?: number }): Interface {
  const maxAttempts = Math.max(1, options?.maxAttempts ?? 3)

  async function load(requestID: Question.ID): Promise<Snapshot> {
    const records = await store.read(requestID)
    let expected = 0
    for (const record of records) {
      if (record.seq !== expected) {
        throw new HistoryError(
          requestID,
          `Question ${requestID} history is not contiguous: expected sequence ${expected}, got ${record.seq}`,
        )
      }
      expected += 1
    }
    return {
      state: QuestionAggregate.fold(records.map((record) => record.event)),
      latestSeq: records.at(-1)?.seq ?? -1,
      records,
    }
  }

  async function execute(
    requestID: Question.ID,
    decide: (state: QuestionAggregate.State) => QuestionAggregate.Decision,
  ): Promise<CommandResult> {
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const snapshot = await load(requestID)
      const decision = decide(snapshot.state)
      if (decision.kind === "idempotent") {
        return {
          status: "idempotent",
          state: decision.state,
          seq: snapshot.latestSeq,
        }
      }
      try {
        const record = await store.append({
          requestID,
          expectedSeq: snapshot.latestSeq,
          event: decision.event,
        })
        const state = QuestionAggregate.evolve(snapshot.state, record.event)
        if (state.status === "empty") throw new HistoryError(requestID, `Question ${requestID} did not materialize`)
        return {
          status: "appended",
          state,
          seq: record.seq,
        }
      } catch (error) {
        if (!(error instanceof SequenceConflictError) || attempt + 1 === maxAttempts) throw error
      }
    }
    throw new SequenceConflictError(requestID, -1, -1)
  }

  return {
    load,
    ask: (data) => execute(data.id, (state) => QuestionAggregate.decideAsk(state, data)),
    reply: (input) => execute(input.requestID, (state) => QuestionAggregate.decideReply(state, input)),
    reject: (input) => execute(input.requestID, (state) => QuestionAggregate.decideReject(state, input)),
  }
}

export function makePostgres(config: {
  readonly sql: Sql
  readonly tenant: TenantContext
  readonly ownerID?: string
  readonly maxAttempts?: number
}): Interface {
  const types = Array.from(QuestionDurable.definitions.keys())
  return make(
    {
      async read(requestID) {
        const rows = await readPostgresAggregate(config.sql, {
          tenant: config.tenant,
          aggregateID: requestID,
          limit: 100,
          types,
        })
        return rows.map((row) => {
          const definition = QuestionDurable.definitions.get(row.type)
          if (definition?.durable === undefined) {
            throw new HistoryError(requestID, `Unknown Question event type ${row.type}`)
          }
          return {
            seq: row.seq,
            event: Schema.decodeUnknownSync(QuestionAggregate.StoredEvent)({
              type: definition.type,
              version: definition.durable.version,
              data: row.data,
            }),
          }
        })
      },
      async append(input) {
        const type = EventV2.versionedType(input.event.type, input.event.version)
        const definition = QuestionDurable.definitions.get(type)
        if (definition?.durable === undefined) {
          throw new HistoryError(input.requestID, `Unknown Question event type ${type}`)
        }
        const data = Schema.encodeUnknownSync(definition.data)(input.event.data) as Record<string, unknown>
        try {
          const stored = await appendPostgres(config.sql, {
            tenant: config.tenant,
            id: EventV2.ID.create(),
            aggregateID: input.requestID,
            type,
            data,
            ownerID: config.ownerID,
            expectedSeq: input.expectedSeq,
          })
          return {
            seq: stored.seq,
            event: input.event,
          }
        } catch (error) {
          if (error instanceof PostgresSequenceConflictError) {
            throw new SequenceConflictError(input.requestID, error.expectedSeq, error.actualSeq)
          }
          throw error
        }
      },
    },
    { maxAttempts: config.maxAttempts },
  )
}
