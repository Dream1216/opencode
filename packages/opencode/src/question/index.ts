import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Deferred, Effect, Layer, Schema, Context } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "@/session/schema"
import { QuestionID } from "./schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import { QuestionV1Durable } from "@opencode-ai/schema/durable-event-manifest"
import { DatabaseBackend } from "@opencode-ai/core/database/backend"
import { Database } from "@opencode-ai/core/database/database"
import { makeClient, type TenantContext } from "@opencode-ai/core/database/postgres/client"
import { readPendingQuestionAskedEvents } from "@opencode-ai/core/database/postgres/question-event-store"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session } from "@/session/session"
import { eq } from "drizzle-orm"
import { QuestionCommandShadow } from "./command-shadow-governed"

export const Option = QuestionV1.Option
export type Option = typeof Option.Type
export const Info = QuestionV1.Info
export type Info = typeof Info.Type
export const Prompt = QuestionV1.Prompt
export type Prompt = typeof Prompt.Type
export const Tool = QuestionV1.Tool
export type Tool = typeof Tool.Type
export const Request = QuestionV1.Request
export type Request = typeof Request.Type
export const Answer = QuestionV1.Answer
export type Answer = typeof Answer.Type
export const Reply = QuestionV1.Reply
export type Reply = typeof Reply.Type
export const Replied = QuestionV1.Replied
export const Rejected = QuestionV1.Rejected
export const Event = QuestionV1.Event

export class RejectedError extends Schema.TaggedErrorClass<RejectedError>()("QuestionRejectedError", {}) {
  override get message() {
    return "The user dismissed this question"
  }
}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("Question.NotFoundError", {
  requestID: QuestionID,
}) {}

interface PendingEntry {
  info: Request
  deferred?: Deferred.Deferred<ReadonlyArray<Answer>, RejectedError>
}

interface State {
  pending: Map<QuestionID, PendingEntry>
}

// Service

export interface Interface {
  readonly ask: (input: {
    sessionID: SessionID
    questions: ReadonlyArray<Info>
    tool?: Tool
  }) => Effect.Effect<ReadonlyArray<Answer>, RejectedError>
  readonly reply: (input: {
    requestID: QuestionID
    answers: ReadonlyArray<Answer>
  }) => Effect.Effect<Resolution, NotFoundError>
  readonly reject: (requestID: QuestionID) => Effect.Effect<Resolution, NotFoundError>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export interface Resolution {
  readonly request: Request
  readonly recovered: boolean
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Question") {}

export const toModelOutput = (questions: ReadonlyArray<Info>, answers: ReadonlyArray<Answer>) => {
  const formatted = questions
    .map((question, index) =>
      `"${question.question}"="${answers[index]?.length ? answers[index].join(", ") : "Unanswered"}"`,
    )
    .join(", ")
  return `User has answered your questions: ${formatted}. You can now continue with the user's answers in mind.`
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const shadow = yield* QuestionCommandShadow.Service
    const { db } = yield* Database.Service
    const sessions = yield* Session.Service
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

    const askedType = EventV2.versionedType(Event.Asked.type, Event.Asked.durable!.version)
    const terminalTypes = [Event.Replied, Event.Rejected].map((definition) =>
      EventV2.versionedType(definition.type, definition.durable!.version),
    )

    const restorePending = Effect.fn("Question.restorePending")(function* (location: QuestionV1.AskedData["location"]) {
      const rows = yield* db
        .select({ aggregateID: EventTable.aggregate_id })
        .from(EventTable)
        .where(eq(EventTable.type, askedType))
        .all()
        .pipe(Effect.orDie)
      const seen = new Set<string>()
      const pending = new Map<QuestionID, Request>()

      for (const row of rows) {
        seen.add(row.aggregateID)
        const page = yield* EventV2.readAggregate(db, {
          aggregateID: row.aggregateID,
          limit: 10,
          manifest: QuestionV1Durable,
        })
        if (page.events.at(-1)?.type !== Event.Asked.type) continue
        const asked = page.events.find((event) => event.type === Event.Asked.type)
        if (!asked) continue
        const data = asked.data as QuestionV1.AskedData
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
            Effect.logWarning("Unable to restore pending runtime Questions from PostgreSQL EventStore", cause).pipe(
              Effect.as([] as const),
            ),
          ),
        )
        for (const event of recovered) {
          if (seen.has(event.aggregateID)) continue
          const data = Schema.decodeUnknownSync(Event.Asked.data)(event.data)
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

    const currentLocation = Effect.fn("Question.currentLocation")(function* () {
      const ctx = yield* InstanceState.context
      const workspaceID = yield* InstanceState.workspaceID
      return {
        directory: AbsolutePath.make(ctx.directory),
        ...(workspaceID ? { workspaceID } : {}),
      } satisfies QuestionV1.AskedData["location"]
    })

    const state = yield* InstanceState.make<State>(
      Effect.fn("Question.state")(function* () {
        const location = yield* currentLocation()
        const restored = yield* restorePending(location)
        yield* Effect.forEach(
          restored,
          (request) =>
            shadow.seed({
              ...request,
              location,
            }),
          { discard: true },
        )
        const state = {
          pending: new Map<QuestionID, PendingEntry>(
            restored.map((info) => [info.id, { info }]),
          ),
        }

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            for (const item of state.pending.values()) {
              if (item.deferred) yield* Deferred.fail(item.deferred, new RejectedError())
            }
            state.pending.clear()
          }),
        )

        return state
      }),
    )

    const findPending = Effect.fn("Question.findPending")(function* (
      pending: Map<QuestionID, PendingEntry>,
      requestID: QuestionID,
    ) {
      const existing = pending.get(requestID)
      if (existing) return existing
      const restored = (yield* restorePending(yield* currentLocation())).find((request) => request.id === requestID)
      if (!restored) return undefined
      const concurrent = pending.get(requestID)
      if (concurrent) return concurrent
      const entry: PendingEntry = { info: restored }
      pending.set(requestID, entry)
      return entry
    })

    const ask = Effect.fn("Question.ask")((input: {
      sessionID: SessionID
      questions: ReadonlyArray<Info>
      tool?: Tool
    }) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const pending = (yield* InstanceState.get(state)).pending
          const restored =
            input.tool === undefined
              ? undefined
              : Array.from(pending.values()).find(
                  (item) =>
                    item.info.sessionID === input.sessionID &&
                    item.info.tool?.messageID === input.tool?.messageID &&
                    item.info.tool?.callID === input.tool?.callID,
                )
          const id = restored?.info.id ?? QuestionID.ascending()
          yield* Effect.logInfo("asking", { id, questions: input.questions.length, restored: restored !== undefined })

          const deferred = yield* Deferred.make<ReadonlyArray<Answer>, RejectedError>()
          const info: Request =
            restored?.info ?? {
              id,
              sessionID: input.sessionID,
              questions: input.questions,
              tool: input.tool,
            }
          pending.set(id, { info, deferred })
          const location = yield* currentLocation()
          if (!restored) {
            yield* events.publish(Event.Asked, {
              ...info,
              location,
            })
          }
          yield* shadow.observeAsk({
            data: {
              ...info,
              location,
            },
            expected: restored ? "idempotent" : "appended",
          })

          return yield* restore(Deferred.await(deferred)).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                const current = pending.get(id)
                if (current?.deferred === deferred) current.deferred = undefined
              }),
            ),
          )
        }),
      ),
    )

    const findToolPart = Effect.fn("Question.findToolPart")(function* (request: Request) {
      if (!request.tool) return undefined
      const messages = yield* sessions.messages({ sessionID: request.sessionID }).pipe(Effect.orDie)
      for (const message of messages) {
        if (message.info.id !== request.tool.messageID) continue
        const part = message.parts.find(
          (candidate): candidate is SessionV1.ToolPart =>
            candidate.type === "tool" && candidate.callID === request.tool?.callID,
        )
        if (part) return part
      }
      return undefined
    })

    const completeRecoveredTool = Effect.fn("Question.completeRecoveredTool")(function* (
      request: Request,
      answers: ReadonlyArray<Answer>,
    ) {
      const part = yield* findToolPart(request)
      if (!part) {
        yield* Effect.logWarning("Unable to find recovered Question tool part", { requestID: request.id })
        return
      }
      const output = toModelOutput(request.questions, answers)
      const start = "time" in part.state && part.state.time?.start ? part.state.time.start : Date.now()
      yield* sessions.updatePart({
        ...part,
        state: {
          status: "completed",
          input: part.state.input,
          title: `Asked ${request.questions.length} question${request.questions.length > 1 ? "s" : ""}`,
          metadata: { answers: answers.map((answer) => [...answer]), recovered: true },
          output,
          time: { start, end: Date.now() },
        },
      } satisfies SessionV1.ToolPart)
    })

    const rejectRecoveredTool = Effect.fn("Question.rejectRecoveredTool")(function* (request: Request) {
      const part = yield* findToolPart(request)
      if (!part) return
      const start = "time" in part.state && part.state.time?.start ? part.state.time.start : Date.now()
      yield* sessions.updatePart({
        ...part,
        state: {
          status: "error",
          input: part.state.input,
          error: "Question dismissed after service restart",
          metadata: { recovered: true },
          time: { start, end: Date.now() },
        },
      } satisfies SessionV1.ToolPart)
    })

    const reply = Effect.fn("Question.reply")(function* (input: {
      requestID: QuestionID
      answers: ReadonlyArray<Answer>
    }) {
      const pending = (yield* InstanceState.get(state)).pending
      const existing = yield* findPending(pending, input.requestID)
      if (!existing) {
        yield* Effect.logWarning("reply for unknown request", { requestID: input.requestID })
        return yield* new NotFoundError({ requestID: input.requestID })
      }
      const recovered = existing.deferred === undefined
      pending.delete(input.requestID)
      yield* Effect.logInfo("replied", { requestID: input.requestID, answers: input.answers })
      yield* events.publish(Event.Replied, {
        sessionID: existing.info.sessionID,
        requestID: existing.info.id,
        answers: input.answers.map((a) => [...a]),
      })
      yield* shadow.observeReply({
        requestID: existing.info.id,
        sessionID: existing.info.sessionID,
        answers: input.answers,
        expected: "appended",
      })
      if (recovered) yield* completeRecoveredTool(existing.info, input.answers)
      if (existing.deferred) yield* Deferred.succeed(existing.deferred, input.answers)
      return { request: existing.info, recovered }
    })

    const reject = Effect.fn("Question.reject")(function* (requestID: QuestionID) {
      const pending = (yield* InstanceState.get(state)).pending
      const existing = yield* findPending(pending, requestID)
      if (!existing) {
        yield* Effect.logWarning("reject for unknown request", { requestID })
        return yield* new NotFoundError({ requestID })
      }
      const recovered = existing.deferred === undefined
      pending.delete(requestID)
      yield* Effect.logInfo("rejected", { requestID })
      yield* events.publish(Event.Rejected, {
        sessionID: existing.info.sessionID,
        requestID: existing.info.id,
      })
      yield* shadow.observeReject({
        requestID: existing.info.id,
        sessionID: existing.info.sessionID,
        expected: "appended",
      })
      if (recovered) yield* rejectRecoveredTool(existing.info)
      if (existing.deferred) yield* Deferred.fail(existing.deferred, new RejectedError())
      return { request: existing.info, recovered }
    })

    const list = Effect.fn("Question.list")(function* () {
      const pending = (yield* InstanceState.get(state)).pending
      return Array.from(pending.values(), (x) => x.info)
    })

    return Service.of({ ask, reply, reject, list })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [Database.node, EventV2Bridge.node, Session.node, QuestionCommandShadow.node],
})

function sameLocation(left: QuestionV1.AskedData["location"], right: QuestionV1.AskedData["location"]) {
  return left.directory === right.directory && left.workspaceID === right.workspaceID
}

function toRequest(data: QuestionV1.AskedData): Request {
  return {
    id: data.id,
    sessionID: data.sessionID,
    questions: data.questions,
    ...(data.tool === undefined ? {} : { tool: data.tool }),
  }
}

export * as Question from "."
