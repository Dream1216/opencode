export * as QuestionV2 from "./question"

import { makeLocationNode } from "./effect/app-node"
import { Context, DateTime, Deferred, Effect, Layer, Schema } from "effect"
import { Question } from "@opencode-ai/schema/question"
import { EventV2 } from "./event"
import { Location } from "./location"
import { QuestionPersistence } from "./question-persistence"
import { SessionEvent } from "./session/event"
import { SessionMessage } from "./session/message"
import { SessionSchema } from "./session/schema"

export const ID = Question.ID
export type ID = typeof ID.Type

export const Option = Question.Option
export type Option = typeof Option.Type

export const Info = Question.Info
export type Info = typeof Info.Type

export const Prompt = Question.Prompt
export type Prompt = typeof Prompt.Type

export const Tool = Question.Tool
export type Tool = typeof Tool.Type

export const Request = Question.Request
export type Request = typeof Request.Type

export const Answer = Question.Answer
export type Answer = typeof Answer.Type

export const Reply = Question.Reply
export type Reply = typeof Reply.Type

export const Event = Question.Event

export class RejectedError extends Schema.TaggedErrorClass<RejectedError>()("QuestionV2.RejectedError", {}) {
  override get message() {
    return "The user dismissed this question"
  }
}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("QuestionV2.NotFoundError", {
  requestID: ID,
}) {}

export interface AskInput {
  readonly sessionID: SessionSchema.ID
  readonly questions: ReadonlyArray<Info>
  readonly tool?: Tool
}

export interface ReplyInput {
  readonly requestID: ID
  readonly answers: ReadonlyArray<Answer>
}

export interface Resolution {
  readonly request: Request
  readonly recovered: boolean
}

export const toModelOutput = (
  questions: ReadonlyArray<Info>,
  answers: ReadonlyArray<Answer>,
) => {
  const formatted = questions
    .map(
      (question, index) =>
        `"${question.question}"="${answers[index]?.length ? answers[index].join(", ") : "Unanswered"}"`,
    )
    .join(", ")
  return `User has answered your questions: ${formatted}. You can now continue with the user's answers in mind.`
}

export interface Interface {
  readonly ask: (input: AskInput) => Effect.Effect<ReadonlyArray<Answer>, RejectedError>
  readonly reply: (input: ReplyInput) => Effect.Effect<Resolution, NotFoundError>
  readonly reject: (requestID: ID) => Effect.Effect<Resolution, NotFoundError>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/Question") {}

interface Pending {
  readonly request: Request
  deferred?: Deferred.Deferred<ReadonlyArray<Answer>, RejectedError>
}

/**
 * Location-owned pending prompts. The Location layer map must materialize this
 * layer once per embedded Location so replies cannot settle another Location's
 * deferred request.
 */
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const location = yield* Location.Service
    const persistence = yield* QuestionPersistence.Service
    const pending = new Map<ID, Pending>(
      (yield* persistence.restore(location)).map((request) => [request.id, { request }]),
    )

    yield* Effect.addFinalizer(() =>
      Effect.forEach(
        pending.values(),
        (item) =>
          item.deferred ? Deferred.fail(item.deferred, new RejectedError()).pipe(Effect.asVoid) : Effect.void,
        {
          discard: true,
        },
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            pending.clear()
          }),
        ),
      ),
    )

    const ask = Effect.fn("QuestionV2.ask")((input: AskInput) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const tool = input.tool
          const restored =
            tool === undefined
              ? undefined
              : Array.from(pending.values()).find(
                  (item) =>
                    item.request.sessionID === input.sessionID &&
                    item.request.tool?.messageID === tool.messageID &&
                    item.request.tool?.callID === tool.callID,
                )
          const id = restored?.request.id ?? ID.ascending()
          const deferred = yield* Deferred.make<ReadonlyArray<Answer>, RejectedError>()
          const request: Request = restored?.request ?? { id, ...input }
          pending.set(id, { request, deferred })
          if (!restored)
            yield* events.publish(Event.Asked, {
              ...request,
              location: { directory: location.directory, workspaceID: location.workspaceID },
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

    const reply = Effect.fn("QuestionV2.reply")((input: ReplyInput) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const existing = pending.get(input.requestID)
          if (!existing) return yield* new NotFoundError({ requestID: input.requestID })
          const recovered = existing.deferred === undefined
          yield* events.publish(Event.Replied, {
            sessionID: existing.request.sessionID,
            requestID: existing.request.id,
            answers: input.answers.map((answer) => [...answer]),
          })
          if (recovered && existing.request.tool) {
            const text = toModelOutput(existing.request.questions, input.answers)
            yield* events.publish(SessionEvent.Tool.Success, {
              sessionID: existing.request.sessionID,
              timestamp: yield* DateTime.now,
              assistantMessageID: SessionMessage.ID.make(existing.request.tool.messageID),
              callID: existing.request.tool.callID,
              structured: { answers: input.answers.map((answer) => [...answer]) },
              content: [{ type: "text", text }],
              result: { type: "text", value: text },
              provider: { executed: false },
            })
            yield* events.publish(Event.RecoveryRequested, {
              sessionID: existing.request.sessionID,
              requestID: existing.request.id,
            })
          }
          if (existing.deferred) yield* Deferred.succeed(existing.deferred, input.answers)
          pending.delete(input.requestID)
          return { request: existing.request, recovered }
        }),
      ),
    )

    const reject = Effect.fn("QuestionV2.reject")((requestID: ID) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const existing = pending.get(requestID)
          if (!existing) return yield* new NotFoundError({ requestID })
          const recovered = existing.deferred === undefined
          yield* events.publish(Event.Rejected, {
            sessionID: existing.request.sessionID,
            requestID: existing.request.id,
          })
          if (recovered && existing.request.tool)
            yield* events.publish(SessionEvent.Tool.Failed, {
              sessionID: existing.request.sessionID,
              timestamp: yield* DateTime.now,
              assistantMessageID: SessionMessage.ID.make(existing.request.tool.messageID),
              callID: existing.request.tool.callID,
              error: { type: "unknown", message: "Question dismissed after service restart" },
              provider: { executed: false },
            })
          if (existing.deferred) yield* Deferred.fail(existing.deferred, new RejectedError())
          pending.delete(requestID)
          return { request: existing.request, recovered }
        }),
      ),
    )

    const list = Effect.fn("QuestionV2.list")(function* () {
      return Array.from(pending.values(), (item) => item.request)
    })

    return Service.of({ ask, reply, reject, list })
  }),
)

export const locationLayer = layer

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [EventV2.node, Location.node, QuestionPersistence.node],
})
