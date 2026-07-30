import { describe, expect } from "bun:test"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { QuestionV2 } from "@opencode-ai/core/question"
import { QuestionPersistence } from "@opencode-ai/core/question-persistence"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { testEffect } from "./lib/effect"
import { location } from "./fixture/location"

const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory: AbsolutePath.make("/question-test") })),
)
const questions = AppNodeBuilder.build(
  LayerNode.group([EventV2.node, QuestionPersistence.node, QuestionV2.node]),
  [
  [Location.node, locationLayer],
  ],
)
const it = testEffect(questions)

const sessionID = SessionV2.ID.make("ses_question_test")
const question: QuestionV2.Info = {
  question: "Which option?",
  header: "Option",
  options: [{ label: "One", description: "First option" }],
}

const waitForAsk = Effect.fn("QuestionV2Test.waitForAsk")(function* (
  service: QuestionV2.Interface,
  input: QuestionV2.AskInput,
) {
  const events = yield* EventV2.Service
  const asked = yield* Deferred.make<QuestionV2.Request>()
  const unsubscribe = yield* events.listen((event) =>
    event.type === QuestionV2.Event.Asked.type
      ? Effect.sync(() => {
          const { location: _, ...request } = event.data as QuestionV2.Request & { location: Location.Ref }
          return request
        }).pipe(Effect.flatMap((request) => Deferred.succeed(asked, request)), Effect.asVoid)
      : Effect.void,
  )
  yield* Effect.addFinalizer(() => unsubscribe)
  const fiber = yield* service.ask(input).pipe(Effect.forkScoped)
  return { fiber, request: yield* Deferred.await(asked) }
})

describe("QuestionV2", () => {
  it.effect("publishes lifecycle events and settles a pending reply", () =>
    Effect.gen(function* () {
      const service = yield* QuestionV2.Service
      const events = yield* EventV2.Service
      const published: EventV2.Payload[] = []
      const unsubscribe = yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type.startsWith("question.v2.")) published.push(event)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const { fiber, request } = yield* waitForAsk(service, { sessionID, questions: [question] })

      expect(request.id).toMatch(/^que_/)
      expect(yield* service.list()).toEqual([request])
      yield* service.reply({ requestID: request.id, answers: [["One"]] })

      expect(yield* Fiber.join(fiber)).toEqual([["One"]])
      expect(yield* service.list()).toEqual([])
      expect(published.map((event) => event.type)).toEqual([
        QuestionV2.Event.Asked.type,
        QuestionV2.Event.Replied.type,
      ])
      expect(published[0]?.data).toMatchObject(request)
      expect(published[1]?.data).toEqual({ sessionID, requestID: request.id, answers: [["One"]] })
      expect(published.map((event) => event.durable?.seq)).toEqual([0, 1])
    }),
  )

  it.effect("publishes rejection, fails the ask, and rejects unknown IDs", () =>
    Effect.gen(function* () {
      const service = yield* QuestionV2.Service
      const events = yield* EventV2.Service
      const published: EventV2.Payload[] = []
      const unsubscribe = yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === QuestionV2.Event.Rejected.type) published.push(event)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const { fiber, request } = yield* waitForAsk(service, { sessionID, questions: [question] })

      yield* service.reject(request.id)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(exit.cause.toString()).toContain("QuestionV2.RejectedError")
      expect(published.map((event) => event.data)).toEqual([{ sessionID, requestID: request.id }])

      const unknown = QuestionV2.ID.ascending("que_unknown")
      expect(yield* service.reply({ requestID: unknown, answers: [] }).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: unknown }),
      )
      expect(yield* service.reject(unknown).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: unknown }),
      )
    }),
  )

  it.effect("isolates pending requests by location-layer instance and rejects them on finalization", () =>
    Effect.gen(function* () {
      const firstScope = yield* Scope.make()
      const secondScope = yield* Scope.make()
      const first = Context.get(yield* Layer.buildWithScope(Layer.fresh(questions), firstScope), QuestionV2.Service)
      const second = Context.get(yield* Layer.buildWithScope(Layer.fresh(questions), secondScope), QuestionV2.Service)
      const fiber = yield* first.ask({ sessionID, questions: [question] }).pipe(Effect.forkScoped)
      yield* Effect.yieldNow
      const request = (yield* first.list())[0]!

      expect(yield* second.list()).toEqual([])
      expect(yield* second.reply({ requestID: request.id, answers: [["One"]] }).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: request.id }),
      )

      yield* Scope.close(firstScope, Exit.void)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(exit.cause.toString()).toContain("QuestionV2.RejectedError")
      yield* Scope.close(secondScope, Exit.void)
    }),
  )

  it.effect("restores and settles a pending request after rebuilding the Question layer", () =>
    Effect.gen(function* () {
      const eventService = yield* EventV2.Service
      const persistenceService = yield* QuestionPersistence.Service
      const dependencies = Layer.mergeAll(
        Layer.succeed(EventV2.Service, eventService),
        Layer.succeed(QuestionPersistence.Service, persistenceService),
        locationLayer,
      )
      const firstScope = yield* Scope.make()
      const first = Context.get(
        yield* Layer.buildWithScope(QuestionV2.locationLayer.pipe(Layer.provide(dependencies)), firstScope),
        QuestionV2.Service,
      )
      const fiber = yield* first
        .ask({
          sessionID,
          questions: [question],
          tool: { messageID: "msg_recovery", callID: "call_recovery" },
        })
        .pipe(Effect.forkIn(firstScope, { startImmediately: true }))
      yield* Effect.yieldNow
      const request = (yield* first.list())[0]!
      yield* Scope.close(firstScope, Exit.void)
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true)

      const secondScope = yield* Scope.make()
      const second = Context.get(
        yield* Layer.buildWithScope(
          Layer.fresh(QuestionV2.locationLayer).pipe(Layer.provide(dependencies)),
          secondScope,
        ),
        QuestionV2.Service,
      )
      expect(yield* second.list()).toEqual([request])
      expect(yield* second.reply({ requestID: request.id, answers: [["One"]] })).toEqual({
        request,
        recovered: true,
      })
      expect(yield* second.list()).toEqual([])

      yield* Scope.close(secondScope, Exit.void)
    }),
  )
})
