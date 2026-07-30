import { describe, expect } from "bun:test"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Effect, Layer } from "effect"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { QuestionCommandShadow } from "../../src/question/command-shadow"
import { testEffect } from "../lib/effect"

const enabled = testEffect(
  QuestionCommandShadow.layer.pipe(
    Layer.provide(RuntimeFlags.layer({ experimentalQuestionCommandGatewayShadow: true })),
  ),
)
const disabled = testEffect(
  QuestionCommandShadow.layer.pipe(
    Layer.provide(RuntimeFlags.layer({ experimentalQuestionCommandGatewayShadow: false })),
  ),
)

const requestID = QuestionV1.ID.ascending("que_p733_shadow")
const sessionID = "ses_p733_shadow" as QuestionV1.AskedData["sessionID"]
const asked: QuestionV1.AskedData = {
  id: requestID,
  sessionID,
  questions: [
    {
      header: "Target",
      question: "Which target?",
      options: [{ label: "Web", description: "Build the web target" }],
    },
  ],
  location: {
    directory: AbsolutePath.make("/p733-shadow"),
  },
}

describe("QuestionCommandShadow", () => {
  disabled.effect("is a zero-work adapter when the flag is disabled", () =>
    QuestionCommandShadow.Service.use((shadow) =>
      shadow.observeAsk({ data: asked, expected: "appended" }).pipe(
        Effect.andThen(
          Effect.sync(() => {
            expect(shadow.snapshot()).toEqual({
              enabled: false,
              seeded: 0,
              compared: 0,
              matched: 0,
              diverged: 0,
              errors: 0,
            })
          }),
        ),
      ),
    ),
  )

  enabled.effect("matches an ask and reply without writing production events", () =>
    QuestionCommandShadow.Service.use((shadow) =>
      shadow.observeAsk({ data: asked, expected: "appended" }).pipe(
        Effect.andThen(
          shadow.observeReply({
            requestID,
            sessionID,
            answers: [["Web"]],
            expected: "appended",
          }),
        ),
        Effect.andThen(
          Effect.sync(() => {
            expect(shadow.snapshot()).toMatchObject({
              enabled: true,
              compared: 2,
              matched: 2,
              diverged: 0,
              errors: 0,
            })
          }),
        ),
      ),
    ),
  )

  enabled.effect("records divergent expectations and command errors without failing the primary path", () =>
    QuestionCommandShadow.Service.use((shadow) =>
      shadow.observeAsk({ data: asked, expected: "appended" }).pipe(
        Effect.andThen(shadow.observeAsk({ data: asked, expected: "appended" })),
        Effect.andThen(
          shadow.observeReply({
            requestID: QuestionV1.ID.ascending("que_missing_shadow"),
            sessionID,
            answers: [["Web"]],
            expected: "appended",
          }),
        ),
        Effect.andThen(
          Effect.sync(() => {
            expect(shadow.snapshot()).toMatchObject({
              enabled: true,
              compared: 3,
              matched: 1,
              diverged: 1,
              errors: 1,
            })
          }),
        ),
      ),
    ),
  )
})
