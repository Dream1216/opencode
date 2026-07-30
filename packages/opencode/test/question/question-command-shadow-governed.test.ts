import { describe, expect } from "bun:test"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { questionShadowGovernanceConfig } from "@opencode-ai/core/question-shadow-governance"
import { Effect, Layer } from "effect"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { layer as baseLayer } from "../../src/question/command-shadow"
import { QuestionCommandShadow } from "../../src/question/command-shadow-governed"
import { testEffect } from "../lib/effect"

const config = questionShadowGovernanceConfig({
  OPENCODE_QUESTION_SHADOW_SAMPLE_RATE: "1",
  OPENCODE_QUESTION_SHADOW_METRICS_SCOPE: "p7.3.4-governed",
  OPENCODE_QUESTION_SHADOW_BREAKER_WINDOW_SIZE: "2",
  OPENCODE_QUESTION_SHADOW_BREAKER_MIN_SAMPLES: "1",
  OPENCODE_QUESTION_SHADOW_BREAKER_DIVERGENCE_RATE: "0",
})
const base = baseLayer.pipe(
  Layer.provide(RuntimeFlags.layer({ experimentalQuestionCommandGatewayShadow: true })),
)
const governed = testEffect(QuestionCommandShadow.layerWith({ config }).pipe(Layer.provide(base)))

const asked = (id: string): QuestionV1.AskedData => ({
  id: QuestionV1.ID.ascending(id),
  sessionID: "ses_p734_governed" as QuestionV1.AskedData["sessionID"],
  questions: [
    {
      header: "Target",
      question: "Which target?",
      options: [{ label: "Web", description: "Build the web target" }],
    },
  ],
  location: { directory: AbsolutePath.make("/p734-governed") },
})

describe("QuestionCommandShadow governed adapter", () => {
  governed.effect("opens the breaker on a divergence and skips later shadow commands", () =>
    QuestionCommandShadow.Service.use((shadow) =>
      shadow.observeAsk({ data: asked("que_p734_diverged"), expected: "idempotent" }).pipe(
        Effect.andThen(
          shadow.observeAsk({ data: asked("que_p734_blocked"), expected: "appended" }),
        ),
        Effect.andThen(
          Effect.sync(() => {
            expect(shadow.snapshot()).toMatchObject({
              compared: 1,
              diverged: 1,
              governance: {
                admitted: 1,
                breakerOpen: 1,
                breaker: { open: true, sampleCount: 1 },
              },
            })
          }),
        ),
      ),
    ),
  )
})
