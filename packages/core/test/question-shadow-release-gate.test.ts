import { describe, expect, test } from "bun:test"
import {
  createQuestionShadowAcceptanceArtifact,
  defaultQuestionShadowRolloutPolicy,
  evaluateQuestionShadowRollout,
  questionShadowAcceptanceChecks,
  verifyQuestionShadowAcceptanceArtifact,
  type QuestionShadowAcceptancePayload,
  type QuestionShadowTelemetryGateInput,
} from "../src/question-shadow-release-gate"

const key = "p7.3.6-question-shadow-proof-key"
const now = 1_800_000_000_000
const payload: QuestionShadowAcceptancePayload = {
  environment: "p7.3.6-test",
  buildID: "build-p7.3.6",
  generatedAt: now - 1_000,
  expiresAt: now + 60_000,
  acceptance: {
    status: "passed",
    checks: questionShadowAcceptanceChecks,
    postgres: {
      processes: 8,
      sampleCount: 8,
      failureRate: 0.375,
      divergenceRate: 0.375,
      revision: 8,
    },
    collector: { version: "otelcol 0.157.0", metricObserved: true },
    prometheus: {
      version: "prometheus 3.13.1",
      value: "1",
      labels: { shadow_scope: "p7.3.6-test" },
    },
  },
}
const expectation = {
  environment: payload.environment,
  buildID: payload.buildID,
  key,
  now,
}
const healthy: QuestionShadowTelemetryGateInput = {
  ready: true,
  breakerOpen: false,
  sampleCount: 2_000,
  errorRate: 0.01,
  divergenceRate: 0.02,
}

describe("QuestionShadowReleaseGate", () => {
  test("verifies a signed build- and environment-bound acceptance proof", () => {
    const artifact = createQuestionShadowAcceptanceArtifact(payload, key)
    expect(verifyQuestionShadowAcceptanceArtifact(artifact, expectation)).toEqual(artifact)
    expect(() =>
      verifyQuestionShadowAcceptanceArtifact(
        { ...artifact, payload: { ...artifact.payload, buildID: "tampered" } },
        expectation,
      ),
    ).toThrow("digest mismatch")
  })

  test("allows the initial one-percent rollout only with live acceptance proof", () => {
    const artifact = createQuestionShadowAcceptanceArtifact(payload, key)
    expect(
      evaluateQuestionShadowRollout({
        currentSampleRate: 0,
        requestedSampleRate: 0.01,
        stageStartedAt: now,
        artifact,
        expectation,
        now,
      }),
    ).toEqual({
      decision: "promote",
      currentSampleRate: 0,
      requestedSampleRate: 0.01,
      recommendedSampleRate: 0.01,
      reasons: [],
    })
  })

  test("holds promotion until sample and soak requirements are met", () => {
    const artifact = createQuestionShadowAcceptanceArtifact(payload, key)
    const held = evaluateQuestionShadowRollout({
      currentSampleRate: 0.01,
      requestedSampleRate: 0.05,
      stageStartedAt: now - 60_000,
      artifact,
      expectation,
      telemetry: { ...healthy, sampleCount: 25 },
      now,
    })
    expect(held.decision).toBe("hold")
    expect(held.reasons.join(" ")).toContain("minimum sample count")
    expect(held.reasons.join(" ")).toContain("minimum soak")

    expect(
      evaluateQuestionShadowRollout({
        currentSampleRate: 0.01,
        requestedSampleRate: 0.05,
        stageStartedAt: now - defaultQuestionShadowRolloutPolicy.requirements[0.05].soakMs,
        artifact,
        expectation,
        telemetry: healthy,
        now,
      }).decision,
    ).toBe("promote")
  })

  test("rolls an active stage back to zero when the breaker opens", () => {
    const artifact = createQuestionShadowAcceptanceArtifact(payload, key)
    expect(
      evaluateQuestionShadowRollout({
        currentSampleRate: 0.25,
        requestedSampleRate: 1,
        stageStartedAt: now - 24 * 60 * 60 * 1000,
        artifact,
        expectation,
        telemetry: { ...healthy, breakerOpen: true },
        now,
      }),
    ).toMatchObject({
      decision: "rollback",
      recommendedSampleRate: 0,
      reasons: ["Question shadow breaker is open"],
    })
  })

  test("blocks skipped rollout stages", () => {
    const artifact = createQuestionShadowAcceptanceArtifact(payload, key)
    const result = evaluateQuestionShadowRollout({
      currentSampleRate: 0,
      requestedSampleRate: 0.25,
      stageStartedAt: now,
      artifact,
      expectation,
      now,
    })
    expect(result.decision).toBe("hold")
    expect(result.reasons).toContain("next permitted sample rate is 0.01")
  })
})
