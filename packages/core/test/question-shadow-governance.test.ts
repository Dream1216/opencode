import { afterEach, describe, expect, test } from "bun:test"
import {
  makeQuestionShadowGovernance,
  questionShadowGovernanceConfig,
} from "../src/question-shadow-governance"
import { makeMemoryShadowCanaryBreakerStore } from "../src/session/runner/shadow-canary-breaker-store"
import {
  renderQuestionShadowPrometheus,
  resetQuestionShadowTelemetry,
} from "../src/observability/question-shadow-telemetry"

const scopes = new Set<string>()

afterEach(() => {
  for (const scope of scopes) resetQuestionShadowTelemetry(scope)
  scopes.clear()
})

describe("QuestionShadowGovernance", () => {
  test("uses stable sampling and fails closed on invalid thresholds", async () => {
    const sampledScope = scope("sampling")
    const sampledConfig = questionShadowGovernanceConfig({
      OPENCODE_QUESTION_SHADOW_SAMPLE_RATE: "0",
      OPENCODE_QUESTION_SHADOW_METRICS_SCOPE: sampledScope,
    })
    const sampled = makeQuestionShadowGovernance(
      sampledConfig,
      makeMemoryShadowCanaryBreakerStore(sampledConfig.policy),
    )
    expect(await sampled.admit("que_stable", "ask")).toMatchObject({
      allowed: false,
      reason: "sampled_out",
    })
    await sampled.close()

    const invalidScope = scope("invalid")
    const invalidConfig = questionShadowGovernanceConfig({
      OPENCODE_QUESTION_SHADOW_SAMPLE_RATE: "2",
      OPENCODE_QUESTION_SHADOW_METRICS_SCOPE: invalidScope,
    })
    const invalid = makeQuestionShadowGovernance(
      invalidConfig,
      makeMemoryShadowCanaryBreakerStore(invalidConfig.policy),
    )
    expect(invalidConfig.invalidReason).toContain("OPENCODE_QUESTION_SHADOW_SAMPLE_RATE")
    expect(await invalid.admit("que_invalid", "reply")).toMatchObject({
      allowed: false,
      reason: "config_error",
    })
    await invalid.close()
  })

  test("opens the breaker from error and divergence rates and exports Prometheus metrics", async () => {
    const metricsScope = scope("breaker")
    const config = questionShadowGovernanceConfig({
      OPENCODE_QUESTION_SHADOW_SAMPLE_RATE: "1",
      OPENCODE_QUESTION_SHADOW_METRICS_SCOPE: metricsScope,
      OPENCODE_QUESTION_SHADOW_BREAKER_WINDOW_SIZE: "4",
      OPENCODE_QUESTION_SHADOW_BREAKER_MIN_SAMPLES: "2",
      OPENCODE_QUESTION_SHADOW_BREAKER_ERROR_RATE: "0.4",
      OPENCODE_QUESTION_SHADOW_BREAKER_DIVERGENCE_RATE: "0.4",
    })
    const governance = makeQuestionShadowGovernance(
      config,
      makeMemoryShadowCanaryBreakerStore(config.policy),
    )

    expect((await governance.admit("que_error", "ask")).allowed).toBe(true)
    await governance.record({ key: "que_error", command: "ask", outcome: "error" })
    expect((await governance.admit("que_diverged", "reply")).allowed).toBe(true)
    const opened = await governance.record({
      key: "que_diverged",
      command: "reply",
      outcome: "diverged",
    })
    expect(opened).toMatchObject({
      open: true,
      sampleCount: 2,
      failureRate: 0.5,
      structuralMismatchRate: 0.5,
    })
    expect(await governance.admit("que_blocked", "reject")).toMatchObject({
      allowed: false,
      reason: "breaker_open",
    })

    const output = renderQuestionShadowPrometheus()
    expect(output).toContain(`opencode_question_shadow_breaker_open{scope="${metricsScope}"} 1`)
    expect(output).toContain('command="ask",outcome="error"')
    expect(output).toContain('command="reply",outcome="diverged"')
    expect(governance.snapshot()).toMatchObject({
      admitted: 2,
      breakerOpen: 1,
      breaker: { open: true },
    })
    await governance.close()
  })
})

function scope(suffix: string) {
  const value = `p7.3.4-${suffix}`
  scopes.add(value)
  return value
}
