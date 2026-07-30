import { createHash, createHmac, timingSafeEqual } from "node:crypto"
import fs from "node:fs/promises"

export const questionShadowAcceptanceChecks = [
  "postgres-eight-process-contention",
  "postgres-breaker-open-shared",
  "postgres-breaker-persisted-across-restart",
  "governed-shadow-blocked-by-shared-breaker",
  "real-otelcol-otlp-http-received",
  "otelcol-prometheus-exporter-published",
  "real-prometheus-target-up",
  "real-prometheus-breaker-query-equals-one",
] as const

export const questionShadowRolloutStages = [0, 0.01, 0.05, 0.25, 1] as const
export type QuestionShadowRolloutStage = (typeof questionShadowRolloutStages)[number]

export type QuestionShadowLiveAcceptance = {
  readonly status: "passed"
  readonly checks: readonly string[]
  readonly postgres: {
    readonly processes: number
    readonly sampleCount: number
    readonly failureRate: number
    readonly divergenceRate: number
    readonly revision: number
  }
  readonly collector: {
    readonly version: string
    readonly metricObserved: boolean
  }
  readonly prometheus: {
    readonly version: string
    readonly value: string
    readonly labels: Readonly<Record<string, string>>
  }
}

export type QuestionShadowAcceptancePayload = {
  readonly environment: string
  readonly buildID: string
  readonly generatedAt: number
  readonly expiresAt: number
  readonly acceptance: QuestionShadowLiveAcceptance
}

export type QuestionShadowAcceptanceArtifact = {
  readonly version: 1
  readonly payload: QuestionShadowAcceptancePayload
  readonly digest: string
  readonly signature: string
}

export type QuestionShadowAcceptanceExpectation = {
  readonly environment: string
  readonly buildID: string
  readonly key: string
  readonly now?: number
}

export type QuestionShadowTelemetryGateInput = {
  readonly ready: boolean
  readonly breakerOpen: boolean
  readonly sampleCount: number
  readonly errorRate: number
  readonly divergenceRate: number
}

export type QuestionShadowRolloutPolicy = {
  readonly maxErrorRate: number
  readonly maxDivergenceRate: number
  readonly requirements: Readonly<
    Record<Exclude<QuestionShadowRolloutStage, 0>, { readonly minimumSamples: number; readonly soakMs: number }>
  >
}

export const defaultQuestionShadowRolloutPolicy: QuestionShadowRolloutPolicy = {
  maxErrorRate: 0.05,
  maxDivergenceRate: 0.1,
  requirements: {
    0.01: { minimumSamples: 0, soakMs: 0 },
    0.05: { minimumSamples: 100, soakMs: 30 * 60 * 1000 },
    0.25: { minimumSamples: 500, soakMs: 2 * 60 * 60 * 1000 },
    1: { minimumSamples: 2_000, soakMs: 6 * 60 * 60 * 1000 },
  },
}

export type QuestionShadowRolloutDecision = {
  readonly decision: "promote" | "hold" | "rollback"
  readonly currentSampleRate: QuestionShadowRolloutStage
  readonly requestedSampleRate: QuestionShadowRolloutStage
  readonly recommendedSampleRate: QuestionShadowRolloutStage
  readonly reasons: readonly string[]
}

export function createQuestionShadowAcceptanceArtifact(
  payload: QuestionShadowAcceptancePayload,
  key: string,
): QuestionShadowAcceptanceArtifact {
  if (!key.trim()) throw new Error("Question shadow acceptance signing key is required")
  validateAcceptance(payload.acceptance)
  const digest = sha256(canonical(payload))
  return {
    version: 1,
    payload,
    digest,
    signature: createHmac("sha256", key).update(digest).digest("hex"),
  }
}

export function verifyQuestionShadowAcceptanceArtifact(
  artifact: QuestionShadowAcceptanceArtifact,
  expected: QuestionShadowAcceptanceExpectation,
) {
  if (artifact.version !== 1) throw new Error(`Unsupported Question shadow acceptance version ${artifact.version}`)
  const digest = sha256(canonical(artifact.payload))
  if (!safeEqual(artifact.digest, digest)) throw new Error("Question shadow acceptance digest mismatch")
  const signature = createHmac("sha256", expected.key).update(digest).digest("hex")
  if (!safeEqual(artifact.signature, signature)) throw new Error("Question shadow acceptance signature mismatch")
  const now = expected.now ?? Date.now()
  if (artifact.payload.generatedAt > now + 60_000) {
    throw new Error("Question shadow acceptance was generated in the future")
  }
  if (artifact.payload.expiresAt <= now) throw new Error("Question shadow acceptance has expired")
  if (artifact.payload.environment !== expected.environment) {
    throw new Error("Question shadow acceptance environment mismatch")
  }
  if (artifact.payload.buildID !== expected.buildID) {
    throw new Error("Question shadow acceptance build ID mismatch")
  }
  validateAcceptance(artifact.payload.acceptance)
  return artifact
}

export async function loadQuestionShadowAcceptanceArtifact(
  path: string,
  expected: QuestionShadowAcceptanceExpectation,
) {
  const artifact = JSON.parse(await fs.readFile(path, "utf8")) as QuestionShadowAcceptanceArtifact
  return verifyQuestionShadowAcceptanceArtifact(artifact, expected)
}

export function evaluateQuestionShadowRollout(input: {
  readonly currentSampleRate: QuestionShadowRolloutStage
  readonly requestedSampleRate: QuestionShadowRolloutStage
  readonly stageStartedAt: number
  readonly artifact: QuestionShadowAcceptanceArtifact
  readonly expectation: QuestionShadowAcceptanceExpectation
  readonly telemetry?: QuestionShadowTelemetryGateInput
  readonly policy?: QuestionShadowRolloutPolicy
  readonly now?: number
}): QuestionShadowRolloutDecision {
  const now = input.now ?? Date.now()
  const policy = input.policy ?? defaultQuestionShadowRolloutPolicy
  const rollbackReasons = unhealthy(input.currentSampleRate, input.telemetry, policy)
  if (rollbackReasons.length > 0) {
    return {
      decision: "rollback",
      currentSampleRate: input.currentSampleRate,
      requestedSampleRate: input.requestedSampleRate,
      recommendedSampleRate: 0,
      reasons: rollbackReasons,
    }
  }

  const reasons: string[] = []
  try {
    verifyQuestionShadowAcceptanceArtifact(input.artifact, { ...input.expectation, now })
  } catch (error) {
    reasons.push(error instanceof Error ? error.message : String(error))
  }
  if (input.requestedSampleRate === input.currentSampleRate) reasons.push("requested stage is already active")
  const expected = nextStage(input.currentSampleRate)
  if (input.requestedSampleRate !== expected) {
    reasons.push(`next permitted sample rate is ${expected}`)
  }
  if (input.requestedSampleRate > 0 && input.currentSampleRate > 0) {
    const requested = input.requestedSampleRate as Exclude<QuestionShadowRolloutStage, 0>
    const requirement = policy.requirements[requested]
    const telemetry = input.telemetry
    if (!telemetry) {
      reasons.push("Question shadow telemetry is required for promotion")
    } else if (telemetry.sampleCount < requirement.minimumSamples) {
      reasons.push(
        `minimum sample count ${requirement.minimumSamples} not reached: ${telemetry.sampleCount}`,
      )
    }
    const elapsed = now - input.stageStartedAt
    if (elapsed < requirement.soakMs) {
      reasons.push(`minimum soak ${requirement.soakMs}ms not reached: ${Math.max(0, elapsed)}ms`)
    }
  }
  return {
    decision: reasons.length === 0 ? "promote" : "hold",
    currentSampleRate: input.currentSampleRate,
    requestedSampleRate: input.requestedSampleRate,
    recommendedSampleRate: reasons.length === 0 ? input.requestedSampleRate : input.currentSampleRate,
    reasons,
  }
}

function unhealthy(
  current: QuestionShadowRolloutStage,
  telemetry: QuestionShadowTelemetryGateInput | undefined,
  policy: QuestionShadowRolloutPolicy,
) {
  if (current === 0) return []
  if (!telemetry) return ["active Question shadow stage has no telemetry"]
  const reasons: string[] = []
  if (!telemetry.ready) reasons.push("Question shadow readiness is false")
  if (telemetry.breakerOpen) reasons.push("Question shadow breaker is open")
  if (telemetry.errorRate > policy.maxErrorRate) {
    reasons.push(`Question shadow error rate ${telemetry.errorRate} exceeds ${policy.maxErrorRate}`)
  }
  if (telemetry.divergenceRate > policy.maxDivergenceRate) {
    reasons.push(
      `Question shadow divergence rate ${telemetry.divergenceRate} exceeds ${policy.maxDivergenceRate}`,
    )
  }
  return reasons
}

function nextStage(current: QuestionShadowRolloutStage): QuestionShadowRolloutStage {
  const index = questionShadowRolloutStages.indexOf(current)
  return questionShadowRolloutStages[Math.min(index + 1, questionShadowRolloutStages.length - 1)]!
}

function validateAcceptance(acceptance: QuestionShadowLiveAcceptance) {
  if (acceptance.status !== "passed") throw new Error("Question shadow live acceptance did not pass")
  const missing = questionShadowAcceptanceChecks.filter((check) => !acceptance.checks.includes(check))
  if (missing.length > 0) throw new Error(`Question shadow live acceptance checks missing: ${missing.join(", ")}`)
  if (acceptance.postgres.processes < 2 || acceptance.postgres.sampleCount < 8) {
    throw new Error("Question shadow live acceptance lacks multi-process PostgreSQL evidence")
  }
  if (!acceptance.collector.metricObserved) {
    throw new Error("Question shadow live acceptance lacks Collector metric evidence")
  }
  if (acceptance.prometheus.value !== "1") {
    throw new Error("Question shadow live acceptance lacks Prometheus breaker evidence")
  }
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`)
      .join(",")}}`
  }
  return JSON.stringify(value)
}

function sha256(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function safeEqual(left: string, right: string) {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.length === b.length && timingSafeEqual(a, b)
}
