import { createHash, randomUUID } from "node:crypto"
import {
  makeMemoryShadowCanaryBreakerStore,
  makePostgresShadowCanaryBreakerStore,
  type ShadowCanaryBreakerPolicy,
  type ShadowCanaryBreakerState,
  type ShadowCanaryBreakerStore,
} from "./session/runner/shadow-canary-breaker-store"
import {
  observeQuestionShadowBreaker,
  recordQuestionShadowDecision,
  recordQuestionShadowOutcome,
  type QuestionShadowCommand,
  type QuestionShadowOutcome,
} from "./observability/question-shadow-telemetry"

type Environment = Readonly<Record<string, string | undefined>>

export type QuestionShadowGovernanceConfig = {
  readonly sampleRate: number
  readonly metricsScope: string
  readonly breakerScope: string
  readonly breakerBackend: "memory" | "postgres"
  readonly databaseURL?: string
  readonly databaseMax: number
  readonly policy: ShadowCanaryBreakerPolicy
  readonly invalidReason?: string
}

export type QuestionShadowAdmission = {
  readonly allowed: boolean
  readonly reason: "admitted" | "sampled_out" | "breaker_open" | "config_error" | "store_error"
  readonly breaker: ShadowCanaryBreakerState
}

export type QuestionShadowGovernanceSnapshot = {
  readonly sampleRate: number
  readonly admitted: number
  readonly sampledOut: number
  readonly breakerOpen: number
  readonly configErrors: number
  readonly storeErrors: number
  readonly breaker: ShadowCanaryBreakerState
}

const emptyBreaker = (): ShadowCanaryBreakerState => ({
  open: false,
  sampleCount: 0,
  failureRate: 0,
  structuralMismatchRate: 0,
  slowRate: 0,
  updatedAt: Date.now(),
  revision: 0,
})

export function questionShadowGovernanceConfig(
  env: Environment = process.env,
): QuestionShadowGovernanceConfig {
  const issues: string[] = []
  const sampleRate = number(env, "OPENCODE_QUESTION_SHADOW_SAMPLE_RATE", 1, 0, 1, issues)
  const windowSize = integer(env, "OPENCODE_QUESTION_SHADOW_BREAKER_WINDOW_SIZE", 100, 1, 10_000, issues)
  const minimumSamples = integer(
    env,
    "OPENCODE_QUESTION_SHADOW_BREAKER_MIN_SAMPLES",
    20,
    1,
    windowSize,
    issues,
  )
  const backendValue = env.OPENCODE_QUESTION_SHADOW_BREAKER_BACKEND?.trim().toLowerCase() || "memory"
  const breakerBackend = backendValue === "postgres" ? "postgres" : "memory"
  if (backendValue !== "memory" && backendValue !== "postgres") {
    issues.push("OPENCODE_QUESTION_SHADOW_BREAKER_BACKEND must be memory or postgres")
  }
  const databaseURL =
    env.OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL?.trim() || env.OPENCODE_DATABASE_URL?.trim()
  if (breakerBackend === "postgres" && !databaseURL) {
    issues.push("PostgreSQL Question shadow breaker requires OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL")
  }
  const tenant = env.OPENCODE_TENANT_ID?.trim() || "global"
  return {
    sampleRate,
    metricsScope: env.OPENCODE_QUESTION_SHADOW_METRICS_SCOPE?.trim() || "global",
    breakerScope:
      env.OPENCODE_QUESTION_SHADOW_BREAKER_SCOPE?.trim() || `${tenant}:question-command-gateway`,
    breakerBackend,
    databaseURL,
    databaseMax: integer(
      env,
      "OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_MAX",
      2,
      1,
      20,
      issues,
    ),
    policy: {
      windowSize,
      minimumSamples,
      failureRateThreshold: number(
        env,
        "OPENCODE_QUESTION_SHADOW_BREAKER_ERROR_RATE",
        0.05,
        0,
        1,
        issues,
      ),
      structuralMismatchRateThreshold: number(
        env,
        "OPENCODE_QUESTION_SHADOW_BREAKER_DIVERGENCE_RATE",
        0.1,
        0,
        1,
        issues,
      ),
      slowRateThreshold: 1,
      latencyRatioThreshold: Number.MAX_SAFE_INTEGER,
      cooldownMs: integer(
        env,
        "OPENCODE_QUESTION_SHADOW_BREAKER_COOLDOWN_MS",
        60_000,
        1_000,
        86_400_000,
        issues,
      ),
    },
    invalidReason: issues.length === 0 ? undefined : issues.join("; "),
  }
}

export async function makeQuestionShadowBreakerStore(
  config: QuestionShadowGovernanceConfig,
): Promise<ShadowCanaryBreakerStore> {
  if (config.breakerBackend === "memory") return makeMemoryShadowCanaryBreakerStore(config.policy)
  if (!config.databaseURL) throw new Error("Question shadow PostgreSQL breaker URL is missing")
  return makePostgresShadowCanaryBreakerStore({
    url: config.databaseURL,
    scope: config.breakerScope,
    policy: config.policy,
    max: config.databaseMax,
  })
}

export function makeQuestionShadowGovernance(
  config: QuestionShadowGovernanceConfig,
  store: ShadowCanaryBreakerStore,
) {
  let breaker = emptyBreaker()
  const counters = {
    admitted: 0,
    sampledOut: 0,
    breakerOpen: 0,
    configErrors: 0,
    storeErrors: 0,
  }
  const observe = (configured = config.invalidReason === undefined) =>
    observeQuestionShadowBreaker({
      scope: config.metricsScope,
      sampleRate: config.sampleRate,
      configured,
      breaker,
    })
  observe()

  const decision = (
    command: QuestionShadowCommand,
    reason: QuestionShadowAdmission["reason"],
    allowed: boolean,
  ): QuestionShadowAdmission => {
    if (reason === "admitted") counters.admitted++
    if (reason === "sampled_out") counters.sampledOut++
    if (reason === "breaker_open") counters.breakerOpen++
    if (reason === "config_error") counters.configErrors++
    if (reason === "store_error") counters.storeErrors++
    recordQuestionShadowDecision({
      scope: config.metricsScope,
      command,
      decision: reason,
    })
    observe(reason !== "config_error" && reason !== "store_error")
    return { allowed, reason, breaker }
  }

  return {
    async admit(key: string, command: QuestionShadowCommand): Promise<QuestionShadowAdmission> {
      if (config.invalidReason) return decision(command, "config_error", false)
      if (!sampled(key, config.sampleRate)) return decision(command, "sampled_out", false)
      try {
        breaker = await store.read()
      } catch {
        return decision(command, "store_error", false)
      }
      if (breaker.open) return decision(command, "breaker_open", false)
      return decision(command, "admitted", true)
    },
    async record(input: {
      readonly key: string
      readonly command: Exclude<QuestionShadowCommand, "seed">
      readonly outcome: QuestionShadowOutcome
    }) {
      recordQuestionShadowOutcome({
        scope: config.metricsScope,
        command: input.command,
        outcome: input.outcome,
      })
      try {
        breaker = await store.record({
          requestDigest: digest(`${input.key}:${input.command}:${Date.now()}:${randomUUID()}`),
          comparedAt: Date.now(),
          candidateFailed: input.outcome === "error",
          statusMatch: input.outcome !== "diverged",
          toolPlanDigestMatch: true,
          latencyRatio: 0,
        })
        observe()
      } catch {
        counters.storeErrors++
        recordQuestionShadowDecision({
          scope: config.metricsScope,
          command: input.command,
          decision: "store_error",
        })
        observe(false)
      }
      return breaker
    },
    snapshot(): QuestionShadowGovernanceSnapshot {
      return {
        sampleRate: config.sampleRate,
        ...counters,
        breaker: { ...breaker },
      }
    },
    close() {
      return store.close()
    },
  }
}

function sampled(key: string, rate: number) {
  if (rate <= 0) return false
  if (rate >= 1) return true
  const value = createHash("sha256").update(key).digest().readUInt32BE(0)
  return value / 0x1_0000_0000 < rate
}

function digest(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function number(
  env: Environment,
  name: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[],
) {
  const raw = env[name]
  if (raw === undefined || raw.trim() === "") return fallback
  const value = Number(raw)
  if (!Number.isFinite(value) || value < min || value > max) {
    issues.push(`${name} must be between ${min} and ${max}`)
    return fallback
  }
  return value
}

function integer(
  env: Environment,
  name: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[],
) {
  const value = number(env, name, fallback, min, max, issues)
  if (!Number.isSafeInteger(value)) {
    issues.push(`${name} must be an integer`)
    return fallback
  }
  return value
}
