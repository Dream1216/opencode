import { metrics, type Meter } from "@opentelemetry/api"
import type { ShadowCanaryBreakerState } from "../session/runner/shadow-canary-breaker-store"

export type QuestionShadowCommand = "seed" | "ask" | "reply" | "reject"
export type QuestionShadowDecision = "admitted" | "sampled_out" | "breaker_open" | "config_error" | "store_error"
export type QuestionShadowOutcome = "matched" | "diverged" | "error"

type Snapshot = {
  readonly scope: string
  readonly sampleRate: number
  readonly ready: boolean
  readonly breaker: ShadowCanaryBreakerState
}

const snapshots = new Map<string, Snapshot>()
const decisions = new Map<string, number>()
const commands = new Map<string, number>()
const meter = metrics.getMeter("opencode.question-shadow", "1.0.0")
const defaultInstruments = registerQuestionShadowTelemetry(meter)

export function registerQuestionShadowTelemetry(target: Meter) {
  const decisionCounter = target.createCounter("opencode.question_shadow.decisions", {
    description: "Question shadow admission decisions",
  })
  const commandCounter = target.createCounter("opencode.question_shadow.commands", {
    description: "Question shadow command comparison outcomes",
  })
  const ready = target.createObservableGauge("opencode.question_shadow.ready", {
    description: "Whether Question shadow comparison is configured and its breaker is closed",
  })
  const breakerOpen = target.createObservableGauge("opencode.question_shadow.breaker_open", {
    description: "Whether the Question shadow automatic circuit breaker is open",
  })
  const sampleRate = target.createObservableGauge("opencode.question_shadow.sample_rate", {
    description: "Configured stable-hash Question shadow sampling rate",
  })
  const breakerSamples = target.createObservableGauge("opencode.question_shadow.breaker_samples", {
    description: "Number of comparison samples in the Question shadow breaker window",
  })
  const divergenceRate = target.createObservableGauge("opencode.question_shadow.divergence_rate", {
    description: "Question shadow divergence rate in the breaker window",
  })
  const errorRate = target.createObservableGauge("opencode.question_shadow.error_rate", {
    description: "Question shadow error rate in the breaker window",
  })

  ready.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.ready ? 1 : 0, labels(snapshot.scope))
  })
  breakerOpen.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.breaker.open ? 1 : 0, labels(snapshot.scope))
  })
  sampleRate.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.sampleRate, labels(snapshot.scope))
  })
  breakerSamples.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.breaker.sampleCount, labels(snapshot.scope))
  })
  divergenceRate.addCallback((result) => {
    for (const snapshot of snapshots.values()) {
      result.observe(snapshot.breaker.structuralMismatchRate, labels(snapshot.scope))
    }
  })
  errorRate.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.breaker.failureRate, labels(snapshot.scope))
  })
  return { decisionCounter, commandCounter }
}

export function recordQuestionShadowDecision(input: {
  readonly scope: string
  readonly command: QuestionShadowCommand
  readonly decision: QuestionShadowDecision
}) {
  increment(decisions, key(input.scope, input.command, input.decision))
  defaultInstruments.decisionCounter.add(1, {
    ...labels(input.scope),
    command: input.command,
    decision: input.decision,
  })
}

export function recordQuestionShadowOutcome(input: {
  readonly scope: string
  readonly command: Exclude<QuestionShadowCommand, "seed">
  readonly outcome: QuestionShadowOutcome
}) {
  increment(commands, key(input.scope, input.command, input.outcome))
  defaultInstruments.commandCounter.add(1, {
    ...labels(input.scope),
    command: input.command,
    outcome: input.outcome,
  })
}

export function observeQuestionShadowBreaker(input: {
  readonly scope: string
  readonly sampleRate: number
  readonly configured: boolean
  readonly breaker: ShadowCanaryBreakerState
}) {
  snapshots.set(input.scope, {
    scope: input.scope,
    sampleRate: input.sampleRate,
    ready: input.configured && !input.breaker.open,
    breaker: input.breaker,
  })
}

export function questionShadowTelemetrySnapshot(scope: string) {
  return snapshots.get(scope)
}

export function renderQuestionShadowPrometheus() {
  const lines = [
    "# HELP opencode_question_shadow_decisions_total Question shadow admission decisions.",
    "# TYPE opencode_question_shadow_decisions_total counter",
  ]
  for (const [entry, value] of [...decisions.entries()].sort()) {
    const [scope, command, decision] = entry.split("\u0000")
    lines.push(
      `opencode_question_shadow_decisions_total${prometheusLabels({ scope, command, decision })} ${value}`,
    )
  }
  lines.push(
    "# HELP opencode_question_shadow_commands_total Question shadow comparison outcomes.",
    "# TYPE opencode_question_shadow_commands_total counter",
  )
  for (const [entry, value] of [...commands.entries()].sort()) {
    const [scope, command, outcome] = entry.split("\u0000")
    lines.push(
      `opencode_question_shadow_commands_total${prometheusLabels({ scope, command, outcome })} ${value}`,
    )
  }
  lines.push(
    "# HELP opencode_question_shadow_ready Whether Question shadow comparison is ready.",
    "# TYPE opencode_question_shadow_ready gauge",
    "# HELP opencode_question_shadow_breaker_open Whether the automatic breaker is open.",
    "# TYPE opencode_question_shadow_breaker_open gauge",
    "# HELP opencode_question_shadow_sample_rate Configured stable-hash sample rate.",
    "# TYPE opencode_question_shadow_sample_rate gauge",
    "# HELP opencode_question_shadow_breaker_samples Samples in the breaker window.",
    "# TYPE opencode_question_shadow_breaker_samples gauge",
    "# HELP opencode_question_shadow_divergence_rate Divergence rate in the breaker window.",
    "# TYPE opencode_question_shadow_divergence_rate gauge",
    "# HELP opencode_question_shadow_error_rate Error rate in the breaker window.",
    "# TYPE opencode_question_shadow_error_rate gauge",
  )
  for (const snapshot of [...snapshots.values()].sort((left, right) => left.scope.localeCompare(right.scope))) {
    const label = prometheusLabels({ scope: snapshot.scope })
    lines.push(
      `opencode_question_shadow_ready${label} ${snapshot.ready ? 1 : 0}`,
      `opencode_question_shadow_breaker_open${label} ${snapshot.breaker.open ? 1 : 0}`,
      `opencode_question_shadow_sample_rate${label} ${snapshot.sampleRate}`,
      `opencode_question_shadow_breaker_samples${label} ${snapshot.breaker.sampleCount}`,
      `opencode_question_shadow_divergence_rate${label} ${snapshot.breaker.structuralMismatchRate}`,
      `opencode_question_shadow_error_rate${label} ${snapshot.breaker.failureRate}`,
    )
  }
  lines.push("")
  return lines.join("\n")
}

export function resetQuestionShadowTelemetry(scope: string) {
  snapshots.delete(scope)
  for (const values of [decisions, commands]) {
    for (const entry of values.keys()) {
      if (entry.startsWith(`${scope}\u0000`)) values.delete(entry)
    }
  }
}

function increment(values: Map<string, number>, entry: string) {
  values.set(entry, (values.get(entry) ?? 0) + 1)
}

function key(...values: string[]) {
  return values.join("\u0000")
}

function labels(scope: string) {
  return { "shadow.scope": scope }
}

function prometheusLabels(values: Record<string, string | undefined>) {
  return `{${Object.entries(values)
    .filter((entry): entry is [string, string] => entry[1] !== undefined)
    .map(([name, value]) => `${name}="${escapeLabel(value)}"`)
    .join(",")}}`
}

function escapeLabel(value: string) {
  return value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll('"', '\\"')
}
