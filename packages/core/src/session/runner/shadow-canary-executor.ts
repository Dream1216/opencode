import { createHash, randomUUID, timingSafeEqual } from "node:crypto"
import { spawn } from "node:child_process"
import { appendFile, mkdir, mkdtemp, rm } from "node:fs/promises"
import { createServer, type IncomingMessage, type ServerResponse } from "node:http"
import { tmpdir } from "node:os"
import { dirname, join, resolve } from "node:path"
import type { ShadowCanaryOutcome } from "./shadow-canary"
import {
  shadowCanaryBreakerStoreFromEnv,
  type ShadowCanaryBreakerState,
  type ShadowCanaryBreakerStore,
} from "./shadow-canary-breaker-store"

export type ShadowCanaryExecutorConfig = {
  readonly host?: string
  readonly port?: number
  readonly bearerToken?: string
  readonly targetVersion?: string
  readonly candidateCommand?: readonly string[]
  readonly candidateCheckout?: string
  readonly candidateConfigContent?: string
  readonly candidateAuthContent?: string
  readonly candidateEnvAllowlist?: readonly string[]
  readonly candidateTimeoutMs?: number
  readonly maxBodyBytes?: number
  readonly concurrency?: number
  readonly storePath?: string
  readonly windowSize?: number
  readonly minimumSamples?: number
  readonly failureRateThreshold?: number
  readonly structuralMismatchRateThreshold?: number
  readonly slowRateThreshold?: number
  readonly latencyRatioThreshold?: number
  readonly cooldownMs?: number
}

export type ShadowCanaryDiff = {
  readonly version: "opencode-shadow-canary-diff.v1"
  readonly requestDigest: string
  readonly invocationID: string
  readonly statusMatch: boolean
  readonly outputDigestMatch: boolean
  readonly toolPlanDigestMatch: boolean
  readonly candidateFailed: boolean
  readonly latencyRatio: number
  readonly usageDeltaRatio: number
  readonly costDelta: number
  readonly regression: boolean
  readonly comparedAt: number
}

export type ShadowCanaryExecutorSnapshot = {
  readonly ready: boolean
  readonly breaker: {
    readonly open: boolean
    readonly reason?: string
    readonly openedAt?: number
  }
  readonly primaryOutcomes: number
  readonly candidateOutcomes: number
  readonly diffs: readonly ShadowCanaryDiff[]
  readonly active: number
  readonly queued: number
}

type CandidateInput = {
  readonly envelope: Record<string, unknown>
  readonly body: string
  readonly requestDigest: string
  readonly invocationID: string
}

export type ShadowCanaryExecutorOptions = {
  readonly executeCandidate?: (input: CandidateInput) => Promise<ShadowCanaryOutcome>
  readonly breakerStore?: ShadowCanaryBreakerStore
}

type ValidatedConfig = Required<
  Pick<
    ShadowCanaryExecutorConfig,
    | "host"
    | "port"
    | "targetVersion"
    | "candidateTimeoutMs"
    | "maxBodyBytes"
    | "concurrency"
    | "windowSize"
    | "minimumSamples"
    | "failureRateThreshold"
    | "structuralMismatchRateThreshold"
    | "slowRateThreshold"
    | "latencyRatioThreshold"
    | "cooldownMs"
  >
> &
  Pick<
    ShadowCanaryExecutorConfig,
    | "bearerToken"
    | "candidateCommand"
    | "candidateCheckout"
    | "candidateConfigContent"
    | "candidateAuthContent"
    | "candidateEnvAllowlist"
    | "storePath"
  >

export function shadowCanaryExecutorConfigFromEnv(env: NodeJS.ProcessEnv = process.env): ShadowCanaryExecutorConfig {
  return {
    host: env.OPENCODE_SHADOW_CANARY_EXECUTOR_HOST ?? "127.0.0.1",
    port: integer(env.OPENCODE_SHADOW_CANARY_EXECUTOR_PORT, 9470, 1, 65_535),
    bearerToken: optional(env.OPENCODE_SHADOW_CANARY_TOKEN),
    targetVersion: env.OPENCODE_SHADOW_CANARY_TARGET_VERSION ?? "v1.18.8",
    candidateCommand: command(env.OPENCODE_SHADOW_CANARY_EXECUTOR_COMMAND_JSON),
    candidateCheckout: optional(env.OPENCODE_SHADOW_CANDIDATE_CHECKOUT),
    candidateConfigContent: optional(env.OPENCODE_SHADOW_CANDIDATE_CONFIG_CONTENT),
    candidateAuthContent: optional(env.OPENCODE_SHADOW_CANDIDATE_AUTH_CONTENT),
    candidateEnvAllowlist: csv(env.OPENCODE_SHADOW_CANARY_EXECUTOR_ENV_ALLOWLIST),
    candidateTimeoutMs: integer(env.OPENCODE_SHADOW_CANARY_EXECUTOR_TIMEOUT_MS, 120_000, 1_000, 900_000),
    maxBodyBytes: integer(env.OPENCODE_SHADOW_CANARY_EXECUTOR_MAX_BODY_BYTES, 1_048_576, 1_024, 4_194_304),
    concurrency: integer(env.OPENCODE_SHADOW_CANARY_EXECUTOR_CONCURRENCY, 2, 1, 32),
    storePath: optional(env.OPENCODE_SHADOW_CANARY_EXECUTOR_STORE),
    windowSize: integer(env.OPENCODE_SHADOW_CANARY_BREAKER_WINDOW, 100, 2, 10_000),
    minimumSamples: integer(env.OPENCODE_SHADOW_CANARY_BREAKER_MIN_SAMPLES, 20, 2, 10_000),
    failureRateThreshold: ratio(env.OPENCODE_SHADOW_CANARY_BREAKER_FAILURE_RATE, 0.1),
    structuralMismatchRateThreshold: ratio(env.OPENCODE_SHADOW_CANARY_BREAKER_MISMATCH_RATE, 0.2),
    slowRateThreshold: ratio(env.OPENCODE_SHADOW_CANARY_BREAKER_SLOW_RATE, 0.25),
    latencyRatioThreshold: number(env.OPENCODE_SHADOW_CANARY_BREAKER_LATENCY_RATIO, 2, 1, 100),
    cooldownMs: integer(env.OPENCODE_SHADOW_CANARY_BREAKER_COOLDOWN_MS, 300_000, 1_000, 86_400_000),
  }
}

export async function startShadowCanaryExecutor(
  input: ShadowCanaryExecutorConfig,
  options: ShadowCanaryExecutorOptions = {},
) {
  const config = validate(input)
  const primary = new Map<string, ShadowCanaryOutcome>()
  const candidate = new Map<string, ShadowCanaryOutcome>()
  const diffs = new Map<string, ShadowCanaryDiff>()
  const jobs: Array<() => Promise<void>> = []
  const running = new Set<Promise<void>>()
  let active = 0
  const breakerStore =
    options.breakerStore ??
    (await shadowCanaryBreakerStoreFromEnv(
      process.env,
      {
        windowSize: config.windowSize,
        minimumSamples: config.minimumSamples,
        failureRateThreshold: config.failureRateThreshold,
        structuralMismatchRateThreshold: config.structuralMismatchRateThreshold,
        slowRateThreshold: config.slowRateThreshold,
        latencyRatioThreshold: config.latencyRatioThreshold,
        cooldownMs: config.cooldownMs,
      },
      config.targetVersion,
    ))
  let breaker: ShadowCanaryBreakerState = await breakerStore.read()
  let persist = Promise.resolve()
  const counters = {
    requestsAccepted: 0,
    requestsRejected: 0,
    candidateCompleted: 0,
    candidateFailed: 0,
    primaryCompleted: 0,
    primaryFailed: 0,
    diffsMatched: 0,
    diffsRegressed: 0,
    breakerOpened: 0,
  }

  if (config.storePath) {
    await mkdir(dirname(config.storePath), { recursive: true, mode: 0o700 })
  }

  const ready = () => options.executeCandidate !== undefined || (config.candidateCommand?.length ?? 0) > 0
  const currentBreaker = async () => {
    breaker = await breakerStore.read()
    return breaker
  }
  const record = (event: unknown) => {
    if (!config.storePath) return
    persist = persist.then(() =>
      appendFile(config.storePath!, `${JSON.stringify({ storedAt: Date.now(), ...recordValue(event) })}\n`, {
        mode: 0o600,
      }),
    )
  }
  const compare = async (requestDigest: string) => {
    const left = primary.get(requestDigest)
    const right = candidate.get(requestDigest)
    if (!left || !right || diffs.has(requestDigest)) return
    const latencyRatio = ratioOf(right.durationMs, left.durationMs)
    const statusMatch = left.status === right.status
    const toolPlanDigestMatch = left.toolPlanDigest === right.toolPlanDigest
    const candidateFailed = right.status === "failed"
    const diff: ShadowCanaryDiff = {
      version: "opencode-shadow-canary-diff.v1",
      requestDigest,
      invocationID: left.invocationID,
      statusMatch,
      outputDigestMatch: left.outputDigest === right.outputDigest,
      toolPlanDigestMatch,
      candidateFailed,
      latencyRatio,
      usageDeltaRatio: ratioOf(right.usage.total - left.usage.total, Math.max(1, left.usage.total)),
      costDelta: right.cost.total - left.cost.total,
      regression:
        candidateFailed ||
        !statusMatch ||
        !toolPlanDigestMatch ||
        latencyRatio > config.latencyRatioThreshold,
      comparedAt: Date.now(),
    }
    diffs.set(requestDigest, diff)
    if (diff.regression) counters.diffsRegressed += 1
    else counters.diffsMatched += 1
    record({ type: "diff", diff })
    const wasOpen = breaker.open
    breaker = await breakerStore.record(diff)
    if (!wasOpen && breaker.open) {
      counters.breakerOpened += 1
      record({ type: "breaker.opened", breaker })
    }
  }
  const recordOutcome = async (outcome: ShadowCanaryOutcome) => {
    validateOutcome(outcome)
    const target = outcome.source === "primary" ? primary : candidate
    target.set(outcome.requestDigest, outcome)
    if (outcome.source === "primary") {
      if (outcome.status === "completed") counters.primaryCompleted += 1
      else counters.primaryFailed += 1
    } else if (outcome.status === "completed") counters.candidateCompleted += 1
    else counters.candidateFailed += 1
    record({ type: `${outcome.source}.outcome`, outcome })
    await compare(outcome.requestDigest)
  }
  const drain = () => {
    while (active < config.concurrency && jobs.length > 0) {
      const job = jobs.shift()!
      active += 1
      const task = job()
        .catch(() => undefined)
        .finally(() => {
          active -= 1
          running.delete(task)
          drain()
        })
      running.add(task)
    }
  }
  const enqueue = (job: () => Promise<void>) => {
    jobs.push(job)
    drain()
  }
  const execute = async (input: CandidateInput) => {
    let outcome: ShadowCanaryOutcome
    try {
      outcome = options.executeCandidate
        ? await options.executeCandidate(input)
        : await executeCandidateCommand(config, input)
    } catch (error) {
      outcome = failedOutcome(input, error)
    }
    if (outcome.requestDigest !== input.requestDigest || outcome.source !== "candidate") {
      outcome = failedOutcome(input, new Error("candidate outcome correlation mismatch"))
    }
    await recordOutcome(outcome)
  }

  const server = createServer((request, response) => {
    void handle(request, response).catch((error) => {
      if (response.headersSent) return response.destroy()
      response.statusCode = 500
      json(response, { error: "shadow_canary_executor_error", message: snapshotError(error) })
    })
  })
  server.requestTimeout = config.candidateTimeoutMs + 5_000
  server.headersTimeout = 10_000

  const handle = async (request: IncomingMessage, response: ServerResponse) => {
    secure(response)
    const path = new URL(request.url ?? "/", "http://localhost").pathname
    if (request.method === "GET" && path === "/healthz") return json(response, { status: "ok" })
    if (!authorized(request, config.bearerToken)) {
      response.statusCode = 401
      return json(response, { error: "unauthorized" })
    }
    if (request.method === "GET" && path === "/readyz") {
      const state = await currentBreaker()
      response.statusCode = ready() && !state.open ? 200 : 503
      return json(response, { ready: ready() && !state.open, breaker: state })
    }
    if (request.method === "GET" && path === "/metrics") {
      response.statusCode = 200
      response.setHeader("content-type", "text/plain; version=0.0.4; charset=utf-8")
      return response.end(metrics())
    }
    if (request.method === "GET" && path.startsWith("/v1/shadow/diffs/")) {
      const digest = path.slice("/v1/shadow/diffs/".length)
      const diff = diffs.get(digest)
      response.statusCode = diff ? 200 : 404
      return json(response, diff ?? { error: "not_found" })
    }
    if (request.method === "POST" && path === "/v1/shadow/requests") {
      if (!ready()) {
        counters.requestsRejected += 1
        response.statusCode = 503
        return json(response, { error: "candidate_executor_not_configured" })
      }
      const state = await currentBreaker()
      if (state.open) {
        counters.requestsRejected += 1
        response.statusCode = 503
        response.setHeader("retry-after", String(Math.ceil(config.cooldownMs / 1_000)))
        return json(response, { error: "shadow_canary_breaker_open", reason: state.reason })
      }
      const body = await readBody(request, config.maxBodyBytes)
      const requestDigest = sha256(body)
      if (request.headers["x-opencode-shadow-request-digest"] !== requestDigest) {
        response.statusCode = 400
        return json(response, { error: "request_digest_mismatch" })
      }
      const envelope = parseEnvelope(body, config.targetVersion)
      const invocationID = String(recordValue(envelope.invocation).id ?? "")
      const receiptID = `canary_${randomUUID()}`
      counters.requestsAccepted += 1
      record({ type: "request.accepted", requestDigest, invocationID, receiptID })
      enqueue(() => execute({ envelope, body, requestDigest, invocationID }))
      response.statusCode = 202
      return json(response, { receiptID })
    }
    if (request.method === "POST" && path === "/v1/shadow/outcomes/primary") {
      const body = await readBody(request, config.maxBodyBytes)
      const outcome = JSON.parse(body) as ShadowCanaryOutcome
      if (outcome.source !== "primary") {
        response.statusCode = 400
        return json(response, { error: "primary_outcome_required" })
      }
      await recordOutcome(outcome)
      response.statusCode = 202
      return json(response, { accepted: true })
    }
    response.statusCode = 404
    json(response, { error: "not_found" })
  }

  const metrics = () => {
    return [
      "# TYPE opencode_shadow_canary_requests_total counter",
      `opencode_shadow_canary_requests_total{result="accepted"} ${counters.requestsAccepted}`,
      `opencode_shadow_canary_requests_total{result="rejected"} ${counters.requestsRejected}`,
      "# TYPE opencode_shadow_canary_outcomes_total counter",
      `opencode_shadow_canary_outcomes_total{source="primary",status="completed"} ${counters.primaryCompleted}`,
      `opencode_shadow_canary_outcomes_total{source="primary",status="failed"} ${counters.primaryFailed}`,
      `opencode_shadow_canary_outcomes_total{source="candidate",status="completed"} ${counters.candidateCompleted}`,
      `opencode_shadow_canary_outcomes_total{source="candidate",status="failed"} ${counters.candidateFailed}`,
      "# TYPE opencode_shadow_canary_diffs_total counter",
      `opencode_shadow_canary_diffs_total{result="matched"} ${counters.diffsMatched}`,
      `opencode_shadow_canary_diffs_total{result="regression"} ${counters.diffsRegressed}`,
      "# TYPE opencode_shadow_canary_breaker_open gauge",
      `opencode_shadow_canary_breaker_open ${breaker.open ? 1 : 0}`,
      "# TYPE opencode_shadow_canary_candidate_failure_ratio gauge",
      `opencode_shadow_canary_candidate_failure_ratio ${breaker.failureRate}`,
      "# TYPE opencode_shadow_canary_structural_mismatch_ratio gauge",
      `opencode_shadow_canary_structural_mismatch_ratio ${breaker.structuralMismatchRate}`,
      "# TYPE opencode_shadow_canary_slow_ratio gauge",
      `opencode_shadow_canary_slow_ratio ${breaker.slowRate}`,
      "# TYPE opencode_shadow_canary_queue_depth gauge",
      `opencode_shadow_canary_queue_depth ${jobs.length}`,
      "",
    ].join("\n")
  }

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(config.port, config.host, () => {
      server.off("error", reject)
      resolve()
    })
  })
  const address = server.address()
  if (!address || typeof address === "string") throw new Error("Shadow canary executor did not bind a TCP address")
  const url = `http://${formatHost(config.host)}:${address.port}`

  return {
    url,
    snapshot(): ShadowCanaryExecutorSnapshot {
      return {
        ready: ready(),
        breaker,
        primaryOutcomes: primary.size,
        candidateOutcomes: candidate.size,
        diffs: [...diffs.values()],
        active,
        queued: jobs.length,
      }
    },
    async close() {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()))
        server.closeAllConnections()
      })
      while (running.size > 0) await Promise.all([...running])
      await persist
      await breakerStore.close()
    },
  }
}

async function executeCandidateCommand(config: ValidatedConfig, input: CandidateInput) {
  const command = config.candidateCommand
  if (!command || command.length === 0) throw new Error("candidate executor command is not configured")
  const workspace = await mkdtemp(join(tmpdir(), "opencode-shadow-candidate-"))
  try {
    const env: NodeJS.ProcessEnv = {
      PATH: process.env.PATH,
      HOME: process.env.HOME,
      TMPDIR: process.env.TMPDIR,
      OPENCODE_SHADOW_ONLY: "1",
      OPENCODE_SHADOW_TOOLS_DISABLED: "1",
      OPENCODE_SHADOW_REQUEST_DIGEST: input.requestDigest,
      OPENCODE_SHADOW_INVOCATION_ID: input.invocationID,
      OPENCODE_SHADOW_TARGET_VERSION: config.targetVersion,
      OPENCODE_SHADOW_CANDIDATE_CHECKOUT: config.candidateCheckout,
      OPENCODE_SHADOW_CANDIDATE_CONFIG_CONTENT: config.candidateConfigContent,
      OPENCODE_SHADOW_CANDIDATE_AUTH_CONTENT: config.candidateAuthContent,
    }
    for (const name of config.candidateEnvAllowlist ?? []) {
      if (process.env[name] !== undefined) env[name] = process.env[name]
    }
    const child = spawn(command[0]!, command.slice(1), { cwd: workspace, env, stdio: ["pipe", "pipe", "pipe"] })
    child.stdin.end(input.body)
    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk))
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk))
    const exitCode = await new Promise<number | null>((resolve, reject) => {
      const timeout = setTimeout(() => {
        child.kill("SIGKILL")
        reject(new Error(`candidate executor timed out after ${config.candidateTimeoutMs}ms`))
      }, config.candidateTimeoutMs)
      child.once("error", (error) => {
        clearTimeout(timeout)
        reject(error)
      })
      child.once("close", (code) => {
        clearTimeout(timeout)
        resolve(code)
      })
    })
    if (exitCode !== 0) {
      throw new Error(`candidate executor exited ${exitCode}: ${Buffer.concat(stderr).toString("utf8").slice(0, 2_048)}`)
    }
    const text = Buffer.concat(stdout).toString("utf8")
    if (Buffer.byteLength(text) > 1_048_576) throw new Error("candidate executor output is too large")
    return JSON.parse(text) as ShadowCanaryOutcome
  } finally {
    await rm(workspace, { recursive: true, force: true })
  }
}

function failedOutcome(input: CandidateInput, error: unknown): ShadowCanaryOutcome {
  return {
    version: "opencode-shadow-canary-outcome.v1",
    source: "candidate",
    requestDigest: input.requestDigest,
    invocationID: input.invocationID,
    status: "failed",
    durationMs: 0,
    outputDigest: sha256(""),
    toolPlanDigest: sha256(""),
    eventCount: 0,
    toolCalls: 0,
    usage: { input: 0, output: 0, reasoning: 0, total: 0 },
    cost: { total: 0, currency: "USD" },
    error: snapshotError(error),
  }
}

function parseEnvelope(body: string, targetVersion: string) {
  const value = JSON.parse(body) as Record<string, unknown>
  if (value.version !== "opencode-shadow-canary-request.v1" || value.shadowOnly !== true) {
    throw new Error("invalid shadow canary request envelope")
  }
  if (value.targetVersion !== targetVersion) throw new Error("shadow canary target version mismatch")
  const invocation = recordValue(value.invocation)
  if (typeof invocation.id !== "string" || typeof invocation.sessionID !== "string") {
    throw new Error("shadow canary invocation correlation is missing")
  }
  return value
}

function validateOutcome(value: ShadowCanaryOutcome) {
  if (value.version !== "opencode-shadow-canary-outcome.v1") throw new Error("invalid shadow canary outcome")
  if (value.source !== "primary" && value.source !== "candidate") throw new Error("invalid shadow canary outcome source")
  if (!value.requestDigest || !value.invocationID) throw new Error("shadow canary outcome correlation is missing")
  if (value.status !== "completed" && value.status !== "failed") throw new Error("invalid shadow canary outcome status")
}

async function readBody(request: IncomingMessage, maxBytes: number) {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    size += value.length
    if (size > maxBytes) throw new Error("shadow canary request body is too large")
    chunks.push(value)
  }
  return Buffer.concat(chunks).toString("utf8")
}

function validate(input: ShadowCanaryExecutorConfig): ValidatedConfig {
  const host = input.host ?? "127.0.0.1"
  const bearerToken = optional(input.bearerToken)
  if (!isLoopback(host) && !bearerToken) throw new Error("non-loopback shadow canary executor requires bearer token")
  if (bearerToken && bearerToken.length < 16) throw new Error("shadow canary bearer token must contain at least 16 characters")
  const windowSize = input.windowSize ?? 100
  const minimumSamples = input.minimumSamples ?? 20
  if (minimumSamples > windowSize) throw new Error("shadow canary breaker minimum samples exceeds window size")
  return {
    host,
    port: input.port ?? 9470,
    bearerToken,
    targetVersion: input.targetVersion ?? "v1.18.8",
    candidateCommand: input.candidateCommand ?? (input.candidateCheckout ? candidateModelExecutorCommand() : undefined),
    candidateCheckout: input.candidateCheckout ? resolve(input.candidateCheckout) : undefined,
    candidateConfigContent: input.candidateConfigContent,
    candidateAuthContent: input.candidateAuthContent,
    candidateEnvAllowlist: input.candidateEnvAllowlist,
    candidateTimeoutMs: input.candidateTimeoutMs ?? 120_000,
    maxBodyBytes: input.maxBodyBytes ?? 1_048_576,
    concurrency: input.concurrency ?? 2,
    storePath: input.storePath,
    windowSize,
    minimumSamples,
    failureRateThreshold: input.failureRateThreshold ?? 0.1,
    structuralMismatchRateThreshold: input.structuralMismatchRateThreshold ?? 0.2,
    slowRateThreshold: input.slowRateThreshold ?? 0.25,
    latencyRatioThreshold: input.latencyRatioThreshold ?? 2,
    cooldownMs: input.cooldownMs ?? 300_000,
  }
}

function candidateModelExecutorCommand() {
  return [process.execPath, resolve(import.meta.dir, "../../../script/shadow-canary-v1-18-8-model-executor.ts")]
}

function authorized(request: IncomingMessage, token: string | undefined) {
  if (!token) return true
  const value = request.headers.authorization
  return value?.startsWith("Bearer ") === true && safeEqual(value.slice(7), token)
}

function safeEqual(left: string, right: string) {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.length === b.length && timingSafeEqual(a, b)
}

function secure(response: ServerResponse) {
  response.setHeader("cache-control", "no-store")
  response.setHeader("x-content-type-options", "nosniff")
}

function json(response: ServerResponse, value: unknown) {
  response.setHeader("content-type", "application/json; charset=utf-8")
  response.end(JSON.stringify(value))
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}

function snapshotError(error: unknown) {
  return (error instanceof Error ? error.message : String(error)).slice(0, 2_048)
}

function sha256(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function ratioOf(value: number, baseline: number) {
  if (!Number.isFinite(value) || !Number.isFinite(baseline)) return 0
  return baseline === 0 ? (value === 0 ? 1 : Number.MAX_SAFE_INTEGER) : value / baseline
}

function isLoopback(host: string) {
  return host === "127.0.0.1" || host === "::1" || host === "localhost"
}

function formatHost(host: string) {
  return host.includes(":") ? `[${host}]` : host
}

function optional(value: string | undefined) {
  const result = value?.trim()
  return result ? result : undefined
}

function csv(value: string | undefined) {
  return value?.split(",").map((item) => item.trim()).filter(Boolean)
}

function command(value: string | undefined) {
  if (!value) return undefined
  const parsed = JSON.parse(value)
  if (!Array.isArray(parsed) || parsed.length === 0 || parsed.some((item) => typeof item !== "string" || !item)) {
    throw new Error("OPENCODE_SHADOW_CANARY_EXECUTOR_COMMAND_JSON must be a non-empty string array")
  }
  return parsed as string[]
}

function integer(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) throw new Error(`expected integer ${min}..${max}`)
  return parsed
}

function number(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) throw new Error(`expected number ${min}..${max}`)
  return parsed
}

function ratio(value: string | undefined, fallback: number) {
  return number(value, fallback, 0, 1)
}
