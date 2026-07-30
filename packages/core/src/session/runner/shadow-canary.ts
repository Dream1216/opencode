import type { LLMEvent, LLMRequest } from "@opencode-ai/llm"
import { createHash } from "node:crypto"

type Environment = Readonly<Record<string, string | undefined>>

export type ShadowCanaryConfig = {
  readonly enabled: boolean
  readonly endpoint?: string
  readonly outcomeEndpoint?: string
  readonly token?: string
  readonly sourceVersion: string
  readonly targetVersion: string
  readonly sampleRate: number
  readonly timeoutMs: number
  readonly maxPayloadBytes: number
  readonly invalidReason?: string
}

type PrepareInput = {
  readonly invocationID: string
  readonly sessionID: string
  readonly agent: string
  readonly model: unknown
  readonly request: LLMRequest
}

export type ShadowCanaryPrepared = {
  readonly status: "ready"
  readonly body: string
  readonly requestDigest: string
  readonly payloadBytes: number
}

export type ShadowCanarySkipped = {
  readonly status: "skipped"
  readonly reason: "disabled" | "invalid-config" | "sampled-out" | "payload-too-large"
}

export type ShadowCanaryDispatchResult =
  | {
      readonly status: "accepted"
      readonly durationMs: number
      readonly statusCode: number
      readonly receiptID?: string
    }
  | {
      readonly status: "failed"
      readonly durationMs: number
      readonly failureType: "config" | "timeout" | "network" | "http"
      readonly error: string
      readonly statusCode?: number
    }

export type ShadowCanaryOutcome = {
  readonly version: "opencode-shadow-canary-outcome.v1"
  readonly source: "primary" | "candidate"
  readonly requestDigest: string
  readonly invocationID: string
  readonly status: "completed" | "failed"
  readonly durationMs: number
  readonly outputDigest: string
  readonly toolPlanDigest: string
  readonly eventCount: number
  readonly toolCalls: number
  readonly usage: {
    readonly input: number
    readonly output: number
    readonly reasoning: number
    readonly total: number
  }
  readonly cost: {
    readonly total: number
    readonly currency: string
  }
  readonly error?: string
}

export const ShadowCanary = {
  config(env: Environment = process.env): ShadowCanaryConfig {
    const enabled = boolean(env.OPENCODE_SHADOW_CANARY_ENABLED)
    const sourceVersion = env.OPENCODE_SHADOW_CANARY_SOURCE_VERSION?.trim() || "v1.18.8"
    const targetVersion = env.OPENCODE_SHADOW_CANARY_TARGET_VERSION?.trim() || "v1.18.8"
    const sampleRate = bounded(env.OPENCODE_SHADOW_CANARY_SAMPLE_RATE, 0.01, 0, 1)
    const timeoutMs = bounded(env.OPENCODE_SHADOW_CANARY_TIMEOUT_MS, 2_000, 10, 30_000)
    const maxPayloadBytes = bounded(env.OPENCODE_SHADOW_CANARY_MAX_BYTES, 1_048_576, 1_024, 4_194_304)
    const endpoint = env.OPENCODE_SHADOW_CANARY_URL?.trim()
    const explicitOutcomeEndpoint = env.OPENCODE_SHADOW_CANARY_OUTCOME_URL?.trim()
    let outcomeEndpoint = explicitOutcomeEndpoint
    if (!outcomeEndpoint && endpoint) {
      try {
        outcomeEndpoint = new URL("/v1/shadow/outcomes/primary", endpoint).toString()
      } catch {
        // The request endpoint validation below reports the malformed URL.
      }
    }
    const token = env.OPENCODE_SHADOW_CANARY_TOKEN?.trim()
    const issues: string[] = []

    if (enabled && !endpoint) issues.push("OPENCODE_SHADOW_CANARY_URL is required")
    if (enabled && endpoint) validateEndpoint(endpoint, token, "request", issues)
    if (enabled && outcomeEndpoint) validateEndpoint(outcomeEndpoint, token, "outcome", issues)
    if (
      enabled &&
      endpoint &&
      outcomeEndpoint &&
      !boolean(env.OPENCODE_SHADOW_CANARY_ALLOW_CROSS_ORIGIN_OUTCOME)
    ) {
      try {
        if (new URL(endpoint).origin !== new URL(outcomeEndpoint).origin) {
          issues.push("shadow canary outcome URL must use the request URL origin")
        }
      } catch {
        // Endpoint-specific validation reports malformed URLs.
      }
    }

    return {
      enabled,
      endpoint,
      outcomeEndpoint,
      token,
      sourceVersion,
      targetVersion,
      sampleRate,
      timeoutMs,
      maxPayloadBytes,
      invalidReason: issues.length > 0 ? issues.join("; ") : undefined,
    }
  },

  prepare(config: ShadowCanaryConfig, input: PrepareInput): ShadowCanaryPrepared | ShadowCanarySkipped {
    if (!config.enabled) return { status: "skipped", reason: "disabled" }
    if (config.invalidReason || !config.endpoint) return { status: "skipped", reason: "invalid-config" }
    if (!sampled(`${input.sessionID}:${input.invocationID}`, config.sampleRate)) {
      return { status: "skipped", reason: "sampled-out" }
    }

    const body = JSON.stringify({
      version: "opencode-shadow-canary-request.v1",
      shadowOnly: true,
      sourceVersion: config.sourceVersion,
      targetVersion: config.targetVersion,
      createdAt: new Date().toISOString(),
      invocation: {
        id: input.invocationID,
        sessionID: input.sessionID,
        agent: input.agent,
        model: sanitize(input.model),
        provider: input.request.model.provider,
        route: input.request.model.route.id,
      },
      request: requestSnapshot(input.request),
    })
    const payloadBytes = Buffer.byteLength(body)
    if (payloadBytes > config.maxPayloadBytes) return { status: "skipped", reason: "payload-too-large" }
    return {
      status: "ready",
      body,
      requestDigest: sha256(body),
      payloadBytes,
    }
  },

  async dispatch(
    config: ShadowCanaryConfig,
    prepared: ShadowCanaryPrepared,
  ): Promise<ShadowCanaryDispatchResult> {
    const started = Date.now()
    if (!config.endpoint || config.invalidReason) {
      return {
        status: "failed",
        durationMs: 0,
        failureType: "config",
        error: config.invalidReason ?? "shadow canary endpoint is missing",
      }
    }

    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), config.timeoutMs)
    try {
      const response = await fetch(config.endpoint, {
        method: "POST",
        redirect: "error",
        signal: controller.signal,
        headers: {
          "content-type": "application/json",
          "x-opencode-shadow-canary": "1",
          "x-opencode-shadow-target-version": config.targetVersion,
          "x-opencode-shadow-request-digest": prepared.requestDigest,
          ...(config.token ? { authorization: `Bearer ${config.token}` } : {}),
        },
        body: prepared.body,
      })
      const durationMs = Date.now() - started
      const responseBody = (await response.text()).slice(0, 4_096)
      if (!response.ok) {
        return {
          status: "failed",
          durationMs,
          failureType: "http",
          error: scrub(responseBody || `shadow canary returned HTTP ${response.status}`),
          statusCode: response.status,
        }
      }
      return {
        status: "accepted",
        durationMs,
        statusCode: response.status,
        receiptID: receiptID(responseBody),
      }
    } catch (error) {
      return {
        status: "failed",
        durationMs: Date.now() - started,
        failureType: controller.signal.aborted ? "timeout" : "network",
        error: controller.signal.aborted ? `shadow canary timed out after ${config.timeoutMs}ms` : snapshotError(error),
      }
    } finally {
      clearTimeout(timeout)
    }
  },

  async reportOutcome(config: ShadowCanaryConfig, outcome: ShadowCanaryOutcome): Promise<ShadowCanaryDispatchResult> {
    const started = Date.now()
    if (!config.outcomeEndpoint || config.invalidReason) {
      return {
        status: "failed",
        durationMs: 0,
        failureType: "config",
        error: config.invalidReason ?? "shadow canary outcome endpoint is missing",
      }
    }
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), config.timeoutMs)
    try {
      const response = await fetch(config.outcomeEndpoint, {
        method: "POST",
        redirect: "error",
        signal: controller.signal,
        headers: {
          "content-type": "application/json",
          "x-opencode-shadow-canary": "1",
          ...(config.token ? { authorization: `Bearer ${config.token}` } : {}),
        },
        body: JSON.stringify(outcome),
      })
      const durationMs = Date.now() - started
      if (!response.ok) {
        return {
          status: "failed",
          durationMs,
          failureType: "http",
          error: `shadow outcome collector returned HTTP ${response.status}`,
          statusCode: response.status,
        }
      }
      return { status: "accepted", durationMs, statusCode: response.status }
    } catch (error) {
      return {
        status: "failed",
        durationMs: Date.now() - started,
        failureType: controller.signal.aborted ? "timeout" : "network",
        error: controller.signal.aborted ? `shadow outcome collector timed out after ${config.timeoutMs}ms` : snapshotError(error),
      }
    } finally {
      clearTimeout(timeout)
    }
  },
}

function validateEndpoint(value: string, token: string | undefined, label: "request" | "outcome", issues: string[]) {
  try {
    const url = new URL(value)
    const loopback =
      url.hostname === "localhost" ||
      url.hostname === "127.0.0.1" ||
      url.hostname === "::1" ||
      url.hostname === "[::1]"
    if (url.username || url.password) issues.push(`shadow canary ${label} URL must not contain credentials`)
    if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) {
      issues.push(`remote shadow canary ${label} URL must use HTTPS`)
    }
    if (!loopback && !token) {
      issues.push(`remote shadow canary ${label} requires OPENCODE_SHADOW_CANARY_TOKEN`)
    }
  } catch {
    issues.push(`OPENCODE_SHADOW_CANARY_${label === "request" ? "URL" : "OUTCOME_URL"} is invalid`)
  }
}

export function createShadowCanaryOutcomeAccumulator() {
  const output: string[] = []
  const tools: string[] = []
  let eventCount = 0
  let toolCalls = 0

  return {
    observe(event: LLMEvent) {
      eventCount += 1
      const snapshot = JSON.stringify(sanitize(event)).slice(0, 16_384)
      if (output.length < 512) output.push(snapshot)
      const type = String((event as unknown as { type?: unknown }).type ?? "")
      if (type.includes("tool")) {
        toolCalls += 1
        if (tools.length < 256) tools.push(snapshot)
      }
    },
    snapshot(input: {
      readonly source: "primary" | "candidate"
      readonly requestDigest: string
      readonly invocationID: string
      readonly status: "completed" | "failed"
      readonly durationMs: number
      readonly usage: ShadowCanaryOutcome["usage"]
      readonly cost: ShadowCanaryOutcome["cost"]
      readonly error?: string
    }): ShadowCanaryOutcome {
      return {
        version: "opencode-shadow-canary-outcome.v1",
        ...input,
        outputDigest: sha256(output.join("\n")),
        toolPlanDigest: sha256(tools.join("\n")),
        eventCount,
        toolCalls,
      }
    },
  }
}

function requestSnapshot(request: LLMRequest) {
  const source = request as unknown as Record<string, unknown>
  const snapshot: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(source)) {
    if (key === "model" || key === "signal" || key === "abortSignal") continue
    const next = sanitize(value, key)
    if (next !== undefined) snapshot[key] = next
  }
  snapshot.model = {
    provider: request.model.provider,
    route: request.model.route.id,
  }
  return snapshot
}

function sanitize(value: unknown, key?: string, seen = new WeakSet<object>(), depth = 0): unknown {
  if (key && sensitiveKey(key)) return "[REDACTED]"
  if (value === undefined || typeof value === "function" || typeof value === "symbol") return undefined
  if (value === null || typeof value === "boolean" || typeof value === "number") return value
  if (typeof value === "string") return scrub(value)
  if (typeof value === "bigint") return value.toString()
  if (depth >= 20) return "[MAX_DEPTH]"
  if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) return "[BINARY_OMITTED]"
  if (Array.isArray(value)) {
    return value.flatMap((item) => {
      const next = sanitize(item, undefined, seen, depth + 1)
      return next === undefined ? [] : [next]
    })
  }
  if (typeof value !== "object") return String(value)
  if (seen.has(value)) return "[CIRCULAR]"
  seen.add(value)
  const result: Record<string, unknown> = {}
  for (const [childKey, childValue] of Object.entries(value)) {
    const next = sanitize(childValue, childKey, seen, depth + 1)
    if (next !== undefined) result[childKey] = next
  }
  seen.delete(value)
  return result
}

function sensitiveKey(key: string) {
  const normalized = key.toLowerCase().replace(/[^a-z]/g, "")
  return new Set([
    "authorization",
    "auth",
    "apikey",
    "accesstoken",
    "refreshtoken",
    "token",
    "secret",
    "clientsecret",
    "password",
    "credential",
    "credentials",
    "cookie",
    "cookies",
    "header",
    "headers",
  ]).has(normalized)
}

function scrub(value: string) {
  return value
    .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [REDACTED]")
    .replace(/\bsk-[A-Za-z0-9_-]{12,}\b/g, "sk-[REDACTED]")
}

function receiptID(body: string) {
  if (!body) return undefined
  try {
    const value = JSON.parse(body) as { receiptID?: unknown; id?: unknown }
    if (typeof value.receiptID === "string") return value.receiptID.slice(0, 256)
    if (typeof value.id === "string") return value.id.slice(0, 256)
  } catch {}
  return undefined
}

function snapshotError(error: unknown) {
  return scrub(error instanceof Error ? error.message : String(error)).slice(0, 1_024)
}

function sampled(key: string, rate: number) {
  if (rate <= 0) return false
  if (rate >= 1) return true
  const value = createHash("sha256").update(key).digest().readUInt32BE(0) / 0x1_0000_0000
  return value < rate
}

function sha256(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function boolean(value: string | undefined) {
  const normalized = value?.trim().toLowerCase()
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on"
}

function bounded(value: string | undefined, fallback: number, minimum: number, maximum: number) {
  const parsed = value === undefined ? fallback : Number(value)
  return Number.isFinite(parsed) && parsed >= minimum && parsed <= maximum ? parsed : fallback
}
