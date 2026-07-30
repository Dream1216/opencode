import { createServer, type IncomingMessage, type ServerResponse } from "node:http"
import { randomBytes, timingSafeEqual } from "node:crypto"
import { signRequest } from "../database/postgres/worker-queue-admin"

const metricsTarget = "/experimental/worker-queue/metrics"

export type WorkerQueuePrometheusProxyConfig = {
  readonly upstreamURL: string
  readonly actorID: string
  readonly actorSecret: string
  readonly actorKeyID?: string
  readonly host?: string
  readonly port?: number
  readonly bearerToken?: string
  readonly upstreamUsername?: string
  readonly upstreamPassword?: string
  readonly timeoutMs?: number
  readonly maxResponseBytes?: number
}

export type WorkerQueuePrometheusProxy = {
  readonly url: string
  readonly close: () => Promise<void>
}

export function prometheusProxyConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): WorkerQueuePrometheusProxyConfig | undefined {
  if (env.OPENCODE_WORKER_QUEUE_PROMETHEUS_PROXY_ENABLED !== "1") return undefined
  return {
    upstreamURL: required(
      env.OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_URL,
      "OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_URL",
    ),
    actorID: required(
      env.OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_ID,
      "OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_ID",
    ),
    actorSecret: required(
      env.OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_SECRET,
      "OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_SECRET",
    ),
    actorKeyID: optional(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_KEY_ID),
    host: env.OPENCODE_WORKER_QUEUE_PROMETHEUS_HOST ?? "127.0.0.1",
    port: integer(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_PORT, 9465, 1, 65_535),
    bearerToken: optional(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_BEARER_TOKEN),
    upstreamUsername: optional(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_USERNAME),
    upstreamPassword: optional(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_PASSWORD),
    timeoutMs: integer(env.OPENCODE_WORKER_QUEUE_PROMETHEUS_TIMEOUT_MS, 5_000, 250, 60_000),
    maxResponseBytes: integer(
      env.OPENCODE_WORKER_QUEUE_PROMETHEUS_MAX_RESPONSE_BYTES,
      1024 * 1024,
      1024,
      16 * 1024 * 1024,
    ),
  }
}

export async function startWorkerQueuePrometheusProxy(
  input: WorkerQueuePrometheusProxyConfig,
): Promise<WorkerQueuePrometheusProxy> {
  const config = validate(input)
  const server = createServer((request, response) => {
    void handle(request, response, config).catch(() => internalError(response))
  })
  server.requestTimeout = config.timeoutMs + 1_000
  server.headersTimeout = config.timeoutMs + 2_000
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(config.port, config.host, () => {
      server.off("error", reject)
      resolve()
    })
  })
  const address = server.address()
  if (address === null || typeof address === "string") {
    server.close()
    throw new Error("Worker queue Prometheus proxy did not bind a TCP address")
  }
  return {
    url: `http://${formatHost(config.host)}:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error === undefined ? resolve() : reject(error)))
        server.closeAllConnections()
      }),
  }
}

type ValidatedConfig = Required<
  Pick<WorkerQueuePrometheusProxyConfig, "upstreamURL" | "actorID" | "actorSecret" | "host" | "port" | "timeoutMs" | "maxResponseBytes">
> &
  Pick<
    WorkerQueuePrometheusProxyConfig,
    "actorKeyID" | "bearerToken" | "upstreamUsername" | "upstreamPassword"
  >

function validate(input: WorkerQueuePrometheusProxyConfig): ValidatedConfig {
  const upstream = new URL(input.upstreamURL)
  if (upstream.protocol !== "http:" && upstream.protocol !== "https:") {
    throw new Error("Worker queue Prometheus upstream must use HTTP or HTTPS")
  }
  if (input.actorID.trim() === "") throw new Error("Worker queue Prometheus actor ID is required")
  if (input.actorSecret.length < 16) throw new Error("Worker queue Prometheus actor secret must contain at least 16 characters")
  const host = input.host ?? "127.0.0.1"
  const bearerToken = optional(input.bearerToken)
  if (!isLoopback(host) && bearerToken === undefined) {
    throw new Error("A bearer token is required when the Worker queue Prometheus proxy binds outside loopback")
  }
  if (bearerToken !== undefined && bearerToken.length < 16) {
    throw new Error("Worker queue Prometheus bearer token must contain at least 16 characters")
  }
  if (input.upstreamUsername !== undefined && input.upstreamPassword === undefined) {
    throw new Error("Worker queue Prometheus upstream password is required when username is configured")
  }
  return {
    upstreamURL: upstream.toString(),
    actorID: input.actorID,
    actorSecret: input.actorSecret,
    actorKeyID: input.actorKeyID,
    host,
    port: input.port ?? 9465,
    bearerToken,
    upstreamUsername: input.upstreamUsername,
    upstreamPassword: input.upstreamPassword,
    timeoutMs: input.timeoutMs ?? 5_000,
    maxResponseBytes: input.maxResponseBytes ?? 1024 * 1024,
  }
}

async function handle(request: IncomingMessage, response: ServerResponse, config: ValidatedConfig) {
  secure(response)
  if (!authorized(request, config.bearerToken)) {
    response.statusCode = 401
    response.setHeader("www-authenticate", 'Bearer realm="opencode-worker-queue-metrics"')
    return json(response, { error: "unauthorized" })
  }
  if (request.method !== "GET") {
    response.statusCode = 405
    response.setHeader("allow", "GET")
    return json(response, { error: "method_not_allowed" })
  }
  const path = new URL(request.url ?? "/", "http://localhost").pathname
  if (path === "/healthz") return json(response, { status: "ok" })
  if (path === "/metrics") {
    const metrics = await fetchMetrics(config)
    response.statusCode = 200
    response.setHeader("content-type", "text/plain; version=0.0.4; charset=utf-8")
    return response.end(metrics)
  }
  if (path === "/readyz") {
    await fetchMetrics(config)
    return json(response, { ready: true })
  }
  response.statusCode = 404
  return json(response, { error: "not_found" })
}

async function fetchMetrics(config: ValidatedConfig) {
  const timestamp = Date.now()
  const nonce = randomBytes(18).toString("base64url")
  const unsigned = {
    method: "GET",
    target: metricsTarget,
    actorID: config.actorID,
    timestamp,
    nonce,
    ...(config.actorKeyID === undefined ? {} : { keyID: config.actorKeyID }),
    body: "",
  }
  const headers: Record<string, string> = {
    "x-opencode-actor-id": config.actorID,
    "x-opencode-request-timestamp": String(timestamp),
    "x-opencode-request-nonce": nonce,
    "x-opencode-request-signature": signRequest({ ...unsigned, secret: config.actorSecret }),
  }
  if (config.actorKeyID !== undefined) headers["x-opencode-request-key-id"] = config.actorKeyID
  if (config.upstreamPassword !== undefined) {
    const username = config.upstreamUsername ?? "opencode"
    headers.authorization = `Basic ${Buffer.from(`${username}:${config.upstreamPassword}`).toString("base64")}`
  }
  const response = await fetch(new URL(metricsTarget, config.upstreamURL), {
    method: "GET",
    headers,
    signal: AbortSignal.timeout(config.timeoutMs),
  })
  if (response.status !== 200) throw new Error(`Worker queue metrics upstream returned ${response.status}`)
  const contentType = response.headers.get("content-type") ?? ""
  if (!contentType.toLowerCase().startsWith("text/plain")) {
    throw new Error("Worker queue metrics upstream did not return Prometheus text")
  }
  const contentLength = Number(response.headers.get("content-length") ?? 0)
  if (Number.isFinite(contentLength) && contentLength > config.maxResponseBytes) {
    throw new Error("Worker queue metrics upstream response is too large")
  }
  const body = await response.text()
  if (Buffer.byteLength(body) > config.maxResponseBytes) {
    throw new Error("Worker queue metrics upstream response is too large")
  }
  if (body.includes(config.actorID)) throw new Error("Worker queue metrics upstream exposed actor identity")
  return body
}

function authorized(request: IncomingMessage, token: string | undefined) {
  if (token === undefined) return true
  const value = request.headers.authorization
  if (value === undefined || !value.startsWith("Bearer ")) return false
  return safeEqual(value.slice("Bearer ".length), token)
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

function internalError(response: ServerResponse) {
  if (response.headersSent) return response.destroy()
  secure(response)
  response.statusCode = 502
  json(response, { error: "worker_queue_metrics_upstream_unavailable" })
}

function isLoopback(host: string) {
  return host === "127.0.0.1" || host === "::1" || host === "localhost"
}

function formatHost(host: string) {
  return host.includes(":") ? `[${host}]` : host
}

function required(value: string | undefined, name: string) {
  const result = optional(value)
  if (result === undefined) throw new Error(`${name} is required`)
  return result
}

function optional(value: string | undefined) {
  const result = value?.trim()
  return result === undefined || result === "" ? undefined : result
}

function integer(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < min || result > max) {
    throw new Error(`Expected an integer between ${min} and ${max}`)
  }
  return result
}
