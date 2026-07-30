import { createServer, type RequestListener } from "node:http"
import { MeterProvider } from "@opentelemetry/sdk-metrics"
import { makeMetricReader } from "./otlp"
import {
  observeWorkerQueue,
  registerWorkerQueueTelemetry,
} from "../database/postgres/worker-queue-telemetry"
import { signRequest } from "../database/postgres/worker-queue-admin"
import { startWorkerQueuePrometheusProxy } from "./worker-queue-prometheus-proxy"

export async function runWorkerQueueTelemetryExportSmoke() {
  const checks: string[] = []
  await verifyOtlp(checks)
  await verifyPrometheusProxy(checks)
  return { status: "ok" as const, checks }
}

async function verifyOtlp(checks: string[]) {
  const requests: { readonly headers: Headers; readonly body: Buffer }[] = []
  const collector = await testServer(async (request, response) => {
    if (request.method !== "POST" || request.url !== "/v1/metrics") {
      response.statusCode = 404
      return response.end()
    }
    const chunks: Buffer[] = []
    for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    requests.push({ headers: new Headers(request.headers as Record<string, string>), body: Buffer.concat(chunks) })
    response.statusCode = 200
    response.setHeader("content-type", "application/x-protobuf")
    response.end()
  })
  const reader = await makeMetricReader({
    endpoint: collector.url,
    headers: { "x-opencode-smoke": "worker-queue-otlp" },
    exportIntervalMillis: 60_000,
  })
  if (reader === undefined) throw new Error("OTLP metric reader was not created")
  const provider = new MeterProvider({ readers: [reader] })
  registerWorkerQueueTelemetry(provider.getMeter("opencode.worker-queue-smoke", "1.0.0"))
  observeWorkerQueue(
    {
      ready: true,
      degraded: false,
      reasons: [],
      metrics: {
        pending: 2,
        running: 1,
        completed: 3,
        failed: 0,
        cancelled: 0,
        expiredRunning: 0,
        oldestPendingAgeMs: 125,
      },
    },
    { tenantID: "tenant-otel-smoke", teamID: "team-otel-smoke" },
  )
  try {
    await provider.forceFlush()
    const exported = requests.find((request) => request.headers.get("x-opencode-smoke") === "worker-queue-otlp")
    if (exported === undefined || exported.body.length === 0) {
      throw new Error("OTLP metric receiver did not receive a payload")
    }
    checks.push("otlp-http-metrics-exported")
    if (
      !exported.body.includes(Buffer.from("opencode.worker_queue.ready")) ||
      !exported.body.includes(Buffer.from("tenant-otel-smoke"))
    ) {
      throw new Error("OTLP payload did not contain queue instruments and tenant attributes")
    }
    checks.push("otlp-queue-instruments-observed")
  } finally {
    await provider.shutdown()
    await collector.close()
  }
}

async function verifyPrometheusProxy(checks: string[]) {
  const actorID = "metrics-viewer-smoke"
  const actorSecret = "metrics-viewer-secret-0123456789"
  const bearerToken = "prometheus-scrape-token-0123456789"
  const nonces = new Set<string>()
  let upstreamStatus = 200
  const metrics = [
    "# TYPE opencode_worker_queue_ready gauge",
    'opencode_worker_queue_ready{tenant_id="tenant-prom-smoke",team_id="team-prom-smoke"} 1',
    "",
  ].join("\n")
  const upstream = await testServer((request, response) => {
    const timestamp = Number(request.headers["x-opencode-request-timestamp"] ?? Number.NaN)
    const nonce = String(request.headers["x-opencode-request-nonce"] ?? "")
    const signature = String(request.headers["x-opencode-request-signature"] ?? "")
    const requestActor = String(request.headers["x-opencode-actor-id"] ?? "")
    const expected = signRequest({
      method: request.method ?? "",
      target: request.url ?? "",
      actorID: requestActor,
      timestamp,
      nonce,
      body: "",
      secret: actorSecret,
    })
    if (
      request.method !== "GET" ||
      request.url !== "/experimental/worker-queue/metrics" ||
      requestActor !== actorID ||
      Math.abs(Date.now() - timestamp) > 5_000 ||
      !/^[a-zA-Z0-9_-]{16,128}$/.test(nonce) ||
      signature !== expected ||
      nonces.has(nonce)
    ) {
      response.statusCode = 401
      return response.end()
    }
    nonces.add(nonce)
    response.statusCode = upstreamStatus
    response.setHeader("content-type", "text/plain; version=0.0.4")
    response.end(upstreamStatus === 200 ? metrics : "unavailable")
  })
  const proxy = await startWorkerQueuePrometheusProxy({
    upstreamURL: upstream.url,
    actorID,
    actorSecret,
    host: "127.0.0.1",
    port: 0,
    bearerToken,
    timeoutMs: 2_000,
  })
  try {
    const unsigned = await fetch(`${proxy.url}/metrics`)
    if (unsigned.status !== 401) throw new Error("Prometheus proxy accepted an unsigned scrape")
    checks.push("prometheus-unsigned-scrape-blocked")

    const first = await fetch(`${proxy.url}/metrics`, { headers: { authorization: `Bearer ${bearerToken}` } })
    const body = await first.text()
    if (first.status !== 200 || !body.includes("opencode_worker_queue_ready")) {
      throw new Error("Prometheus proxy did not return signed upstream metrics")
    }
    checks.push("prometheus-hmac-upstream-signed")
    if (body.includes(actorID)) throw new Error("Prometheus proxy exposed actor identity")
    checks.push("prometheus-actor-identity-not-exposed")

    const ready = await fetch(`${proxy.url}/readyz`, { headers: { authorization: `Bearer ${bearerToken}` } })
    if (ready.status !== 200 || nonces.size !== 2) throw new Error("Prometheus proxy did not use a unique readiness nonce")
    checks.push("prometheus-nonce-unique")

    upstreamStatus = 503
    const unavailable = await fetch(`${proxy.url}/metrics`, {
      headers: { authorization: `Bearer ${bearerToken}` },
    })
    if (unavailable.status !== 502) throw new Error("Prometheus proxy did not fail closed on upstream failure")
    checks.push("prometheus-upstream-failure-fail-closed")
  } finally {
    await proxy.close()
    await upstream.close()
  }

  await expectRejected(() =>
    startWorkerQueuePrometheusProxy({
      upstreamURL: "http://127.0.0.1:1",
      actorID,
      actorSecret,
      host: "0.0.0.0",
      port: 0,
    }),
  )
  checks.push("prometheus-non-loopback-without-token-blocked")
}

async function testServer(listener: RequestListener) {
  const server = createServer(listener)
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject)
      resolve()
    })
  })
  const address = server.address()
  if (address === null || typeof address === "string") throw new Error("Smoke HTTP server did not bind a TCP address")
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error === undefined ? resolve() : reject(error)))
        server.closeAllConnections()
      }),
  }
}

async function expectRejected(run: () => Promise<unknown>) {
  try {
    await run()
  } catch {
    return
  }
  throw new Error("Expected operation to be rejected")
}
