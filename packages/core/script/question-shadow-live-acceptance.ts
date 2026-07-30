import { randomUUID } from "node:crypto"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { createServer } from "node:net"
import { spawn, spawnSync, type ChildProcess } from "node:child_process"
import { MeterProvider } from "@opentelemetry/sdk-metrics"
import {
  makeQuestionShadowGovernance,
  questionShadowGovernanceConfig,
} from "../src/question-shadow-governance"
import {
  registerQuestionShadowTelemetry,
  resetQuestionShadowTelemetry,
} from "../src/observability/question-shadow-telemetry"
import { makeMetricReader } from "../src/observability/otlp"
import { makePostgresShadowCanaryBreakerStore } from "../src/session/runner/shadow-canary-breaker-store"

const url =
  process.env.OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL?.trim() ||
  process.env.OPENCODE_DATABASE_URL?.trim()
const scope = process.env.OPENCODE_QUESTION_SHADOW_BREAKER_SCOPE?.trim()
const policy = {
  windowSize: 16,
  minimumSamples: 8,
  failureRateThreshold: 0.25,
  structuralMismatchRateThreshold: 0.25,
  slowRateThreshold: 1,
  latencyRatioThreshold: Number.MAX_SAFE_INTEGER,
  cooldownMs: 60_000,
}

if (process.env.OPENCODE_QUESTION_SHADOW_LIVE_CHILD === "1") {
  if (!url || !scope) throw new Error("Question shadow live child requires database URL and scope")
  const input = JSON.parse(required("OPENCODE_QUESTION_SHADOW_LIVE_CHILD_DIFF")) as {
    readonly digest: string
    readonly failed: boolean
    readonly matched: boolean
  }
  const store = await makePostgresShadowCanaryBreakerStore({ url, scope, policy })
  try {
    await store.record({
      requestDigest: input.digest,
      candidateFailed: input.failed,
      statusMatch: input.matched,
      toolPlanDigestMatch: true,
      latencyRatio: 0,
      comparedAt: Date.now(),
    })
  } finally {
    await store.close()
  }
  process.exit(0)
}

if (process.env.OPENCODE_QUESTION_SHADOW_LIVE_ACCEPTANCE !== "1") {
  console.log(
    JSON.stringify(
      {
        status: "skipped",
        reason: "set OPENCODE_QUESTION_SHADOW_LIVE_ACCEPTANCE=1",
      },
      undefined,
      2,
    ),
  )
  process.exit(0)
}

if (!url) throw new Error("OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL is required")
const otelcol = required("OPENCODE_P7_3_5_OTELCOL_BIN")
const prometheus = required("OPENCODE_P7_3_5_PROMETHEUS_BIN")
const liveScope = `${scope ? `${scope}:` : ""}p7.3.5:${randomUUID()}`
const metricsScope = `p7.3.5-${randomUUID()}`
const work = await mkdtemp(join(tmpdir(), "opencode-p7.3.5-"))
const processes: RunningProcess[] = []

try {
  await resetPostgres(url, liveScope)
  await runChildren(url, liveScope)
  const persisted = await verifyPersistedBreaker(url, liveScope)

  const otlpPort = await freePort()
  const exporterPort = await freePort()
  const prometheusPort = await freePort()
  const collectorConfig = join(work, "otelcol.yaml")
  const prometheusConfig = join(work, "prometheus.yaml")
  await writeFile(
    collectorConfig,
    [
      "receivers:",
      "  otlp:",
      "    protocols:",
      "      http:",
      `        endpoint: 127.0.0.1:${otlpPort}`,
      "exporters:",
      "  prometheus:",
      `    endpoint: 127.0.0.1:${exporterPort}`,
      "service:",
      "  telemetry:",
      "    metrics:",
      "      level: none",
      "  pipelines:",
      "    metrics:",
      "      receivers: [otlp]",
      "      exporters: [prometheus]",
      "",
    ].join("\n"),
  )
  await writeFile(
    prometheusConfig,
    [
      "global:",
      "  scrape_interval: 1s",
      "  evaluation_interval: 1s",
      "scrape_configs:",
      '  - job_name: "opencode-question-shadow"',
      "    static_configs:",
      `      - targets: ["127.0.0.1:${exporterPort}"]`,
      "",
    ].join("\n"),
  )

  const collector = startProcess(
    "otelcol",
    otelcol,
    ["--config", collectorConfig],
    processes,
  )
  await waitForHTTP(`http://127.0.0.1:${exporterPort}/metrics`, collector)

  const prom = startProcess(
    "prometheus",
    prometheus,
    [
      `--config.file=${prometheusConfig}`,
      `--storage.tsdb.path=${join(work, "prometheus-data")}`,
      `--web.listen-address=127.0.0.1:${prometheusPort}`,
      "--log.level=warn",
    ],
    processes,
  )
  await waitForHTTP(`http://127.0.0.1:${prometheusPort}/-/ready`, prom)

  const config = questionShadowGovernanceConfig({
    OPENCODE_QUESTION_SHADOW_SAMPLE_RATE: "1",
    OPENCODE_QUESTION_SHADOW_METRICS_SCOPE: metricsScope,
    OPENCODE_QUESTION_SHADOW_BREAKER_BACKEND: "postgres",
    OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL: url,
    OPENCODE_QUESTION_SHADOW_BREAKER_SCOPE: liveScope,
    OPENCODE_QUESTION_SHADOW_BREAKER_WINDOW_SIZE: String(policy.windowSize),
    OPENCODE_QUESTION_SHADOW_BREAKER_MIN_SAMPLES: String(policy.minimumSamples),
    OPENCODE_QUESTION_SHADOW_BREAKER_ERROR_RATE: String(policy.failureRateThreshold),
    OPENCODE_QUESTION_SHADOW_BREAKER_DIVERGENCE_RATE: String(
      policy.structuralMismatchRateThreshold,
    ),
  })
  if (config.invalidReason) throw new Error(`Question shadow live config is invalid: ${config.invalidReason}`)
  const telemetryStore = await makePostgresShadowCanaryBreakerStore({
    url,
    scope: liveScope,
    policy: config.policy,
  })
  const governance = makeQuestionShadowGovernance(config, telemetryStore)
  const admission = await governance.admit("que_p735_blocked", "ask")
  if (admission.allowed || admission.reason !== "breaker_open") {
    throw new Error(`shared breaker did not block governed shadow: ${JSON.stringify(admission)}`)
  }

  const reader = await makeMetricReader({
    endpoint: `http://127.0.0.1:${otlpPort}`,
    exportIntervalMillis: 60_000,
  })
  if (!reader) throw new Error("Question shadow live OTLP metric reader was not created")
  const provider = new MeterProvider({ readers: [reader] })
  registerQuestionShadowTelemetry(provider.getMeter("opencode.question-shadow-live", "1.0.0"))
  try {
    await provider.forceFlush()
  } finally {
    await provider.shutdown()
  }

  const collectorMetricsURL = `http://127.0.0.1:${exporterPort}/metrics`
  const collectorMetrics = await waitForText(
    collectorMetricsURL,
    (body) =>
      body.includes("opencode_question_shadow_breaker_open") &&
      body.includes(metricsScope),
  )
  const prometheusResult = await waitForPrometheus(
    `http://127.0.0.1:${prometheusPort}`,
    metricsScope,
  )

  console.log(
    JSON.stringify(
      {
        status: "passed",
        checks: [
          "postgres-eight-process-contention",
          "postgres-breaker-open-shared",
          "postgres-breaker-persisted-across-restart",
          "governed-shadow-blocked-by-shared-breaker",
          "real-otelcol-otlp-http-received",
          "otelcol-prometheus-exporter-published",
          "real-prometheus-target-up",
          "real-prometheus-breaker-query-equals-one",
        ],
        postgres: {
          processes: 8,
          sampleCount: persisted.sampleCount,
          failureRate: persisted.failureRate,
          divergenceRate: persisted.structuralMismatchRate,
          revision: persisted.revision,
        },
        collector: {
          version: version(otelcol),
          metricObserved: collectorMetrics.includes("opencode_question_shadow_breaker_open"),
        },
        prometheus: {
          version: version(prometheus),
          value: prometheusResult.value,
          labels: prometheusResult.labels,
        },
      },
      undefined,
      2,
    ),
  )
} finally {
  for (const process of processes.reverse()) await stopProcess(process)
  await cleanupPostgres(url, liveScope)
  resetQuestionShadowTelemetry(metricsScope)
  await rm(work, { recursive: true, force: true })
}

async function runChildren(databaseURL: string, breakerScope: string) {
  const diffs = [
    { digest: "p735-01", failed: true, matched: true },
    { digest: "p735-02", failed: true, matched: true },
    { digest: "p735-03", failed: true, matched: true },
    { digest: "p735-04", failed: false, matched: false },
    { digest: "p735-05", failed: false, matched: false },
    { digest: "p735-06", failed: false, matched: false },
    { digest: "p735-07", failed: false, matched: true },
    { digest: "p735-08", failed: false, matched: true },
  ]
  await Promise.all(
    diffs.map(
      (diff) =>
        new Promise<void>((resolve, reject) => {
          const child = spawn(process.execPath, [import.meta.path], {
            env: {
              ...process.env,
              OPENCODE_QUESTION_SHADOW_BREAKER_DATABASE_URL: databaseURL,
              OPENCODE_QUESTION_SHADOW_BREAKER_SCOPE: breakerScope,
              OPENCODE_QUESTION_SHADOW_LIVE_CHILD: "1",
              OPENCODE_QUESTION_SHADOW_LIVE_CHILD_DIFF: JSON.stringify(diff),
            },
            stdio: ["ignore", "ignore", "pipe"],
          })
          const stderr: Buffer[] = []
          child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk))
          child.once("error", reject)
          child.once("close", (code) => {
            if (code === 0) return resolve()
            reject(
              new Error(
                `Question shadow child exited ${code}: ${Buffer.concat(stderr).toString("utf8").slice(0, 2_048)}`,
              ),
            )
          })
        }),
    ),
  )
}

async function verifyPersistedBreaker(databaseURL: string, breakerScope: string) {
  const first = await makePostgresShadowCanaryBreakerStore({
    url: databaseURL,
    scope: breakerScope,
    policy,
  })
  const state = await first.read()
  await first.close()
  if (
    !state.open ||
    state.sampleCount !== 8 ||
    state.failureRate !== 3 / 8 ||
    state.structuralMismatchRate !== 3 / 8
  ) {
    throw new Error(`Question shadow shared breaker did not open: ${JSON.stringify(state)}`)
  }
  const restarted = await makePostgresShadowCanaryBreakerStore({
    url: databaseURL,
    scope: breakerScope,
    policy,
  })
  try {
    const next = await restarted.read()
    if (!next.open || next.sampleCount !== state.sampleCount || next.revision !== state.revision) {
      throw new Error(`Question shadow breaker did not survive restart: ${JSON.stringify(next)}`)
    }
    return next
  } finally {
    await restarted.close()
  }
}

async function resetPostgres(databaseURL: string, breakerScope: string) {
  const store = await makePostgresShadowCanaryBreakerStore({
    url: databaseURL,
    scope: breakerScope,
    policy,
  })
  try {
    await store.destroy?.()
  } finally {
    await store.close()
  }
}

async function cleanupPostgres(databaseURL: string | undefined, breakerScope: string) {
  if (!databaseURL) return
  try {
    await resetPostgres(databaseURL, breakerScope)
  } catch {
    // Preserve the original acceptance failure.
  }
}

type RunningProcess = {
  readonly name: string
  readonly child: ChildProcess
  readonly logs: string[]
}

function startProcess(name: string, binary: string, args: string[], processes: RunningProcess[]) {
  const logs: string[] = []
  const child = spawn(binary, args, { stdio: ["ignore", "pipe", "pipe"] })
  const capture = (chunk: Buffer) => {
    logs.push(chunk.toString("utf8"))
    if (logs.length > 100) logs.shift()
  }
  child.stdout?.on("data", capture)
  child.stderr?.on("data", capture)
  const running = { name, child, logs }
  processes.push(running)
  return running
}

async function stopProcess(process: RunningProcess) {
  if (process.child.exitCode !== null) return
  process.child.kill("SIGTERM")
  await Promise.race([
    new Promise<void>((resolve) => process.child.once("close", () => resolve())),
    Bun.sleep(5_000).then(() => {
      process.child.kill("SIGKILL")
    }),
  ])
}

async function waitForHTTP(url: string, process: RunningProcess) {
  await waitFor(async () => {
    if (process.child.exitCode !== null) {
      throw new Error(`${process.name} exited ${process.child.exitCode}: ${process.logs.join("").slice(-4_096)}`)
    }
    try {
      const response = await fetch(url)
      return response.ok
    } catch {
      return false
    }
  })
}

async function waitForText(url: string, accept: (body: string) => boolean) {
  let latest = ""
  await waitFor(async () => {
    try {
      const response = await fetch(url)
      latest = await response.text()
      return response.ok && accept(latest)
    } catch {
      return false
    }
  })
  return latest
}

async function waitForPrometheus(baseURL: string, metricsScope: string) {
  let latest: unknown
  await waitFor(async () => {
    try {
      const target = await fetch(`${baseURL}/api/v1/query?query=up%7Bjob%3D%22opencode-question-shadow%22%7D`)
      const targetBody = (await target.json()) as PrometheusResponse
      if (targetBody.data?.result?.[0]?.value?.[1] !== "1") return false
      const response = await fetch(
        `${baseURL}/api/v1/query?query=opencode_question_shadow_breaker_open`,
      )
      latest = await response.json()
      const body = latest as PrometheusResponse
      return (
        body.status === "success" &&
        body.data?.result?.some(
          (item) =>
            item.value?.[1] === "1" &&
            (item.metric["shadow.scope"] === metricsScope ||
              item.metric.shadow_scope === metricsScope),
        ) === true
      )
    } catch {
      return false
    }
  })
  const body = latest as PrometheusResponse
  const result = body.data?.result?.find(
    (item) =>
      item.value?.[1] === "1" &&
      (item.metric["shadow.scope"] === metricsScope || item.metric.shadow_scope === metricsScope),
  )
  if (!result?.value) throw new Error(`Prometheus breaker result missing: ${JSON.stringify(latest)}`)
  return { value: result.value[1], labels: result.metric }
}

type PrometheusResponse = {
  readonly status?: string
  readonly data?: {
    readonly result?: readonly {
      readonly metric: Readonly<Record<string, string>>
      readonly value?: readonly [number, string]
    }[]
  }
}

async function waitFor(check: () => Promise<boolean>, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return
    await Bun.sleep(250)
  }
  throw new Error(`Timed out after ${timeoutMs}ms`)
}

async function freePort() {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject)
      resolve()
    })
  })
  const address = server.address()
  if (!address || typeof address === "string") throw new Error("Could not allocate a TCP port")
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  )
  return address.port
}

function version(binary: string) {
  const result = spawnSync(binary, ["--version"], { encoding: "utf8" })
  return `${result.stdout || result.stderr}`.trim().split("\n")[0]
}

function required(name: string) {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}
