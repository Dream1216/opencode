import { Layer } from "effect"
import { OtlpLogger } from "effect/unstable/observability"
import { Flag } from "../flag/flag"
import { InstallationChannel, InstallationVersion } from "../installation/version"
import { runID } from "./shared"

const endpoint = Flag.OTEL_EXPORTER_OTLP_ENDPOINT

const headers = Flag.OTEL_EXPORTER_OTLP_HEADERS
  ? Flag.OTEL_EXPORTER_OTLP_HEADERS.split(",").reduce(
      (acc, entry) => {
        const [key, ...value] = entry.split("=")
        acc[key] = value.join("=")
        return acc
      },
      {} as Record<string, string>,
    )
  : undefined

function resourceAttributes() {
  const value = process.env.OTEL_RESOURCE_ATTRIBUTES
  if (!value) return {}
  try {
    return Object.fromEntries(
      value.split(",").map((entry) => {
        const index = entry.indexOf("=")
        if (index < 1) throw new Error("Invalid OTEL_RESOURCE_ATTRIBUTES entry")
        return [decodeURIComponent(entry.slice(0, index)), decodeURIComponent(entry.slice(index + 1))]
      }),
    )
  } catch {
    return {}
  }
}

export function resource(): { serviceName: string; serviceVersion: string; attributes: Record<string, string> } {
  return {
    serviceName: "opencode",
    serviceVersion: InstallationVersion,
    attributes: {
      ...resourceAttributes(),
      "deployment.environment.name": InstallationChannel,
      "opencode.client": Flag.OPENCODE_CLIENT,
      "opencode.run": runID,
      "service.instance.id": runID,
    },
  }
}

export function loggers() {
  if (!endpoint) return []
  return [OtlpLogger.make({ url: signalUrl(endpoint, "logs"), resource: resource(), headers })]
}

export async function makeMetricReader(
  input: {
    readonly endpoint?: string
    readonly headers?: Record<string, string>
    readonly exportIntervalMillis?: number
  } = {},
) {
  const target = input.endpoint ?? endpoint
  if (!target) return undefined
  const MetricsOTLP = await import("@opentelemetry/exporter-metrics-otlp-http")
  const MetricsSdk = await import("@opentelemetry/sdk-metrics")
  return new MetricsSdk.PeriodicExportingMetricReader({
    exporter: new MetricsOTLP.OTLPMetricExporter({
      url: signalUrl(target, "metrics"),
      headers: input.headers ?? headers,
    }),
    exportIntervalMillis: Math.max(
      1_000,
      input.exportIntervalMillis ?? Number(process.env.OTEL_METRIC_EXPORT_INTERVAL ?? 60_000),
    ),
  })
}

export async function tracingLayer() {
  if (!endpoint) return Layer.empty
  const NodeSdk = await import("@effect/opentelemetry/NodeSdk")
  const OTLP = await import("@opentelemetry/exporter-trace-otlp-http")
  const SdkBase = await import("@opentelemetry/sdk-trace-base")
  const { AsyncLocalStorageContextManager } = await import("@opentelemetry/context-async-hooks")
  const { context } = await import("@opentelemetry/api")
  const metricReader = await makeMetricReader()
  if (metricReader === undefined) return Layer.empty

  // The Effect Node SDK does not register a global context manager, but the AI SDK uses it to parent spans.
  const manager = new AsyncLocalStorageContextManager()
  manager.enable()
  context.setGlobalContextManager(manager)

  return NodeSdk.layer(() => ({
    resource: resource(),
    spanProcessor: new SdkBase.BatchSpanProcessor(
      new OTLP.OTLPTraceExporter({
        url: signalUrl(endpoint, "traces"),
        headers,
      }),
    ),
    metricReader,
  }))
}

function signalUrl(base: string, signal: "logs" | "metrics" | "traces") {
  return `${base.replace(/\/+$/, "")}/v1/${signal}`
}

export * as Otlp from "./otlp"
