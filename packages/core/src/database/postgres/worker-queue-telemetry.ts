import { metrics, type Meter } from "@opentelemetry/api"
import type { WorkerQueueReadiness } from "./worker-queue-operations"

export type WorkerQueueTelemetryLabels = {
  readonly tenantID: string
  readonly teamID?: string
}

type Snapshot = {
  readonly readiness: WorkerQueueReadiness
  readonly labels: WorkerQueueTelemetryLabels
}

const meter = metrics.getMeter("opencode.worker-queue", "1.0.0")
const snapshots = new Map<string, Snapshot>()
const defaultInstruments = registerWorkerQueueTelemetry(meter)

export function registerWorkerQueueTelemetry(target: Meter) {
  const jobs = target.createObservableGauge("opencode.worker_queue.jobs", {
    description: "Current durable worker jobs by status",
  })
  const expired = target.createObservableGauge("opencode.worker_queue.expired_claims", {
    description: "Running durable worker jobs with expired claims",
  })
  const pendingAge = target.createObservableGauge("opencode.worker_queue.oldest_pending_age_ms", {
    description: "Age of the oldest pending durable worker job",
    unit: "ms",
  })
  const ready = target.createObservableGauge("opencode.worker_queue.ready", {
    description: "Whether the worker queue PostgreSQL/RLS readiness probe succeeded",
  })
  const degraded = target.createObservableGauge("opencode.worker_queue.degraded", {
    description: "Whether the worker queue is operationally degraded",
  })
  const operatorActions = target.createCounter("opencode.worker_queue.operator_actions", {
    description: "Worker queue operator actions by outcome",
  })

  jobs.addCallback((result) => {
    for (const snapshot of snapshots.values()) {
      const attributes = labels(snapshot.labels)
      result.observe(snapshot.readiness.metrics.pending, { ...attributes, status: "pending" })
      result.observe(snapshot.readiness.metrics.running, { ...attributes, status: "running" })
      result.observe(snapshot.readiness.metrics.completed, { ...attributes, status: "completed" })
      result.observe(snapshot.readiness.metrics.failed, { ...attributes, status: "failed" })
      result.observe(snapshot.readiness.metrics.cancelled, { ...attributes, status: "cancelled" })
    }
  })
  expired.addCallback((result) => {
    for (const snapshot of snapshots.values()) {
      result.observe(snapshot.readiness.metrics.expiredRunning, labels(snapshot.labels))
    }
  })
  pendingAge.addCallback((result) => {
    for (const snapshot of snapshots.values()) {
      result.observe(snapshot.readiness.metrics.oldestPendingAgeMs, labels(snapshot.labels))
    }
  })
  ready.addCallback((result) => {
    for (const snapshot of snapshots.values()) result.observe(snapshot.readiness.ready ? 1 : 0, labels(snapshot.labels))
  })
  degraded.addCallback((result) => {
    for (const snapshot of snapshots.values()) {
      result.observe(snapshot.readiness.degraded ? 1 : 0, labels(snapshot.labels))
    }
  })
  return { operatorActions }
}

export function observeWorkerQueue(readiness: WorkerQueueReadiness, input: WorkerQueueTelemetryLabels) {
  snapshots.set(key(input), { readiness, labels: input })
}

export function recordWorkerQueueOperatorAction(
  input: WorkerQueueTelemetryLabels & {
    readonly action: "request" | "approve" | "execute"
    readonly outcome: "allowed" | "rejected"
  },
) {
  defaultInstruments.operatorActions.add(1, { ...labels(input), action: input.action, outcome: input.outcome })
}

export function renderWorkerQueuePrometheus(
  readiness: WorkerQueueReadiness,
  input: WorkerQueueTelemetryLabels,
) {
  const base = prometheusLabels(input)
  const status = (value: string) => `${base.slice(0, -1)},status="${escapeLabel(value)}"}`
  return [
    "# HELP opencode_worker_queue_jobs Current durable worker jobs by status.",
    "# TYPE opencode_worker_queue_jobs gauge",
    `opencode_worker_queue_jobs${status("pending")} ${readiness.metrics.pending}`,
    `opencode_worker_queue_jobs${status("running")} ${readiness.metrics.running}`,
    `opencode_worker_queue_jobs${status("completed")} ${readiness.metrics.completed}`,
    `opencode_worker_queue_jobs${status("failed")} ${readiness.metrics.failed}`,
    `opencode_worker_queue_jobs${status("cancelled")} ${readiness.metrics.cancelled}`,
    "# HELP opencode_worker_queue_expired_claims Running jobs with expired claims.",
    "# TYPE opencode_worker_queue_expired_claims gauge",
    `opencode_worker_queue_expired_claims${base} ${readiness.metrics.expiredRunning}`,
    "# HELP opencode_worker_queue_oldest_pending_age_ms Age of the oldest pending job.",
    "# TYPE opencode_worker_queue_oldest_pending_age_ms gauge",
    `opencode_worker_queue_oldest_pending_age_ms${base} ${readiness.metrics.oldestPendingAgeMs}`,
    "# HELP opencode_worker_queue_ready PostgreSQL and forced-RLS readiness.",
    "# TYPE opencode_worker_queue_ready gauge",
    `opencode_worker_queue_ready${base} ${readiness.ready ? 1 : 0}`,
    "# HELP opencode_worker_queue_degraded Operational degradation state.",
    "# TYPE opencode_worker_queue_degraded gauge",
    `opencode_worker_queue_degraded${base} ${readiness.degraded ? 1 : 0}`,
    "",
  ].join("\n")
}

export function workerQueueTelemetrySnapshot(input: WorkerQueueTelemetryLabels) {
  return snapshots.get(key(input))
}

function key(input: WorkerQueueTelemetryLabels) {
  return `${input.tenantID}\u0000${input.teamID ?? ""}`
}

function labels(input: WorkerQueueTelemetryLabels) {
  return {
    "tenant.id": input.tenantID,
    ...(input.teamID === undefined ? {} : { "team.id": input.teamID }),
  }
}

function prometheusLabels(input: WorkerQueueTelemetryLabels) {
  const values = [`tenant_id="${escapeLabel(input.tenantID)}"`]
  if (input.teamID !== undefined) values.push(`team_id="${escapeLabel(input.teamID)}"`)
  return `{${values.join(",")}}`
}

function escapeLabel(value: string) {
  return value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll('"', '\\"')
}
