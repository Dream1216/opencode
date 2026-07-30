import { metrics, type Meter } from "@opentelemetry/api"
import type { WorkerQueueReadiness } from "./worker-queue-operations"

export type WorkerQueueTelemetryLabels = {
  readonly tenantID: string
  readonly teamID?: string
}

export type WorkerQueueRecoveryTelemetryLabels = WorkerQueueTelemetryLabels & {
  readonly scope: string
  readonly workspaceID: string
  readonly instanceID: string
}

export type WorkerQueueRecoveryOutcome =
  | "idle"
  | "completed"
  | "failed"
  | "sampled_out"
  | "breaker_open"
  | "config_error"
  | "store_error"

export type WorkerQueueRecoveryOwnershipOutcome =
  | "acquired"
  | "renewed"
  | "takeover"
  | "contended"
  | "released"
  | "lost"
  | "store_error"

type Snapshot = {
  readonly readiness: WorkerQueueReadiness
  readonly labels: WorkerQueueTelemetryLabels
}

type RecoverySnapshot = {
  readonly labels: WorkerQueueRecoveryTelemetryLabels
  readonly selected: boolean
  readonly breaker: {
    readonly open: boolean
    readonly revision: number
    readonly sampleCount: number
  }
  readonly attempts: Record<WorkerQueueRecoveryOutcome, number>
}

type RecoveryOwnershipSnapshot = {
  readonly labels: WorkerQueueRecoveryTelemetryLabels
  readonly held: boolean
  readonly epoch: number
  readonly leaseExpiresAt: number
  readonly attempts: Record<WorkerQueueRecoveryOwnershipOutcome, number>
}

const meter = metrics.getMeter("opencode.worker-queue", "1.0.0")
const snapshots = new Map<string, Snapshot>()
const recoverySnapshots = new Map<string, RecoverySnapshot>()
const recoveryOwnershipSnapshots = new Map<string, RecoveryOwnershipSnapshot>()
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
  const recoveryAttempts = target.createCounter("opencode.worker_queue.recovery_attempts", {
    description: "Workspace recovery attempts by outcome",
  })
  const recoverySelected = target.createObservableGauge("opencode.worker_queue.recovery_selected", {
    description: "Whether a tenant/workspace recovery partition is selected for canary execution",
  })
  const recoveryBreakerOpen = target.createObservableGauge("opencode.worker_queue.recovery_breaker_open", {
    description: "Whether the shared recovery breaker is open",
  })
  const recoveryBreakerRevision = target.createObservableGauge("opencode.worker_queue.recovery_breaker_revision", {
    description: "Shared recovery breaker revision observed by this instance",
  })
  const recoveryBreakerSamples = target.createObservableGauge("opencode.worker_queue.recovery_breaker_samples", {
    description: "Samples in the shared recovery breaker window",
  })
  const recoveryOwnershipAttempts = target.createCounter("opencode.worker_queue.recovery_ownership_attempts", {
    description: "Workspace recovery ownership operations by outcome",
  })
  const recoveryOwnerHeld = target.createObservableGauge("opencode.worker_queue.recovery_owner_held", {
    description: "Whether this instance currently holds the workspace recovery ownership lease",
  })
  const recoveryOwnershipEpoch = target.createObservableGauge("opencode.worker_queue.recovery_ownership_epoch", {
    description: "Workspace recovery ownership epoch observed by this instance",
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
  recoverySelected.addCallback((result) => {
    for (const snapshot of recoverySnapshots.values()) {
      result.observe(snapshot.selected ? 1 : 0, recoveryLabels(snapshot.labels))
    }
  })
  recoveryBreakerOpen.addCallback((result) => {
    for (const snapshot of recoverySnapshots.values()) {
      result.observe(snapshot.breaker.open ? 1 : 0, recoveryLabels(snapshot.labels))
    }
  })
  recoveryBreakerRevision.addCallback((result) => {
    for (const snapshot of recoverySnapshots.values()) {
      result.observe(snapshot.breaker.revision, recoveryLabels(snapshot.labels))
    }
  })
  recoveryBreakerSamples.addCallback((result) => {
    for (const snapshot of recoverySnapshots.values()) {
      result.observe(snapshot.breaker.sampleCount, recoveryLabels(snapshot.labels))
    }
  })
  recoveryOwnerHeld.addCallback((result) => {
    for (const snapshot of recoveryOwnershipSnapshots.values()) {
      result.observe(snapshot.held ? 1 : 0, recoveryLabels(snapshot.labels))
    }
  })
  recoveryOwnershipEpoch.addCallback((result) => {
    for (const snapshot of recoveryOwnershipSnapshots.values()) {
      result.observe(snapshot.epoch, recoveryLabels(snapshot.labels))
    }
  })
  return { operatorActions, recoveryAttempts, recoveryOwnershipAttempts }
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

export function observeWorkerQueueRecovery(
  input: WorkerQueueRecoveryTelemetryLabels & {
    readonly selected: boolean
    readonly breaker: RecoverySnapshot["breaker"]
  },
) {
  const current = recoverySnapshots.get(recoveryKey(input))
  recoverySnapshots.set(recoveryKey(input), {
    labels: input,
    selected: input.selected,
    breaker: input.breaker,
    attempts: current?.attempts ?? emptyRecoveryAttempts(),
  })
}

export function recordWorkerQueueRecoveryAttempt(
  input: WorkerQueueRecoveryTelemetryLabels & {
    readonly outcome: WorkerQueueRecoveryOutcome
  },
) {
  const current = recoverySnapshots.get(recoveryKey(input)) ?? {
    labels: input,
    selected: true,
    breaker: { open: false, revision: 0, sampleCount: 0 },
    attempts: emptyRecoveryAttempts(),
  }
  current.attempts[input.outcome]++
  recoverySnapshots.set(recoveryKey(input), current)
  defaultInstruments.recoveryAttempts.add(1, {
    ...recoveryLabels(input),
    outcome: input.outcome,
  })
}

export function observeWorkerQueueRecoveryOwnership(
  input: WorkerQueueRecoveryTelemetryLabels & {
    readonly held: boolean
    readonly epoch: number
    readonly leaseExpiresAt: number
  },
) {
  const current = recoveryOwnershipSnapshots.get(recoveryKey(input))
  recoveryOwnershipSnapshots.set(recoveryKey(input), {
    labels: input,
    held: input.held,
    epoch: input.epoch,
    leaseExpiresAt: input.leaseExpiresAt,
    attempts: current?.attempts ?? emptyRecoveryOwnershipAttempts(),
  })
}

export function recordWorkerQueueRecoveryOwnershipAttempt(
  input: WorkerQueueRecoveryTelemetryLabels & {
    readonly outcome: WorkerQueueRecoveryOwnershipOutcome
  },
) {
  const current = recoveryOwnershipSnapshots.get(recoveryKey(input)) ?? {
    labels: input,
    held: false,
    epoch: 0,
    leaseExpiresAt: 0,
    attempts: emptyRecoveryOwnershipAttempts(),
  }
  current.attempts[input.outcome]++
  recoveryOwnershipSnapshots.set(recoveryKey(input), current)
  defaultInstruments.recoveryOwnershipAttempts.add(1, {
    ...recoveryLabels(input),
    outcome: input.outcome,
  })
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
    renderWorkerQueueRecoveryPrometheus(input),
    "",
  ].join("\n")
}

export function renderWorkerQueueRecoveryPrometheus(input: WorkerQueueTelemetryLabels) {
  const selected = [...recoverySnapshots.values()].filter(
    (snapshot) => snapshot.labels.tenantID === input.tenantID && snapshot.labels.teamID === input.teamID,
  )
  const lines = [
    "# HELP opencode_worker_queue_recovery_attempts_total Workspace recovery attempts by outcome.",
    "# TYPE opencode_worker_queue_recovery_attempts_total counter",
  ]
  for (const snapshot of selected) {
    for (const [outcome, value] of Object.entries(snapshot.attempts)) {
      lines.push(
        `opencode_worker_queue_recovery_attempts_total${recoveryPrometheusLabels(snapshot.labels, outcome)} ${value}`,
      )
    }
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_selected Whether the recovery partition is selected.",
    "# TYPE opencode_worker_queue_recovery_selected gauge",
  )
  for (const snapshot of selected) {
    lines.push(
      `opencode_worker_queue_recovery_selected${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.selected ? 1 : 0}`,
    )
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_breaker_open Whether the shared recovery breaker is open.",
    "# TYPE opencode_worker_queue_recovery_breaker_open gauge",
  )
  for (const snapshot of selected) {
    lines.push(
      `opencode_worker_queue_recovery_breaker_open${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.breaker.open ? 1 : 0}`,
    )
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_breaker_revision Shared recovery breaker revision.",
    "# TYPE opencode_worker_queue_recovery_breaker_revision gauge",
  )
  for (const snapshot of selected) {
    lines.push(
      `opencode_worker_queue_recovery_breaker_revision${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.breaker.revision}`,
    )
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_breaker_samples Samples in the shared breaker window.",
    "# TYPE opencode_worker_queue_recovery_breaker_samples gauge",
  )
  for (const snapshot of selected) {
    lines.push(
      `opencode_worker_queue_recovery_breaker_samples${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.breaker.sampleCount}`,
    )
  }
  const ownership = [...recoveryOwnershipSnapshots.values()].filter(
    (snapshot) => snapshot.labels.tenantID === input.tenantID && snapshot.labels.teamID === input.teamID,
  )
  lines.push(
    "# HELP opencode_worker_queue_recovery_ownership_attempts_total Workspace ownership operations by outcome.",
    "# TYPE opencode_worker_queue_recovery_ownership_attempts_total counter",
  )
  for (const snapshot of ownership) {
    for (const [outcome, value] of Object.entries(snapshot.attempts)) {
      lines.push(
        `opencode_worker_queue_recovery_ownership_attempts_total${recoveryPrometheusLabels(snapshot.labels, outcome)} ${value}`,
      )
    }
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_owner_held Whether this instance holds the workspace owner lease.",
    "# TYPE opencode_worker_queue_recovery_owner_held gauge",
  )
  for (const snapshot of ownership) {
    lines.push(
      `opencode_worker_queue_recovery_owner_held${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.held ? 1 : 0}`,
    )
  }
  lines.push(
    "# HELP opencode_worker_queue_recovery_ownership_epoch Workspace ownership fencing epoch.",
    "# TYPE opencode_worker_queue_recovery_ownership_epoch gauge",
  )
  for (const snapshot of ownership) {
    lines.push(
      `opencode_worker_queue_recovery_ownership_epoch${recoveryPrometheusLabels(snapshot.labels)} ${snapshot.epoch}`,
    )
  }
  return lines.join("\n")
}

export function workerQueueTelemetrySnapshot(input: WorkerQueueTelemetryLabels) {
  return snapshots.get(key(input))
}

export function workerQueueRecoveryTelemetrySnapshot(input: WorkerQueueRecoveryTelemetryLabels) {
  return recoverySnapshots.get(recoveryKey(input))
}

export function resetWorkerQueueRecoveryTelemetry() {
  recoverySnapshots.clear()
  recoveryOwnershipSnapshots.clear()
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

function recoveryKey(input: WorkerQueueRecoveryTelemetryLabels) {
  return [
    input.tenantID,
    input.teamID ?? "",
    input.scope,
    input.workspaceID,
    input.instanceID,
  ].join("\u0000")
}

function recoveryLabels(input: WorkerQueueRecoveryTelemetryLabels) {
  return {
    ...labels(input),
    "recovery.scope": input.scope,
    "workspace.id": input.workspaceID,
    "service.instance.id": input.instanceID,
  }
}

function recoveryPrometheusLabels(input: WorkerQueueRecoveryTelemetryLabels, outcome?: string) {
  const values = [
    `tenant_id="${escapeLabel(input.tenantID)}"`,
    `scope="${escapeLabel(input.scope)}"`,
    `workspace_id="${escapeLabel(input.workspaceID)}"`,
    `instance_id="${escapeLabel(input.instanceID)}"`,
  ]
  if (input.teamID !== undefined) values.push(`team_id="${escapeLabel(input.teamID)}"`)
  if (outcome !== undefined) values.push(`outcome="${escapeLabel(outcome)}"`)
  return `{${values.join(",")}}`
}

function emptyRecoveryAttempts(): Record<WorkerQueueRecoveryOutcome, number> {
  return {
    idle: 0,
    completed: 0,
    failed: 0,
    sampled_out: 0,
    breaker_open: 0,
    config_error: 0,
    store_error: 0,
  }
}

function emptyRecoveryOwnershipAttempts(): Record<WorkerQueueRecoveryOwnershipOutcome, number> {
  return {
    acquired: 0,
    renewed: 0,
    takeover: 0,
    contended: 0,
    released: 0,
    lost: 0,
    store_error: 0,
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
