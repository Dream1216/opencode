import { createHash, randomUUID } from "node:crypto"
import {
  observeWorkerQueueRecovery,
  recordWorkerQueueRecoveryAttempt,
  type WorkerQueueRecoveryOutcome,
  type WorkerQueueRecoveryTelemetryLabels,
} from "../database/postgres/worker-queue-telemetry"
import {
  makeMemoryShadowCanaryBreakerStore,
  makePostgresShadowCanaryBreakerStore,
  type ShadowCanaryBreakerPolicy,
  type ShadowCanaryBreakerState,
  type ShadowCanaryBreakerStore,
} from "./runner/shadow-canary-breaker-store"

type Environment = Readonly<Record<string, string | undefined>>

export type WorkerRecoveryPartition = {
  readonly tenantID: string
  readonly workspaceDirectory: string
}

export type WorkerRecoveryGovernanceConfig = {
  readonly sampleRate: number
  readonly metricsScope: string
  readonly breakerScope: string
  readonly breakerBackend: "memory" | "postgres"
  readonly databaseURL?: string
  readonly databaseMax: number
  readonly policy: ShadowCanaryBreakerPolicy
  readonly invalidReason?: string
}

export type WorkerRecoveryAdmission = {
  readonly allowed: boolean
  readonly reason: "admitted" | "sampled_out" | "breaker_open" | "config_error" | "store_error"
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

export function workerRecoveryGovernanceConfig(
  env: Environment = process.env,
): WorkerRecoveryGovernanceConfig {
  const issues: string[] = []
  const sampleRate = number(
    env,
    "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_SAMPLE_RATE",
    1,
    0,
    1,
    issues,
  )
  const windowSize = integer(
    env,
    "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_WINDOW_SIZE",
    20,
    1,
    10_000,
    issues,
  )
  const minimumSamples = integer(
    env,
    "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_MIN_SAMPLES",
    5,
    1,
    windowSize,
    issues,
  )
  const backendValue =
    env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_BACKEND?.trim().toLowerCase() || "memory"
  const breakerBackend = backendValue === "postgres" ? "postgres" : "memory"
  if (backendValue !== "memory" && backendValue !== "postgres") {
    issues.push("OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_BACKEND must be memory or postgres")
  }
  const databaseURL =
    env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_DATABASE_URL?.trim() ||
    env.OPENCODE_DATABASE_URL?.trim()
  if (breakerBackend === "postgres" && !databaseURL) {
    issues.push("PostgreSQL worker recovery breaker requires OPENCODE_DATABASE_URL")
  }
  return {
    sampleRate,
    metricsScope:
      env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_METRICS_SCOPE?.trim() || "p7.7.4-worker-recovery",
    breakerScope:
      env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_SCOPE?.trim() || "worker-recovery",
    breakerBackend,
    databaseURL,
    databaseMax: integer(
      env,
      "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_DATABASE_MAX",
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
        "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_ERROR_RATE",
        0.2,
        0,
        1,
        issues,
      ),
      structuralMismatchRateThreshold: 1,
      slowRateThreshold: 1,
      latencyRatioThreshold: Number.MAX_SAFE_INTEGER,
      cooldownMs: integer(
        env,
        "OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_COOLDOWN_MS",
        60_000,
        1_000,
        86_400_000,
        issues,
      ),
    },
    invalidReason: issues.length === 0 ? undefined : issues.join("; "),
  }
}

export function workerRecoveryPartitionID(partition: WorkerRecoveryPartition) {
  return createHash("sha256")
    .update(`${partition.tenantID}\u0000${partition.workspaceDirectory}`)
    .digest("hex")
    .slice(0, 16)
}

export function workerRecoveryPartitionSelected(partition: WorkerRecoveryPartition, sampleRate: number) {
  if (sampleRate <= 0) return false
  if (sampleRate >= 1) return true
  const value = createHash("sha256")
    .update(`${partition.tenantID}\u0000${partition.workspaceDirectory}`)
    .digest()
    .readUInt32BE(0)
  return value / 0x1_0000_0000 < sampleRate
}

export async function makeWorkerRecoveryGovernance(
  config: WorkerRecoveryGovernanceConfig,
  input: {
    readonly partition: WorkerRecoveryPartition
    readonly instanceID: string
    readonly teamID?: string
    readonly store?: ShadowCanaryBreakerStore
  },
) {
  const selected = workerRecoveryPartitionSelected(input.partition, config.sampleRate)
  const partitionID = workerRecoveryPartitionID(input.partition)
  const labels: WorkerQueueRecoveryTelemetryLabels = {
    tenantID: input.partition.tenantID,
    ...(input.teamID === undefined ? {} : { teamID: input.teamID }),
    scope: config.metricsScope,
    workspaceID: partitionID,
    instanceID: input.instanceID,
  }
  const owned = input.store === undefined
  const store =
    input.store ??
    (config.breakerBackend === "postgres"
      ? await makePostgresShadowCanaryBreakerStore({
          url: config.databaseURL!,
          scope: `${config.breakerScope}:${partitionID}`,
          policy: config.policy,
          max: config.databaseMax,
        })
      : makeMemoryShadowCanaryBreakerStore(config.policy))
  let breaker = emptyBreaker()

  const observe = () =>
    observeWorkerQueueRecovery({
      ...labels,
      selected,
      breaker: {
        open: breaker.open,
        revision: breaker.revision,
        sampleCount: breaker.sampleCount,
      },
    })
  const reject = (
    reason: Exclude<WorkerRecoveryAdmission["reason"], "admitted">,
  ): WorkerRecoveryAdmission => {
    recordWorkerQueueRecoveryAttempt({ ...labels, outcome: reason })
    observe()
    return { allowed: false, reason, breaker }
  }
  observe()

  return {
    async admit(): Promise<WorkerRecoveryAdmission> {
      if (config.invalidReason) return reject("config_error")
      if (!selected) return reject("sampled_out")
      try {
        breaker = await store.read()
      } catch {
        return reject("store_error")
      }
      if (breaker.open) return reject("breaker_open")
      observe()
      return { allowed: true, reason: "admitted", breaker }
    },
    async record(outcome: Extract<WorkerQueueRecoveryOutcome, "idle" | "completed" | "failed">) {
      recordWorkerQueueRecoveryAttempt({ ...labels, outcome })
      if (outcome === "idle") {
        observe()
        return breaker
      }
      try {
        breaker = await store.record({
          requestDigest: createHash("sha256")
            .update(`${partitionID}:${input.instanceID}:${Date.now()}:${randomUUID()}`)
            .digest("hex"),
          comparedAt: Date.now(),
          candidateFailed: outcome === "failed",
          statusMatch: true,
          toolPlanDigestMatch: true,
          latencyRatio: 0,
        })
      } catch {
        recordWorkerQueueRecoveryAttempt({ ...labels, outcome: "store_error" })
      }
      observe()
      return breaker
    },
    snapshot() {
      return { selected, partitionID, breaker: { ...breaker } }
    },
    async close() {
      if (owned) await store.close()
    },
  }
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
