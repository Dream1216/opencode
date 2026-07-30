import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import {
  makeWorkerRecoveryGovernance,
  workerRecoveryGovernanceConfig,
  workerRecoveryPartitionID,
  type WorkerRecoveryPartition,
} from "@opencode-ai/core/session/worker-recovery-governance"
import {
  makePostgresWorkspaceRecoveryOwnershipStore,
  type WorkspaceRecoveryOwnershipLease,
} from "@opencode-ai/core/database/postgres/workspace-recovery-ownership"
import {
  observeWorkerQueueRecoveryOwnership,
  recordWorkerQueueRecoveryOwnershipAttempt,
} from "@opencode-ai/core/database/postgres/worker-queue-telemetry"
import { Context, Effect, Layer } from "effect"
import path from "node:path"
import { InstanceStore } from "@/project/instance-store"
import { SessionPrompt } from "./prompt"

export interface Interface {
  readonly enabled: boolean
  readonly ownershipEnabled: boolean
  readonly workspaces: readonly string[]
  readonly partitions: readonly WorkerRecoveryPartition[]
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionWorkerRecovery") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const settings = settingsFromEnv(process.env)
    if (settings === undefined) {
      return Service.of({ enabled: false, ownershipEnabled: false, workspaces: [], partitions: [] })
    }
    const store = yield* InstanceStore.Service
    const prompt = yield* SessionPrompt.Service
    const ownership = settings.ownership.enabled
      ? yield* Effect.acquireRelease(
          Effect.sync(() =>
            makePostgresWorkspaceRecoveryOwnershipStore({
              url: settings.ownership.databaseURL!,
              max: settings.ownership.databaseMax,
            }),
          ),
          (value) => Effect.promise(() => value.close()).pipe(Effect.catchCause(() => Effect.void)),
        )
      : undefined
    yield* Effect.forEach(
      settings.partitions,
      (partition) =>
        Effect.gen(function* () {
          const governance = yield* Effect.acquireRelease(
            Effect.tryPromise(() =>
              makeWorkerRecoveryGovernance(settings.governance, {
                partition,
                instanceID: settings.instanceID,
                teamID: settings.teamID,
              }),
            ).pipe(Effect.orDie),
            (value) => Effect.promise(() => value.close()).pipe(Effect.catchCause(() => Effect.void)),
          )
          const workspaceID = workerRecoveryPartitionID(partition)
          const ownershipLabels = {
            tenantID: partition.tenantID,
            ...(settings.teamID === undefined ? {} : { teamID: settings.teamID }),
            scope: settings.governance.metricsScope,
            workspaceID,
            instanceID: settings.instanceID,
          }
          let ownershipLease: WorkspaceRecoveryOwnershipLease | undefined
          if (ownership !== undefined) {
            observeWorkerQueueRecoveryOwnership({
              ...ownershipLabels,
              held: false,
              epoch: 0,
              leaseExpiresAt: 0,
            })
            yield* Effect.addFinalizer(() => {
              const lease = ownershipLease
              ownershipLease = undefined
              if (lease === undefined) return Effect.void
              return Effect.tryPromise(() => ownership.release(lease)).pipe(
                Effect.tap((released) =>
                  Effect.sync(() => {
                    if (released) {
                      recordWorkerQueueRecoveryOwnershipAttempt({
                        ...ownershipLabels,
                        outcome: "released",
                      })
                    }
                    observeWorkerQueueRecoveryOwnership({
                      ...ownershipLabels,
                      held: false,
                      epoch: lease.epoch,
                      leaseExpiresAt: lease.leaseExpiresAt,
                    })
                  }),
                ),
                Effect.catchCause(() => Effect.void),
                Effect.asVoid,
              )
            })
            yield* Effect.forkScoped(
              Effect.forever(
                Effect.gen(function* () {
                  yield* Effect.sleep(settings.ownership.heartbeatMs)
                  const lease = ownershipLease
                  if (lease === undefined) return
                  const result = yield* Effect.tryPromise(() =>
                    ownership.renew(lease, settings.ownership.leaseMs),
                  ).pipe(
                    Effect.map((value) => ({ ok: true as const, value })),
                    Effect.catch((error) =>
                      Effect.logError("PostgreSQL workspace ownership heartbeat failed", {
                        tenantID: partition.tenantID,
                        workspaceID,
                        instanceID: settings.instanceID,
                        error,
                      }).pipe(Effect.as({ ok: false as const })),
                    ),
                  )
                  if (!result.ok) {
                    ownershipLease = undefined
                    recordWorkerQueueRecoveryOwnershipAttempt({
                      ...ownershipLabels,
                      outcome: "store_error",
                    })
                    observeWorkerQueueRecoveryOwnership({
                      ...ownershipLabels,
                      held: false,
                      epoch: lease.epoch,
                      leaseExpiresAt: lease.leaseExpiresAt,
                    })
                    return
                  }
                  if (result.value === undefined) {
                    ownershipLease = undefined
                    recordWorkerQueueRecoveryOwnershipAttempt({
                      ...ownershipLabels,
                      outcome: "lost",
                    })
                    observeWorkerQueueRecoveryOwnership({
                      ...ownershipLabels,
                      held: false,
                      epoch: lease.epoch,
                      leaseExpiresAt: lease.leaseExpiresAt,
                    })
                    return
                  }
                  ownershipLease = result.value
                  recordWorkerQueueRecoveryOwnershipAttempt({
                    ...ownershipLabels,
                    outcome: "renewed",
                  })
                  observeWorkerQueueRecoveryOwnership({
                    ...ownershipLabels,
                    held: true,
                    epoch: result.value.epoch,
                    leaseExpiresAt: result.value.leaseExpiresAt,
                  })
                }),
              ),
            )
          }
          yield* Effect.logInfo("starting governed PostgreSQL workspace recovery consumer", {
            tenantID: partition.tenantID,
            workspaceID,
            instanceID: settings.instanceID,
            ownershipEnabled: settings.ownership.enabled,
          })
          yield* Effect.forkScoped(
            Effect.forever(
              Effect.gen(function* () {
                const admission = yield* Effect.promise(() => governance.admit())
                if (!admission.allowed) {
                  yield* Effect.sleep(settings.retryMs)
                  return
                }
                if (ownership !== undefined && ownershipLease === undefined) {
                  const result = yield* Effect.tryPromise(() =>
                    ownership.acquire({
                      tenant: {
                        tenantID: partition.tenantID,
                        ...(settings.teamID === undefined ? {} : { teamID: settings.teamID }),
                      },
                      workspaceID,
                      workspaceDirectory: partition.workspaceDirectory,
                      ownerID: settings.instanceID,
                      leaseMs: settings.ownership.leaseMs,
                    }),
                  ).pipe(
                    Effect.map((value) => ({ ok: true as const, value })),
                    Effect.catch((error) =>
                      Effect.logError("PostgreSQL workspace ownership acquisition failed", {
                        tenantID: partition.tenantID,
                        workspaceID,
                        instanceID: settings.instanceID,
                        error,
                      }).pipe(Effect.as({ ok: false as const })),
                    ),
                  )
                  if (!result.ok) {
                    recordWorkerQueueRecoveryOwnershipAttempt({
                      ...ownershipLabels,
                      outcome: "store_error",
                    })
                    yield* Effect.sleep(settings.retryMs)
                    return
                  }
                  if (!result.value.acquired) {
                    recordWorkerQueueRecoveryOwnershipAttempt({
                      ...ownershipLabels,
                      outcome: "contended",
                    })
                    observeWorkerQueueRecoveryOwnership({
                      ...ownershipLabels,
                      held: false,
                      epoch: result.value.current.epoch,
                      leaseExpiresAt: result.value.current.leaseExpiresAt,
                    })
                    yield* Effect.sleep(settings.retryMs)
                    return
                  }
                  ownershipLease = result.value.lease
                  recordWorkerQueueRecoveryOwnershipAttempt({
                    ...ownershipLabels,
                    outcome: result.value.disposition,
                  })
                  observeWorkerQueueRecoveryOwnership({
                    ...ownershipLabels,
                    held: true,
                    epoch: result.value.lease.epoch,
                    leaseExpiresAt: result.value.lease.leaseExpiresAt,
                  })
                }
                const result = yield* store
                  .provide(
                    { directory: partition.workspaceDirectory },
                    prompt.recoverWorkspaceQueueOnce({ tenantID: partition.tenantID }),
                  )
                  .pipe(
                    Effect.catchCause((cause) =>
                      Effect.logError("PostgreSQL workspace recovery attempt failed", {
                        tenantID: partition.tenantID,
                        workspaceID,
                        cause,
                      }).pipe(Effect.as({ status: "failed" as const })),
                    ),
                  )
                yield* Effect.promise(() => governance.record(result.status))
                yield* Effect.sleep(settings.retryMs)
              }),
            ),
          )
        }),
      { discard: true },
    )
    return Service.of({
      enabled: true,
      ownershipEnabled: settings.ownership.enabled,
      workspaces: settings.workspaces,
      partitions: settings.partitions,
    })
  }),
)

export function settingsFromEnv(env: NodeJS.ProcessEnv) {
  if (env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_ENABLED !== "1") return
  if (
    env.OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED !== "1" ||
    env.OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR !== "1" ||
    env.OPENCODE_POSTGRES_WORKER_QUEUE_CONSUMER !== "legacy-prompt"
  ) {
    throw new Error("Workspace recovery requires the SaaS legacy Prompt worker queue")
  }
  const raw = (env.OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
  if (raw.length === 0) throw new Error("Workspace recovery requires a non-empty workspace allowlist")
  if (raw.length > 32) throw new Error("Workspace recovery supports at most 32 workspaces per process")
  for (const directory of raw) {
    if (!path.isAbsolute(directory)) throw new Error(`Workspace recovery path must be absolute: ${directory}`)
  }
  const workspaces = [...new Set(raw.map((directory) => FSUtil.resolve(directory)))]
  const tenants = [
    ...new Set(
      (
        env.OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS ??
        env.OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS ??
        env.OPENCODE_TENANT_ID ??
        ""
      )
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ]
  if (tenants.length === 0) throw new Error("Workspace recovery requires a non-empty tenant allowlist")
  const partitions = tenants.flatMap((tenantID) =>
    workspaces.map((workspaceDirectory) => ({ tenantID, workspaceDirectory })),
  )
  if (partitions.length > 128) {
    throw new Error("Workspace recovery supports at most 128 tenant/workspace partitions per process")
  }
  const parsedRetryMs = Number(env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_RETRY_MS ?? 1_000)
  const retryMs = Number.isFinite(parsedRetryMs) && parsedRetryMs > 0 ? parsedRetryMs : 1_000
  const ownershipFlag = env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_OWNERSHIP_ENABLED
  if (ownershipFlag !== undefined && ownershipFlag !== "0" && ownershipFlag !== "1") {
    throw new Error("Workspace recovery ownership enabled flag must be 0 or 1")
  }
  const ownershipEnabled = ownershipFlag === "1"
  const ownershipDatabaseURL =
    env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_OWNERSHIP_DATABASE_URL?.trim() ||
    env.OPENCODE_DATABASE_URL?.trim()
  if (ownershipEnabled && !ownershipDatabaseURL) {
    throw new Error("Workspace recovery ownership requires OPENCODE_DATABASE_URL")
  }
  const ownershipLeaseMs = positiveInteger(
    env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_OWNERSHIP_LEASE_MS,
    30_000,
    "Workspace recovery ownership lease",
  )
  const ownershipHeartbeatMs = positiveInteger(
    env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_OWNERSHIP_HEARTBEAT_MS,
    5_000,
    "Workspace recovery ownership heartbeat",
  )
  if (ownershipHeartbeatMs >= ownershipLeaseMs) {
    throw new Error("Workspace recovery ownership heartbeat must be shorter than the lease")
  }
  return {
    workspaces,
    tenants,
    partitions,
    retryMs,
    governance: workerRecoveryGovernanceConfig(env),
    instanceID: env.OPENCODE_WORKER_ID?.trim() || `worker-${process.pid}`,
    teamID: env.OPENCODE_TEAM_ID?.trim() || undefined,
    ownership: {
      enabled: ownershipEnabled,
      databaseURL: ownershipDatabaseURL,
      databaseMax: positiveInteger(
        env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_OWNERSHIP_DATABASE_MAX,
        2,
        "Workspace recovery ownership database max",
      ),
      leaseMs: ownershipLeaseMs,
      heartbeatMs: ownershipHeartbeatMs,
    },
  }
}

function positiveInteger(raw: string | undefined, fallback: number, label: string) {
  if (raw === undefined || raw.trim() === "") return fallback
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`)
  }
  return value
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [InstanceStore.node, SessionPrompt.node],
})

export * as SessionWorkerRecovery from "./worker-recovery"
