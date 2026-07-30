import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import {
  makeWorkerRecoveryGovernance,
  workerRecoveryGovernanceConfig,
  workerRecoveryPartitionID,
  type WorkerRecoveryPartition,
} from "@opencode-ai/core/session/worker-recovery-governance"
import { Context, Effect, Layer } from "effect"
import path from "node:path"
import { InstanceStore } from "@/project/instance-store"
import { SessionPrompt } from "./prompt"

export interface Interface {
  readonly enabled: boolean
  readonly workspaces: readonly string[]
  readonly partitions: readonly WorkerRecoveryPartition[]
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionWorkerRecovery") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const settings = settingsFromEnv(process.env)
    if (settings === undefined) return Service.of({ enabled: false, workspaces: [], partitions: [] })
    const store = yield* InstanceStore.Service
    const prompt = yield* SessionPrompt.Service
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
          yield* Effect.logInfo("starting governed PostgreSQL workspace recovery consumer", {
            tenantID: partition.tenantID,
            workspaceID: workerRecoveryPartitionID(partition),
            instanceID: settings.instanceID,
          })
          yield* Effect.forkScoped(
            Effect.forever(
              Effect.gen(function* () {
                const admission = yield* Effect.promise(() => governance.admit())
                if (!admission.allowed) {
                  yield* Effect.sleep(settings.retryMs)
                  return
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
                        workspaceID: workerRecoveryPartitionID(partition),
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
  return {
    workspaces,
    tenants,
    partitions,
    retryMs,
    governance: workerRecoveryGovernanceConfig(env),
    instanceID: env.OPENCODE_WORKER_ID?.trim() || `worker-${process.pid}`,
    teamID: env.OPENCODE_TEAM_ID?.trim() || undefined,
  }
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [InstanceStore.node, SessionPrompt.node],
})

export * as SessionWorkerRecovery from "./worker-recovery"
