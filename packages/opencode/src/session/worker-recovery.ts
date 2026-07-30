import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Context, Effect, Layer } from "effect"
import path from "node:path"
import { InstanceStore } from "@/project/instance-store"
import { SessionPrompt } from "./prompt"

export interface Interface {
  readonly enabled: boolean
  readonly workspaces: readonly string[]
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionWorkerRecovery") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const settings = settingsFromEnv(process.env)
    if (settings === undefined) return Service.of({ enabled: false, workspaces: [] })
    const store = yield* InstanceStore.Service
    const prompt = yield* SessionPrompt.Service
    yield* Effect.forEach(
      settings.workspaces,
      (directory) =>
        Effect.forever(
          store.provide({ directory }, prompt.recoverWorkspaceQueue).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("PostgreSQL workspace recovery consumer stopped", {
                directory,
                cause,
              }),
            ),
            Effect.andThen(Effect.sleep(settings.retryMs)),
          ),
        ).pipe(Effect.forkScoped),
      { discard: true },
    )
    return Service.of({ enabled: true, workspaces: settings.workspaces })
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
  const parsedRetryMs = Number(env.OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_RETRY_MS ?? 1_000)
  const retryMs = Number.isFinite(parsedRetryMs) && parsedRetryMs > 0 ? parsedRetryMs : 1_000
  return { workspaces, retryMs }
}

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [InstanceStore.node, SessionPrompt.node],
})

export * as SessionWorkerRecovery from "./worker-recovery"
