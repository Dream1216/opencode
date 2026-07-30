import { Effect } from "effect"
import type { Sql } from "postgres"
import { cleanupWorkerJob } from "../database/postgres/worker-job"
import { applyMigrations, assertRlsReady } from "../database/postgres/migration"
import { Service, layerFromEnv } from "./worker-queue"
import { SessionSchema } from "./schema"

export async function runWorkerQueueSmoke(sql: Sql, input: { readonly url: string }) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_queue_${suffix}`
  const actorID = `actor_worker_queue_${suffix}`
  const sessionID = SessionSchema.ID.make(`ses_worker_queue_${suffix}`)
  const otherSessionID = SessionSchema.ID.make(`ses_worker_queue_other_${suffix}`)
  const sidecarSessionID = SessionSchema.ID.make(`ses_worker_queue_sidecar_${suffix}`)
  const sidecarOtherSessionID = SessionSchema.ID.make(`ses_worker_queue_sidecar_other_${suffix}`)
  const unlistedSessionID = SessionSchema.ID.make(`ses_worker_queue_unlisted_${suffix}`)
  const workspaceA = `/tmp/opencode-worker-queue-${suffix}-a`
  const workspaceB = `/tmp/opencode-worker-queue-${suffix}-b`
  const tenant = { tenantID, actorID }
  const env = {
    ...process.env,
    OPENCODE_DATABASE_BACKEND: "postgres-alpha",
    OPENCODE_DATABASE_URL: input.url,
    OPENCODE_TENANT_ID: tenantID,
    OPENCODE_ACTOR_ID: actorID,
    OPENCODE_WORKER_ID: "worker-queue-smoke",
    OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED: "1",
    OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED: "1",
    OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS: tenantID,
    OPENCODE_POSTGRES_WORKER_JOB_CLAIM_TTL_MS: "5000",
    OPENCODE_POSTGRES_WORKER_QUEUE_POLL_MS: "25",
  }
  const checks: string[] = []
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const queue = yield* Service
        if (!queue.enabled) return yield* Effect.die("Worker queue should be enabled")
        const firstGeneration = yield* queue.enqueue(sessionID, "wake", workspaceA)
        const otherGeneration = yield* queue.enqueue(otherSessionID, "wake", workspaceB)
        const wrongWorkspace = yield* queue.claim({ sessionID, workspaceDirectory: workspaceB })
        if (wrongWorkspace !== undefined) {
          return yield* Effect.die("Worker queue crossed a workspace boundary")
        }
        const first = yield* queue.claim({ sessionID, workspaceDirectory: workspaceA })
        if (first === undefined || first.requestedGeneration !== firstGeneration) {
          return yield* Effect.die("Worker queue did not claim the first generation")
        }
        const other = yield* queue.claim({ sessionID: otherSessionID, workspaceDirectory: workspaceB })
        if (other === undefined || other.requestedGeneration !== otherGeneration) {
          return yield* Effect.die("Worker queue did not claim the requested Session")
        }
        yield* queue.complete(other)
        checks.push("queue-enqueue-claim-ready")
        checks.push("queue-session-scoped-claim-ready")
        checks.push("queue-workspace-boundary-ready")
        const heartbeat = yield* queue.heartbeat(first)
        if (heartbeat.claimExpiresAt <= first.claimExpiresAt) {
          return yield* Effect.die("Worker queue heartbeat did not extend the claim")
        }
        checks.push("queue-claim-heartbeat-ready")

        const secondGeneration = yield* queue.enqueue(sessionID, "resume", workspaceA)
        yield* queue.complete(heartbeat)
        const second = yield* queue.claim({ sessionID, workspaceDirectory: workspaceA })
        if (second === undefined || second.claimedGeneration !== secondGeneration) {
          return yield* Effect.die("Worker queue did not preserve a generation enqueued during execution")
        }
        yield* queue.complete(second)
        yield* queue.awaitCompletion(sessionID, secondGeneration)
        checks.push("running-generation-requeued")
        checks.push("generation-completion-awaited")
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )

    const disabled = await Effect.runPromise(
      Effect.gen(function* () {
        const queue = yield* Service
        return queue.enabled
      }).pipe(
        Effect.provide(layerFromEnv({ ...env, OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS: "another-tenant" })),
        Effect.scoped,
      ),
    )
    if (disabled) throw new Error("Worker queue tenant allowlist did not disable the adapter")
    checks.push("queue-tenant-feature-flag-disabled")

    await Effect.runPromise(
      Effect.gen(function* () {
        const queue = yield* Service
        if (!queue.enabled || queue.consumerMode !== "legacy-prompt") {
          return yield* Effect.die("SaaS legacy Prompt queue sidecar should be enabled")
        }
        const generation = yield* queue.enqueue(sidecarSessionID, "resume", workspaceA)
        const otherGeneration = yield* queue.enqueue(sidecarOtherSessionID, "resume", workspaceB)
        if (generation !== 1) return yield* Effect.die("SaaS queue did not enqueue the first generation")
        const claim = yield* queue.claim({ workspaceDirectory: workspaceA })
        if (claim?.runID !== sidecarSessionID || claim.tenantID !== tenantID) {
          return yield* Effect.die("SaaS queue claimed the wrong tenant or Session")
        }
        yield* queue.complete(claim)
        yield* queue.awaitCompletion(sidecarSessionID, generation)
        checks.push("saas-queue-session-tenant-resolved")
        checks.push("saas-queue-session-scoped-claim-ready")
        checks.push("saas-queue-workspace-partition-ready")
        checks.push("saas-queue-allowlisted-generation-completed")

        const otherClaim = yield* queue.claim({ workspaceDirectory: workspaceB })
        if (otherClaim?.runID !== sidecarOtherSessionID || otherClaim.claimedGeneration !== otherGeneration) {
          return yield* Effect.die("SaaS queue did not recover the other workspace independently")
        }
        yield* queue.complete(otherClaim)

        const bypass = yield* queue.enqueue(unlistedSessionID, "resume", workspaceA)
        if (bypass !== 0) return yield* Effect.die("Unlisted SaaS tenant was enqueued")
        yield* queue.awaitCompletion(unlistedSessionID, bypass)
        checks.push("saas-queue-unlisted-tenant-bypassed")
      }).pipe(
        Effect.provide(
          layerFromEnv(
            {
              ...env,
              OPENCODE_DATABASE_BACKEND: "sqlite",
              OPENCODE_SAAS_MODE: "true",
              OPENCODE_TENANT_ID: undefined,
              OPENCODE_ACTOR_ID: undefined,
              OPENCODE_POSTGRES_WORKER_COORDINATION_SAAS_SIDECAR: "1",
              OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR: "1",
              OPENCODE_POSTGRES_WORKER_QUEUE_CONSUMER: "legacy-prompt",
            },
            {
              resolveTenant: async (_sql, current) =>
                current === sidecarSessionID || current === sidecarOtherSessionID
                  ? tenant
                  : { tenantID: "tenant_not_allowlisted" },
            },
          ),
        ),
        Effect.scoped,
      ),
    )
    return { status: "ok" as const, checks }
  } finally {
    await cleanupWorkerJob(sql, tenant, sessionID)
    await cleanupWorkerJob(sql, tenant, otherSessionID)
    await cleanupWorkerJob(sql, tenant, sidecarSessionID)
    await cleanupWorkerJob(sql, tenant, sidecarOtherSessionID)
  }
}
