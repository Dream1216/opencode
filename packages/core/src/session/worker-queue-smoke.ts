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
        const firstGeneration = yield* queue.enqueue(sessionID, "wake")
        const first = yield* queue.claim()
        if (first === undefined || first.requestedGeneration !== firstGeneration) {
          return yield* Effect.die("Worker queue did not claim the first generation")
        }
        checks.push("queue-enqueue-claim-ready")
        const heartbeat = yield* queue.heartbeat(first)
        if (heartbeat.claimExpiresAt <= first.claimExpiresAt) {
          return yield* Effect.die("Worker queue heartbeat did not extend the claim")
        }
        checks.push("queue-claim-heartbeat-ready")

        const secondGeneration = yield* queue.enqueue(sessionID, "resume")
        yield* queue.complete(heartbeat)
        const second = yield* queue.claim()
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
    return { status: "ok" as const, checks }
  } finally {
    await cleanupWorkerJob(sql, tenant, sessionID)
  }
}
