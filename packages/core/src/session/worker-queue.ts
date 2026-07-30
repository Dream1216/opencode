import { Context, Effect, Layer } from "effect"
import {
  cancelWorkerJob,
  claimNextWorkerJob,
  completeWorkerJobClaim,
  enqueueWorkerJob,
  failWorkerJobClaim,
  heartbeatWorkerJobClaim,
  readWorkerJob,
  type WorkerJobClaim,
  type WorkerJobReason,
} from "../database/postgres/worker-job"
import { configFromEnv, makeClient, type TenantContext } from "../database/postgres/client"
import { assertRlsReady } from "../database/postgres/migration"
import { readWorkerQueueReadiness } from "../database/postgres/worker-queue-operations"
import { observeWorkerQueue } from "../database/postgres/worker-queue-telemetry"
import { makeGlobalNode } from "../effect/app-node"
import type { SessionSchema } from "./schema"

export type Interface = {
  readonly enabled: boolean
  readonly pollIntervalMs: number
  readonly heartbeatIntervalMs: number
  readonly enqueue: (
    sessionID: SessionSchema.ID,
    reason: Exclude<WorkerJobReason, "recovery">,
  ) => Effect.Effect<number>
  readonly claim: () => Effect.Effect<WorkerJobClaim | undefined>
  readonly heartbeat: (claim: WorkerJobClaim) => Effect.Effect<WorkerJobClaim>
  readonly complete: (claim: WorkerJobClaim) => Effect.Effect<void>
  readonly fail: (claim: WorkerJobClaim, error: string) => Effect.Effect<void>
  readonly cancel: (sessionID: SessionSchema.ID) => Effect.Effect<void>
  readonly awaitCompletion: (sessionID: SessionSchema.ID, generation: number) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionWorkerQueue") {}

const disabled = Service.of({
  enabled: false,
  pollIntervalMs: 1_000,
  heartbeatIntervalMs: 1_000,
  enqueue: () => Effect.succeed(0),
  claim: () => Effect.succeed(undefined),
  heartbeat: (claim) => Effect.succeed(claim),
  complete: () => Effect.void,
  fail: () => Effect.void,
  cancel: () => Effect.void,
  awaitCompletion: () => Effect.void,
})

export const layer = layerFromEnv()

export function layerFromEnv(env: NodeJS.ProcessEnv = process.env) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
  const tenantID = env.OPENCODE_TENANT_ID?.trim()
  const actorID = env.OPENCODE_ACTOR_ID?.trim()
  const queueAllowlist =
    env.OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS ?? env.OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS
  const enabled =
    env.OPENCODE_DATABASE_BACKEND === "postgres-alpha" &&
    env.OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED === "1" &&
    env.OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED === "1" &&
    tenantID !== undefined &&
    tenantID !== "" &&
    allowed(queueAllowlist, tenantID)
      if (!enabled) return disabled
  const config = configFromEnv(env)
      if (config === undefined) return yield* Effect.die("PostgreSQL worker queue requires OPENCODE_DATABASE_URL")
  const tenant = { tenantID, ...(actorID === undefined || actorID === "" ? {} : { actorID }) } satisfies TenantContext
  const ownerID = env.OPENCODE_WORKER_ID?.trim() || `worker-${process.pid}`
  const claimMs = positive(env.OPENCODE_POSTGRES_WORKER_JOB_CLAIM_TTL_MS, 30_000)
  const pollIntervalMs = positive(env.OPENCODE_POSTGRES_WORKER_QUEUE_POLL_MS, 250)
  const heartbeatIntervalMs = Math.max(250, Math.floor(claimMs / 3))
  const maxAttempts = positive(env.OPENCODE_POSTGRES_WORKER_JOB_MAX_ATTEMPTS, 5)
  const retryDelayMs = positive(env.OPENCODE_POSTGRES_WORKER_JOB_RETRY_DELAY_MS, 1_000)
      const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 2) })
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise(() => sql.end({ timeout: 5 })).pipe(Effect.catchCause(() => Effect.void)),
      )
      yield* Effect.tryPromise(() => assertRlsReady(sql)).pipe(Effect.orDie)
      const metricsIntervalMs = positive(env.OPENCODE_WORKER_QUEUE_METRICS_INTERVAL_MS, 15_000)
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.tryPromise(() => readWorkerQueueReadiness(sql, { tenant })).pipe(
            Effect.tap((readiness) =>
              Effect.sync(() =>
                observeWorkerQueue(readiness, {
                  tenantID: tenant.tenantID,
                  ...(env.OPENCODE_TEAM_ID === undefined ? {} : { teamID: env.OPENCODE_TEAM_ID }),
                }),
              ),
            ),
            Effect.catchCause(() => Effect.void),
            Effect.andThen(Effect.sleep(metricsIntervalMs)),
          ),
        ),
      )
      return Service.of({
        enabled: true,
        pollIntervalMs,
        heartbeatIntervalMs,
        enqueue: (sessionID, reason) =>
          db(() => enqueueWorkerJob(sql, { tenant, runID: sessionID, reason })).pipe(
            Effect.map((job) => job.requestedGeneration),
          ),
        claim: () => db(() => claimNextWorkerJob(sql, { tenant, ownerID, claimMs })),
        heartbeat: (claim) => db(() => heartbeatWorkerJobClaim(sql, claim, claimMs)),
        complete: (claim) => db(() => completeWorkerJobClaim(sql, claim)).pipe(Effect.asVoid),
        fail: (claim, error) =>
          db(() => failWorkerJobClaim(sql, { claim, error, maxAttempts, retryDelayMs })).pipe(Effect.asVoid),
        cancel: (sessionID) => db(() => cancelWorkerJob(sql, tenant, sessionID)).pipe(Effect.asVoid),
        awaitCompletion: (sessionID, generation) =>
          Effect.gen(function* () {
            while (true) {
              const job = yield* db(() => readWorkerJob(sql, tenant, sessionID))
              if (job?.completedGeneration !== undefined && job.completedGeneration >= generation) return
              if (job?.status === "failed") {
                return yield* Effect.die(
                  new Error(`Worker job ${sessionID} failed after ${job.attempts} attempts: ${job.lastError ?? "unknown"}`),
                )
              }
              if (job?.status === "cancelled") {
                return yield* Effect.die(new Error(`Worker job ${sessionID} was cancelled`))
              }
              yield* Effect.sleep(pollIntervalMs)
            }
          }),
      })
    }),
  )
}

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

function db<A>(operation: () => Promise<A>): Effect.Effect<A> {
  return Effect.tryPromise(operation).pipe(Effect.orDie)
}

function positive(value: string | undefined, fallback: number) {
  const parsed = Number(value ?? fallback)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function allowed(value: string | undefined, tenantID: string) {
  if (value === undefined || value.trim() === "") return true
  return value
    .split(",")
    .map((item) => item.trim())
    .includes(tenantID)
}
