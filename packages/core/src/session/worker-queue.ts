import { Context, Effect, Layer } from "effect"
import type { Sql } from "postgres"
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
  readonly consumerMode: "disabled" | "core" | "legacy-prompt"
  readonly pollIntervalMs: number
  readonly heartbeatIntervalMs: number
  readonly enqueue: (
    sessionID: SessionSchema.ID,
    reason: Exclude<WorkerJobReason, "recovery">,
  ) => Effect.Effect<number>
  readonly claim: (sessionID?: SessionSchema.ID) => Effect.Effect<WorkerJobClaim | undefined>
  readonly heartbeat: (claim: WorkerJobClaim) => Effect.Effect<WorkerJobClaim>
  readonly complete: (claim: WorkerJobClaim) => Effect.Effect<void>
  readonly fail: (claim: WorkerJobClaim, error: string) => Effect.Effect<void>
  readonly cancel: (sessionID: SessionSchema.ID) => Effect.Effect<void>
  readonly awaitCompletion: (sessionID: SessionSchema.ID, generation: number) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionWorkerQueue") {}

const disabled = Service.of({
  enabled: false,
  consumerMode: "disabled" as const,
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

export function layerFromEnv(
  env: NodeJS.ProcessEnv = process.env,
  options: {
    readonly resolveTenant?: (
      sql: Sql,
      sessionID: SessionSchema.ID,
    ) => Promise<TenantContext | undefined>
  } = {},
) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const settings = settingsFromEnv(env)
      if (settings === undefined) return disabled
      const config = configFromEnv(env)
      if (config === undefined) return yield* Effect.die("PostgreSQL worker queue requires OPENCODE_DATABASE_URL")
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
      const tenantFor = Effect.fn("SessionWorkerQueue.tenantFor")(function* (
        sessionID: SessionSchema.ID,
      ) {
        const tenant =
          settings.mode === "static"
            ? settings.tenants[0]
            : yield* Effect.tryPromise(() =>
                (options.resolveTenant ?? resolveSessionTenant)(sql, sessionID),
              ).pipe(Effect.orDie)
        if (tenant === undefined || !settings.allowlist.includes(tenant.tenantID)) return
        return tenant
      })
      const metricsIntervalMs = positive(env.OPENCODE_WORKER_QUEUE_METRICS_INTERVAL_MS, 15_000)
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.forEach(
            settings.tenants,
            (tenant) =>
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
              ),
            { discard: true },
          ).pipe(
            Effect.catchCause(() => Effect.void),
            Effect.andThen(Effect.sleep(metricsIntervalMs)),
          ),
        ),
      )
      let claimCursor = 0
      return Service.of({
        enabled: true,
        consumerMode: settings.consumerMode,
        pollIntervalMs,
        heartbeatIntervalMs,
        enqueue: (sessionID, reason) =>
          tenantFor(sessionID).pipe(
            Effect.flatMap((tenant) =>
              tenant === undefined
                ? Effect.succeed(0)
                : db(() => enqueueWorkerJob(sql, { tenant, runID: sessionID, reason })).pipe(
                    Effect.map((job) => job.requestedGeneration),
                  ),
            ),
          ),
        claim: (sessionID) =>
          Effect.gen(function* () {
            if (sessionID !== undefined) {
              const tenant = yield* tenantFor(sessionID)
              if (tenant === undefined) return
              return yield* db(() =>
                claimNextWorkerJob(sql, { tenant, ownerID, claimMs, runID: sessionID }),
              )
            }
            for (let offset = 0; offset < settings.tenants.length; offset++) {
              const index = (claimCursor + offset) % settings.tenants.length
              const tenant = settings.tenants[index]!
              const claim = yield* db(() => claimNextWorkerJob(sql, { tenant, ownerID, claimMs }))
              if (claim !== undefined) {
                claimCursor = (index + 1) % settings.tenants.length
                return claim
              }
            }
          }),
        heartbeat: (claim) => db(() => heartbeatWorkerJobClaim(sql, claim, claimMs)),
        complete: (claim) => db(() => completeWorkerJobClaim(sql, claim)).pipe(Effect.asVoid),
        fail: (claim, error) =>
          db(() => failWorkerJobClaim(sql, { claim, error, maxAttempts, retryDelayMs })).pipe(Effect.asVoid),
        cancel: (sessionID) =>
          tenantFor(sessionID).pipe(
            Effect.flatMap((tenant) =>
              tenant === undefined
                ? Effect.void
                : db(() => cancelWorkerJob(sql, tenant, sessionID)).pipe(Effect.asVoid),
            ),
          ),
        awaitCompletion: (sessionID, generation) =>
          Effect.gen(function* () {
            if (generation === 0) return
            const tenant = yield* tenantFor(sessionID)
            if (tenant === undefined) return
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

function settingsFromEnv(env: NodeJS.ProcessEnv) {
  if (
    env.OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED !== "1" ||
    env.OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED !== "1"
  ) {
    return undefined
  }
  const allowlist = parseAllowlist(
    env.OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS ??
      env.OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS,
  )
  if (env.OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR === "1") {
    if (env.OPENCODE_DATABASE_BACKEND !== "sqlite") {
      throw new Error("SaaS worker queue sidecar requires SQLite to remain the primary database")
    }
    if (env.OPENCODE_SAAS_MODE !== "true" && env.OPENCODE_SAAS_MODE !== "1") {
      throw new Error("SaaS worker queue sidecar requires OPENCODE_SAAS_MODE=true")
    }
    if (env.OPENCODE_POSTGRES_WORKER_COORDINATION_SAAS_SIDECAR !== "1") {
      throw new Error("SaaS worker queue sidecar requires the worker coordination sidecar")
    }
    if (env.OPENCODE_POSTGRES_WORKER_QUEUE_CONSUMER !== "legacy-prompt") {
      throw new Error("SaaS worker queue sidecar requires the legacy-prompt consumer")
    }
    if (allowlist.length === 0) {
      throw new Error("SaaS worker queue sidecar requires a non-empty tenant allowlist")
    }
    return {
      mode: "saas-sidecar" as const,
      consumerMode: "legacy-prompt" as const,
      allowlist,
      tenants: allowlist.map((tenantID) => ({ tenantID } satisfies TenantContext)),
    }
  }
  const tenantID = env.OPENCODE_TENANT_ID?.trim()
  const actorID = env.OPENCODE_ACTOR_ID?.trim()
  if (
    env.OPENCODE_DATABASE_BACKEND !== "postgres-alpha" ||
    tenantID === undefined ||
    tenantID === "" ||
    !allowlist.includes(tenantID)
  ) {
    return undefined
  }
  return {
    mode: "static" as const,
    consumerMode: "core" as const,
    allowlist,
    tenants: [
      {
        tenantID,
        ...(actorID === undefined || actorID === "" ? {} : { actorID }),
      } satisfies TenantContext,
    ],
  }
}

async function resolveSessionTenant(sql: Sql, sessionID: SessionSchema.ID) {
  const rows = await sql<{ readonly tenant_id: string }[]>`
    select tenant_id
      from opencode_execution_resource_locator
     where resource_type = 'session'
       and resource_id = ${sessionID}
     limit 1
  `
  return rows[0] === undefined ? undefined : ({ tenantID: rows[0].tenant_id } satisfies TenantContext)
}

function parseAllowlist(value: string | undefined) {
  return (value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
}

function db<A>(operation: () => Promise<A>): Effect.Effect<A> {
  return Effect.tryPromise(operation).pipe(Effect.orDie)
}

function positive(value: string | undefined, fallback: number) {
  const parsed = Number(value ?? fallback)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}
