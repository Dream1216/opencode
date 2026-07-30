export * as SessionWorkerCoordination from "./worker-coordination"

import { Context, Effect, Layer } from "effect"
import type { Sql } from "postgres"
import { DatabaseBackend } from "../database/backend"
import { makeClient, type TenantContext } from "../database/postgres/client"
import { assertRlsReady } from "../database/postgres/migration"
import {
  acquireWorkerLease,
  assertWorkerFence,
  completeWorkerLease,
  heartbeatWorkerLease,
  releaseWorkerLease,
  type WorkerLease,
} from "../database/postgres/worker-lease"
import { makeGlobalNode } from "../effect/app-node"
import { SessionSchema } from "./schema"
import type { WorkerFenceToken } from "./worker-fence"

export type Handle = {
  readonly lease: WorkerLease
  readonly fence: WorkerFenceToken
  readonly tenant: TenantContext
  readonly ttlMs: number
}

export type Acquisition =
  | { readonly status: "disabled" }
  | { readonly status: "contended"; readonly lease: WorkerLease }
  | { readonly status: "acquired"; readonly handle: Handle }

export interface Interface {
  readonly enabled: boolean
  readonly acquire: (sessionID: SessionSchema.ID) => Effect.Effect<Acquisition>
  readonly heartbeat: (handle: Handle) => Effect.Effect<Handle>
  readonly release: (handle: Handle) => Effect.Effect<WorkerLease>
  readonly complete: (handle: Handle) => Effect.Effect<WorkerLease>
  readonly assertExecutionFence: (
    sessionID: SessionSchema.ID,
    fence?: WorkerFenceToken,
  ) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionWorkerCoordination") {}

export class WorkerFenceRequiredError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WorkerFenceRequiredError"
  }
}

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
      if (settings === undefined) return disabledService()
      const sql = makeClient({
        url: settings.url,
        max: 2,
        connectTimeoutSeconds: Number(env.OPENCODE_POSTGRES_CONNECT_TIMEOUT_SECONDS ?? 10),
        idleTimeoutSeconds: Number(env.OPENCODE_POSTGRES_IDLE_TIMEOUT_SECONDS ?? 5),
      })
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise(() => sql.end({ timeout: 5 })).pipe(Effect.catchCause(() => Effect.void)),
      )
      yield* Effect.tryPromise(() => assertRlsReady(sql)).pipe(Effect.orDie)

      const tenantFor = Effect.fn("SessionWorkerCoordination.tenantFor")(function* (
        sessionID: SessionSchema.ID,
      ) {
        const tenant =
          settings.mode === "static"
            ? settings.tenant
            : yield* Effect.tryPromise(() =>
                (options.resolveTenant ?? resolveSessionTenant)(sql, sessionID),
              ).pipe(Effect.orDie)
        if (tenant === undefined || !settings.allowlist.includes(tenant.tenantID)) return
        return tenant
      })
      const fenceOf = (lease: WorkerLease): WorkerFenceToken => ({
        backend: "postgres",
        runID: lease.runID,
        ownerID: lease.ownerID,
        fencingToken: lease.fencingToken,
      })
      const pgFence = (tenant: TenantContext, fence: WorkerFenceToken) => ({
        tenant,
        runID: fence.runID,
        ownerID: fence.ownerID,
        fencingToken: fence.fencingToken,
      })
      const handle = (tenant: TenantContext, lease: WorkerLease): Handle => ({
        lease,
        fence: fenceOf(lease),
        tenant,
        ttlMs: settings.ttlMs,
      })
      return Service.of({
        enabled: true,
        acquire: Effect.fn("SessionWorkerCoordination.acquire")(function* (sessionID) {
          const tenant = yield* tenantFor(sessionID)
          if (tenant === undefined) return { status: "disabled" as const }
          const result = yield* Effect.tryPromise(() =>
            acquireWorkerLease(sql, {
              tenant,
              runID: sessionID,
              ownerID: settings.ownerID,
              leaseMs: settings.ttlMs,
            }),
          ).pipe(Effect.orDie)
          return result.acquired
            ? ({ status: "acquired", handle: handle(tenant, result.lease) } as const)
            : ({ status: "contended", lease: result.lease } as const)
        }),
        heartbeat: Effect.fn("SessionWorkerCoordination.heartbeat")(function* (current) {
          const lease = yield* Effect.tryPromise(() =>
            heartbeatWorkerLease(sql, pgFence(current.tenant, current.fence), settings.ttlMs),
          ).pipe(Effect.orDie)
          return handle(current.tenant, lease)
        }),
        release: Effect.fn("SessionWorkerCoordination.release")(function* (current) {
          return yield* Effect.tryPromise(() =>
            releaseWorkerLease(sql, pgFence(current.tenant, current.fence)),
          ).pipe(Effect.orDie)
        }),
        complete: Effect.fn("SessionWorkerCoordination.complete")(function* (current) {
          return yield* Effect.tryPromise(() =>
            completeWorkerLease(sql, pgFence(current.tenant, current.fence)),
          ).pipe(Effect.orDie)
        }),
        assertExecutionFence: Effect.fn("SessionWorkerCoordination.assertExecutionFence")(function* (
          sessionID,
          fence,
        ) {
          const tenant = yield* tenantFor(sessionID)
          if (tenant === undefined) {
            if (fence !== undefined) {
              return yield* Effect.die(
                new WorkerFenceRequiredError(
                  `Session ${sessionID} has a PostgreSQL fencing token but is outside the worker tenant allowlist`,
                ),
              )
            }
            return
          }
          if (fence === undefined) {
            return yield* Effect.die(
              new WorkerFenceRequiredError(
                `PostgreSQL-coordinated Session ${sessionID} execution requires a fencing token`,
              ),
            )
          }
          if (fence.runID !== sessionID) {
            return yield* Effect.die(
              new WorkerFenceRequiredError(
                `Worker fencing token for ${fence.runID} cannot execute Session ${sessionID}`,
              ),
            )
          }
          yield* Effect.tryPromise(() => assertWorkerFence(sql, pgFence(tenant, fence))).pipe(
            Effect.orDie,
          )
        }),
      })
    }),
  )
}

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

function disabledService() {
  return Service.of({
    enabled: false,
    acquire: () => Effect.succeed({ status: "disabled" as const }),
    heartbeat: (handle) => Effect.succeed(handle),
    release: (handle) => Effect.succeed(handle.lease),
    complete: (handle) => Effect.succeed(handle.lease),
    assertExecutionFence: () => Effect.void,
  })
}

function settingsFromEnv(env: NodeJS.ProcessEnv) {
  if (!enabled(env.OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED)) return undefined
  const backend = DatabaseBackend.fromEnv(() => ":memory:", env)
  const sidecar = enabled(env.OPENCODE_POSTGRES_WORKER_COORDINATION_SAAS_SIDECAR)
  const ownerID = env.OPENCODE_WORKER_ID ?? `pid_${process.pid}`
  const ttlMs = Math.max(1_000, Number(env.OPENCODE_WORKER_LEASE_TTL_MS ?? 15_000))
  if (sidecar) {
    if (backend.type !== "sqlite") {
      throw new Error("SaaS worker coordination sidecar requires SQLite to remain the primary database")
    }
    if (!enabled(env.OPENCODE_SAAS_MODE)) {
      throw new Error("SaaS worker coordination sidecar requires OPENCODE_SAAS_MODE=true")
    }
    const allowlist = parseAllowlist(env.OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS)
    if (allowlist.length === 0) {
      throw new Error("SaaS worker coordination sidecar requires a non-empty tenant allowlist")
    }
    return {
      mode: "saas-sidecar" as const,
      url: required(env.OPENCODE_DATABASE_URL, "OPENCODE_DATABASE_URL"),
      allowlist,
      ownerID,
      ttlMs,
    }
  }
  if (backend.type !== "postgres-alpha") {
    throw new Error("PostgreSQL worker coordination requires OPENCODE_DATABASE_BACKEND=postgres-alpha")
  }
  const tenantID = required(backend.tenantID, "OPENCODE_TENANT_ID")
  const allowlist = parseAllowlist(env.OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS ?? tenantID)
  if (!allowlist.includes(tenantID)) return undefined
  return {
    mode: "static" as const,
    url: required(backend.url, "OPENCODE_DATABASE_URL"),
    tenant: {
      tenantID,
      actorID: required(backend.actorID, "OPENCODE_ACTOR_ID"),
    } satisfies TenantContext,
    allowlist,
    ownerID,
    ttlMs,
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

function enabled(value: string | undefined) {
  return value === "1" || value === "true"
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
