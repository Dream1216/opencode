import { Effect, Exit } from "effect"
import type { Sql } from "postgres"
import { acquireWorkerLease, cleanupWorkerRun } from "../database/postgres/worker-lease"
import { applyMigrations, assertRlsReady } from "../database/postgres/migration"
import { Service, layerFromEnv } from "./worker-coordination"
import { SessionSchema } from "./schema"
import { toolWorkerFence } from "./worker-fence"

export async function runWorkerCoordinationSmoke(sql: Sql, input: { readonly url: string }) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenantID = `tenant_worker_coordination_${suffix}`
  const actorID = `actor_worker_coordination_${suffix}`
  const sessionID = SessionSchema.ID.make(`ses_worker_coordination_${suffix}`)
  const sidecarSessionID = SessionSchema.ID.make(`ses_worker_coordination_sidecar_${suffix}`)
  const unlistedSessionID = SessionSchema.ID.make(`ses_worker_coordination_unlisted_${suffix}`)
  const env = {
    ...process.env,
    OPENCODE_DATABASE_BACKEND: "postgres-alpha",
    OPENCODE_DATABASE_URL: input.url,
    OPENCODE_TENANT_ID: tenantID,
    OPENCODE_ACTOR_ID: actorID,
    OPENCODE_WORKER_ID: "worker-coordination-adapter",
    OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED: "1",
    OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS: tenantID,
    OPENCODE_WORKER_LEASE_TTL_MS: "5000",
  }
  const tenant = { tenantID, actorID }
  const checks: string[] = []
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const coordination = yield* Service
        if (!coordination.enabled) return yield* Effect.die("Worker coordination adapter should be enabled")
        const acquired = yield* coordination.acquire(sessionID)
        if (acquired.status !== "acquired") return yield* Effect.die("Worker coordination adapter did not acquire")
        checks.push("session-execution-adapter-acquired")
        yield* coordination.assertExecutionFence(sessionID, acquired.handle.fence)
        checks.push("execution-boundary-fence-validated")

        const contender = yield* Effect.tryPromise(() =>
          acquireWorkerLease(sql, {
            tenant,
            runID: sessionID,
            ownerID: "worker-coordination-contender",
            leaseMs: 5_000,
          }),
        )
        if (contender.acquired) return yield* Effect.die("Coordination contender acquired a live lease")
        checks.push("competing-session-execution-blocked")

        const heartbeat = yield* coordination.heartbeat(acquired.handle)
        yield* coordination.assertExecutionFence(sessionID, heartbeat.fence)
        checks.push("heartbeat-fence-refreshed")
        const tool = toolWorkerFence(heartbeat.fence, "call_coordination_smoke")
        if (!tool.idempotencyKey.includes(String(heartbeat.fence.fencingToken))) {
          return yield* Effect.die("Tool fencing idempotency key is missing the fencing token")
        }
        checks.push("tool-fence-context-ready")

        yield* coordination.release(heartbeat)
        const stale = yield* coordination.assertExecutionFence(sessionID, heartbeat.fence).pipe(Effect.exit)
        if (Exit.isSuccess(stale)) return yield* Effect.die("Released worker fence remained valid")
        checks.push("released-fence-rejected")
      }).pipe(Effect.provide(layerFromEnv(env)), Effect.scoped),
    )

    const disabled = await Effect.runPromise(
      Service.use((service) => Effect.succeed(service.enabled)).pipe(
        Effect.provide(
          layerFromEnv({
            ...env,
            OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS: "another-tenant",
          }),
        ),
        Effect.scoped,
      ),
    )
    if (disabled) throw new Error("Tenant allowlist did not disable worker coordination")
    checks.push("tenant-feature-flag-disabled")

    await Effect.runPromise(
      Effect.gen(function* () {
        const coordination = yield* Service
        if (!coordination.enabled) return yield* Effect.die("SaaS worker sidecar should be enabled")
        const acquired = yield* coordination.acquire(sidecarSessionID)
        if (acquired.status !== "acquired") return yield* Effect.die("SaaS worker sidecar did not acquire")
        checks.push("saas-sidecar-session-tenant-resolved")
        yield* coordination.assertExecutionFence(sidecarSessionID, acquired.handle.fence)
        checks.push("saas-sidecar-fence-validated")

        const missingFence = yield* coordination
          .assertExecutionFence(sidecarSessionID, undefined)
          .pipe(Effect.exit)
        if (Exit.isSuccess(missingFence)) {
          return yield* Effect.die("Allowlisted SaaS Session executed without a fencing token")
        }
        checks.push("saas-sidecar-missing-fence-rejected")

        const unlisted = yield* coordination.acquire(unlistedSessionID)
        if (unlisted.status !== "disabled") {
          return yield* Effect.die("Unlisted SaaS tenant acquired worker coordination")
        }
        yield* coordination.assertExecutionFence(unlistedSessionID, undefined)
        checks.push("saas-sidecar-unlisted-tenant-disabled")
        yield* coordination.release(acquired.handle)
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
            },
            {
              resolveTenant: async (_sql, current) =>
                current === sidecarSessionID ? tenant : { tenantID: "tenant_not_allowlisted" },
            },
          ),
        ),
        Effect.scoped,
      ),
    )
    return { status: "ok" as const, checks }
  } finally {
    await cleanupWorkerRun(sql, tenant, sessionID)
    await cleanupWorkerRun(sql, tenant, sidecarSessionID)
  }
}
