import { Effect, Layer } from "effect"
import { DatabaseBackend } from "../backend"
import { Database } from "../database"
import { assertAlphaStartup, evaluateAlphaStartup, type AlphaStartupResult } from "./alpha-startup"

export type AlphaStartupSmokeResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
  readonly real?: AlphaStartupResult
}

export async function runAlphaStartupSmoke(env: NodeJS.ProcessEnv = process.env): Promise<AlphaStartupSmokeResult> {
  const checks: string[] = []
  const sqlite = DatabaseBackend.fromEnv(() => "/tmp/opencode-default.sqlite", {})
  assert(sqlite.type === "sqlite", "SQLite should remain the default backend")
  checks.push("sqlite-default-preserved")

  const alpha = DatabaseBackend.fromEnv(() => "/tmp/opencode-alpha.sqlite", {
    OPENCODE_DATABASE_BACKEND: "postgres-alpha",
    OPENCODE_DATABASE_URL: "postgres://opencode@example.test/opencode",
    OPENCODE_TENANT_ID: "tenant_alpha",
    OPENCODE_ACTOR_ID: "actor_alpha",
  })
  assert(alpha.type === "postgres-alpha", "postgres-alpha backend should parse")
  assert(alpha.filename === "/tmp/opencode-alpha.sqlite", "postgres-alpha should retain the SQLite primary path")
  assert(evaluateAlphaStartup(alpha).allowed, "valid postgres-alpha config should pass the static startup gate")
  checks.push("postgres-alpha-config-allowed")
  checks.push("sqlite-primary-path-retained")

  const missingUrl = evaluateAlphaStartup({ ...alpha, url: undefined })
  assert(!missingUrl.allowed, "postgres-alpha should require OPENCODE_DATABASE_URL")
  checks.push("missing-url-blocked")

  const rlsDisabled = evaluateAlphaStartup({ ...alpha, requireRls: false })
  assert(!rlsDisabled.allowed, "postgres-alpha should require RLS")
  checks.push("rls-disable-blocked")

  const missingIdentity = evaluateAlphaStartup({ ...alpha, tenantID: undefined, actorID: undefined })
  assert(!missingIdentity.allowed, "postgres-alpha should require tenant and actor identity")
  checks.push("missing-identity-blocked")

  for (const config of [
    { ...alpha, projectorEnabled: true },
    { ...alpha, streamEnabled: true },
    { ...alpha, sessionProjectionEnabled: true },
  ]) {
    assert(!evaluateAlphaStartup(config).allowed, "postgres-alpha should block unsupported live capabilities")
  }
  checks.push("unsupported-live-capabilities-blocked")

  if (env.OPENCODE_POSTGRES_ALPHA_SMOKE !== "1") return { status: "ok", checks }
  const real = DatabaseBackend.fromEnv(() => ":memory:", {
    ...env,
    OPENCODE_DATABASE_BACKEND: "postgres-alpha",
    OPENCODE_POSTGRES_REQUIRE_RLS: "1",
    OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED: "0",
    OPENCODE_TENANT_ID: env.OPENCODE_TENANT_ID ?? "tenant_alpha_smoke",
    OPENCODE_ACTOR_ID: env.OPENCODE_ACTOR_ID ?? "actor_alpha_smoke",
    OPENCODE_POSTGRES_ALPHA_PROJECTOR_ENABLED: "0",
    OPENCODE_POSTGRES_ALPHA_STREAM_ENABLED: "0",
    OPENCODE_POSTGRES_ALPHA_SESSION_PROJECTION_ENABLED: "0",
  })
  assert(real.type === "postgres-alpha", "real smoke should select postgres-alpha")
  const ready = await assertAlphaStartup(real)
  checks.push("postgres-connectivity-ready")
  checks.push("postgres-rls-ready")
  checks.push("postgres-runtime-role-subject-to-rls")
  await Effect.runPromise(Layer.build(Database.layerFromBackend(real)).pipe(Effect.scoped))
  checks.push("database-layer-startup-gate-ready")
  return { status: "ok", checks, real: ready }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}
