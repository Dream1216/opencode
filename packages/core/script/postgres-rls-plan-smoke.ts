import { DatabaseBackend } from "../src/database/backend"
import { PostgresDatabase } from "../src/database/postgres"
import { workloads } from "../src/database/postgres/dual-run"
import { exportPlan, validateImportBatch } from "../src/database/postgres/migration-tooling"
import { evaluate, passingPostgresReleaseProof } from "../src/database/postgres/release-gate"
import { migrations, validateDraft } from "../src/database/postgres/schema"

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const sqlite = DatabaseBackend.fromEnv(() => ":memory:")
assert(sqlite.type === "sqlite", "default backend must remain sqlite")

const postgres: DatabaseBackend.PostgresConfig = {
  type: "postgres",
  url: "postgres://opencode@example.test/opencode",
  requireRls: true,
  tenantID: "tenant_a",
  actorID: "actor_a",
}
assert(PostgresDatabase.startupConfigIssues(postgres).length === 0, "valid postgres config should pass startup config checks")
assert(validateDraft().length === 0, "postgres schema draft must include required tenant and RLS primitives")
assert(migrations.length >= 5, "postgres schema draft must include staged migrations")
assert(workloads.every((workload) => workload.requiredForRelease), "all current dual-run workloads must be release blocking")
assert(exportPlan().mode === "read-only-export", "migration tooling must keep SQLite export read-only")
assert(
  validateImportBatch({ tenantID: "tenant_a", actorID: "actor_a", sessions: 1, events: 1 }).length === 0,
  "valid import batch should pass",
)

const blocked = evaluate({
  backend: sqlite,
  saasRelease: true,
  workloadResults: new Map(),
  rlsNegativeTestsPassed: false,
  appRoleSubjectToRls: false,
})
assert(!blocked.allowed, "SaaS gate must block without PostgreSQL and RLS proof")

const blockedPostgresAlphaLiveCapabilities = evaluate({
  backend: postgres,
  saasRelease: true,
  workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
  rlsNegativeTestsPassed: true,
  appRoleSubjectToRls: true,
  postgresAlphaProjectorEnabled: true,
  postgresAlphaStreamEnabled: true,
  postgresAlphaSessionProjectionEnabled: true,
})
assert(!blockedPostgresAlphaLiveCapabilities.allowed, "PostgreSQL alpha gate must block projector/stream/session projection")

const allowed = evaluate({
  backend: postgres,
  saasRelease: true,
  workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
  proof: passingPostgresReleaseProof,
})
assert(allowed.allowed, `SaaS gate should allow when all PostgreSQL RLS checks pass: ${allowed.reasons.join(", ")}`)

console.log("postgres-rls-plan smoke passed")
