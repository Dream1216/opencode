import { DatabaseBackend } from "../backend"
import { workloads } from "./dual-run"
import { evaluate, passingPostgresReleaseProof, type PostgresReleaseProof } from "./release-gate"

export type SaasReleaseGateSmokeResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
}

export function runSaasReleaseGateSmoke(): SaasReleaseGateSmokeResult {
  const checks: string[] = []
  const postgres: DatabaseBackend.PostgresConfig = {
    type: "postgres",
    url: "postgres://opencode@example.test/opencode",
    requireRls: true,
    tenantID: "tenant_gate",
    actorID: "actor_gate",
  }

  const allowed = evaluate({
    backend: postgres,
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    proof: passingPostgresReleaseProof,
  })
  assert(allowed.allowed, `structured SaaS release proof should allow release: ${allowed.reasons.join(", ")}`)
  checks.push("structured-proof-allows")

  const missingProof = evaluate({
    backend: postgres,
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    rlsNegativeTestsPassed: true,
    appRoleSubjectToRls: true,
  })
  assert(!missingProof.allowed, "SaaS release should require structured PostgreSQL proof")
  checks.push("missing-proof-blocked")

  const failedProof = evaluate({
    backend: postgres,
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    proof: {
      ...passingPostgresReleaseProof,
      migrationAdvisoryLockPassed: false,
      appRoleHasNoBypassRls: false,
      workerEventAlignmentPassed: false,
      projectorDisabled: false,
    } satisfies PostgresReleaseProof,
  })
  assert(!failedProof.allowed, "SaaS release should block failed PostgreSQL proof")
  assert(
    failedProof.reasons.some((reason) => reason.includes("migration advisory lock")),
    "failed proof should mention migration advisory lock",
  )
  assert(failedProof.reasons.some((reason) => reason.includes("BYPASSRLS")), "failed proof should mention BYPASSRLS")
  assert(
    failedProof.reasons.some((reason) => reason.includes("Worker event alignment")),
    "failed proof should mention worker alignment",
  )
  assert(
    failedProof.reasons.some((reason) => reason.includes("projector path")),
    "failed proof should mention projector path",
  )
  checks.push("failed-proof-blocked")

  const alphaLiveEnabled = evaluate({
    backend: postgres,
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    proof: passingPostgresReleaseProof,
    postgresAlphaProjectorEnabled: true,
    postgresAlphaStreamEnabled: true,
    postgresAlphaSessionProjectionEnabled: true,
  })
  assert(!alphaLiveEnabled.allowed, "SaaS release should block PG alpha live capability flags")
  checks.push("alpha-live-flags-blocked")

  const postgresAlpha: DatabaseBackend.PostgresAlphaConfig = {
    type: "postgres-alpha",
    filename: "/tmp/opencode-alpha.sqlite",
    url: "postgres://opencode@example.test/opencode",
    requireRls: true,
    tenantID: "tenant_gate",
    actorID: "actor_gate",
    dualWriteEnabled: true,
    projectorEnabled: false,
    streamEnabled: false,
    sessionProjectionEnabled: false,
  }
  const alphaAllowed = evaluate({
    backend: postgresAlpha,
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    proof: passingPostgresReleaseProof,
  })
  assert(alphaAllowed.allowed, `postgres-alpha should satisfy the structured gate: ${alphaAllowed.reasons.join(", ")}`)
  checks.push("postgres-alpha-structured-proof-allows")

  const alphaProjectorEnabled = evaluate({
    backend: { ...postgresAlpha, projectorEnabled: true },
    saasRelease: true,
    workloadResults: new Map(workloads.map((workload) => [workload.id, true] as const)),
    proof: passingPostgresReleaseProof,
  })
  assert(!alphaProjectorEnabled.allowed, "postgres-alpha config should block projector integration")
  checks.push("postgres-alpha-config-live-flag-blocked")

  return { status: "ok", checks }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}
