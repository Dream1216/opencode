import { DatabaseBackend } from "../backend"
import { missingRequired } from "./dual-run"
import { validateDraft } from "./schema"

export type GateInput = {
  readonly backend: DatabaseBackend.Config
  readonly saasRelease: boolean
  readonly workloadResults: ReadonlyMap<string, boolean>
  readonly rlsNegativeTestsPassed?: boolean
  readonly appRoleSubjectToRls?: boolean
  readonly proof?: PostgresReleaseProof
  readonly postgresAlphaProjectorEnabled?: boolean
  readonly postgresAlphaStreamEnabled?: boolean
  readonly postgresAlphaSessionProjectionEnabled?: boolean
}

export type PostgresReleaseProof = {
  readonly migrationsApplied: boolean
  readonly migrationAdvisoryLockPassed: boolean
  readonly rlsReady: boolean
  readonly rlsNegativeTestsPassed: boolean
  readonly appRoleSubjectToRls: boolean
  readonly appRoleIsNotSuperuser: boolean
  readonly appRoleHasNoBypassRls: boolean
  readonly eventStoreDualRunPassed: boolean
  readonly eventV2FacadeContractPassed: boolean
  readonly workerEventAlignmentPassed: boolean
  readonly workerLeaseFencingPassed: boolean
  readonly workerLeaseChaosPassed: boolean
  readonly workerCoordinationAdapterPassed: boolean
  readonly workerFencePropagationPassed: boolean
  readonly workerQueueAdapterPassed: boolean
  readonly workerQueueRecoveryPassed: boolean
  readonly workerQueueOperationsPassed: boolean
  readonly workerQueueOperatorRlsPassed: boolean
  readonly workerQueueTelemetryPassed: boolean
  readonly workerQueueOtlpExportPassed: boolean
  readonly workerQueuePrometheusProxyPassed: boolean
  readonly workerQueueOidcIdentityPassed: boolean
  readonly workerQueueBetterAuthIdentityPassed: boolean
  readonly workerQueueActorKeyRotationPassed: boolean
  readonly workerQueueSecretManagerPassed: boolean
  readonly workerQueueRateLimitPassed: boolean
  readonly workerQueueApprovalLifecyclePassed: boolean
  readonly workerQueueBreakGlassPassed: boolean
  readonly workerQueueIdentityAuditPassed: boolean
  readonly workerQueueMultiInstanceRateLimitPassed: boolean
  readonly workerQueueApprovalContentionPassed: boolean
  readonly workerQueueProcessTakeoverSoakPassed: boolean
  readonly workerQueueLiveIntegrationRequired: boolean
  readonly workerQueueLiveIdentityPassed: boolean
  readonly workerQueueLiveSecretManagerPassed: boolean
  readonly workerQueueCloudPolicyPassed: boolean
  readonly workerQueueCloudIntegrationRequired: boolean
  readonly workerQueueExternalOidcPassed: boolean
  readonly workerQueueRemoteSecretManagerPassed: boolean
  readonly workerQueueAdminPolicyPassed: boolean
  readonly workerQueueApprovalPassed: boolean
  readonly sessionProjectionShadowPassed: boolean
  readonly projectorDisabled: boolean
  readonly streamDisabled: boolean
  readonly sessionProjectionDisabled: boolean
}

export type GateResult = {
  readonly allowed: boolean
  readonly reasons: readonly string[]
}

export function evaluate(input: GateInput): GateResult {
  const reasons: string[] = []
  const schemaIssues = validateDraft()
  const postgresBackend = DatabaseBackend.isPostgres(input.backend)
  const projectorEnabled =
    input.postgresAlphaProjectorEnabled ??
    (input.backend.type === "postgres-alpha" ? input.backend.projectorEnabled : false)
  const streamEnabled =
    input.postgresAlphaStreamEnabled ?? (input.backend.type === "postgres-alpha" ? input.backend.streamEnabled : false)
  const sessionProjectionEnabled =
    input.postgresAlphaSessionProjectionEnabled ??
    (input.backend.type === "postgres-alpha" ? input.backend.sessionProjectionEnabled : false)
  if (schemaIssues.length > 0) reasons.push(`PostgreSQL RLS schema draft is missing: ${schemaIssues.join(", ")}`)
  if (input.saasRelease && !postgresBackend) reasons.push("SaaS release requires PostgreSQL backend")
  if (postgresBackend && input.backend.url === undefined) reasons.push("PostgreSQL backend requires OPENCODE_DATABASE_URL")
  if (input.saasRelease && input.backend.type === "postgres-alpha" && !input.backend.dualWriteEnabled) {
    reasons.push("SaaS PostgreSQL alpha release requires the transactional replication outbox")
  }
  if (postgresBackend && projectorEnabled) {
    reasons.push("PostgreSQL EventV2 facade alpha does not support projector integration")
  }
  if (postgresBackend && streamEnabled) {
    reasons.push("PostgreSQL EventV2 facade alpha does not support stream subscription")
  }
  if (postgresBackend && sessionProjectionEnabled) {
    reasons.push("PostgreSQL EventV2 facade alpha does not support session projection")
  }
  for (const workload of missingRequired(input.workloadResults)) reasons.push(`Required dual-run workload missing: ${workload.id}`)
  const rlsNegativeTestsPassed = input.proof?.rlsNegativeTestsPassed ?? input.rlsNegativeTestsPassed
  const appRoleSubjectToRls = input.proof?.appRoleSubjectToRls ?? input.appRoleSubjectToRls
  if (input.saasRelease && postgresBackend && input.proof === undefined) {
    reasons.push("SaaS release requires structured PostgreSQL release proof")
  }
  if (!rlsNegativeTestsPassed) reasons.push("PostgreSQL RLS negative tests have not passed")
  if (!appRoleSubjectToRls) reasons.push("Runtime app role is not proven subject to forced RLS")
  if (input.saasRelease && input.proof !== undefined) reasons.push(...postgresSaasReleaseProofIssues(input.proof))
  if (input.proof !== undefined) reasons.push(...postgresReleaseProofIssues(input.proof))
  return { allowed: reasons.length === 0, reasons }
}

export function postgresSaasReleaseProofIssues(proof: PostgresReleaseProof) {
  const reasons: string[] = []
  if (!proof.workerQueueLiveIntegrationRequired) {
    reasons.push("SaaS release requires the P4.41 live identity and Secret Manager integration gate")
  }
  if (!proof.workerQueueCloudIntegrationRequired) {
    reasons.push("SaaS release requires the P4.42 remote OIDC and AWS Secrets Manager integration gate")
  }
  return reasons
}

export function postgresReleaseProofIssues(proof: PostgresReleaseProof) {
  const reasons: string[] = []
  if (!proof.migrationsApplied) reasons.push("PostgreSQL migrations have not been applied")
  if (!proof.migrationAdvisoryLockPassed) reasons.push("PostgreSQL migration advisory lock has not passed")
  if (!proof.rlsReady) reasons.push("PostgreSQL RLS readiness has not passed")
  if (!proof.rlsNegativeTestsPassed) reasons.push("PostgreSQL RLS negative tests have not passed")
  if (!proof.appRoleSubjectToRls) reasons.push("Runtime app role is not proven subject to forced RLS")
  if (!proof.appRoleIsNotSuperuser) reasons.push("Runtime app role is superuser")
  if (!proof.appRoleHasNoBypassRls) reasons.push("Runtime app role has BYPASSRLS")
  if (!proof.eventStoreDualRunPassed) reasons.push("EventStore dual-run validation has not passed")
  if (!proof.eventV2FacadeContractPassed) reasons.push("EventV2 facade contract has not passed")
  if (!proof.workerEventAlignmentPassed) reasons.push("Worker event alignment has not passed")
  if (!proof.workerLeaseFencingPassed) reasons.push("PostgreSQL worker lease fencing has not passed")
  if (!proof.workerLeaseChaosPassed) reasons.push("PostgreSQL worker lease chaos validation has not passed")
  if (!proof.workerCoordinationAdapterPassed) reasons.push("Session worker coordination adapter has not passed")
  if (!proof.workerFencePropagationPassed) reasons.push("Worker fencing propagation has not passed")
  if (!proof.workerQueueAdapterPassed) reasons.push("PostgreSQL durable worker queue adapter has not passed")
  if (!proof.workerQueueRecoveryPassed) reasons.push("PostgreSQL worker queue recovery chaos validation has not passed")
  if (!proof.workerQueueOperationsPassed) reasons.push("PostgreSQL worker queue operations validation has not passed")
  if (!proof.workerQueueOperatorRlsPassed) reasons.push("PostgreSQL worker queue operator RLS validation has not passed")
  if (!proof.workerQueueTelemetryPassed) reasons.push("Worker queue OpenTelemetry/Prometheus validation has not passed")
  if (!proof.workerQueueOtlpExportPassed) reasons.push("Worker queue OTLP metrics export validation has not passed")
  if (!proof.workerQueuePrometheusProxyPassed) reasons.push("Worker queue Prometheus signing proxy validation has not passed")
  if (!proof.workerQueueOidcIdentityPassed) reasons.push("Worker queue OIDC identity validation has not passed")
  if (!proof.workerQueueBetterAuthIdentityPassed) reasons.push("Worker queue Better Auth identity validation has not passed")
  if (!proof.workerQueueActorKeyRotationPassed) reasons.push("Worker queue actor key rotation validation has not passed")
  if (!proof.workerQueueSecretManagerPassed) reasons.push("Worker queue Secret Manager validation has not passed")
  if (!proof.workerQueueRateLimitPassed) reasons.push("Worker queue distributed rate limit validation has not passed")
  if (!proof.workerQueueApprovalLifecyclePassed) reasons.push("Worker queue approval lifecycle validation has not passed")
  if (!proof.workerQueueBreakGlassPassed) reasons.push("Worker queue break-glass validation has not passed")
  if (!proof.workerQueueIdentityAuditPassed) reasons.push("Worker queue complete identity audit validation has not passed")
  if (!proof.workerQueueMultiInstanceRateLimitPassed) reasons.push("Worker queue multi-instance rate limit soak has not passed")
  if (!proof.workerQueueApprovalContentionPassed) reasons.push("Worker queue approval contention soak has not passed")
  if (!proof.workerQueueProcessTakeoverSoakPassed) reasons.push("Worker queue process takeover soak has not passed")
  if (proof.workerQueueLiveIntegrationRequired && !proof.workerQueueLiveIdentityPassed) {
    reasons.push("Worker queue live external identity integration has not passed")
  }
  if (proof.workerQueueLiveIntegrationRequired && !proof.workerQueueLiveSecretManagerPassed) {
    reasons.push("Worker queue live Secret Manager integration has not passed")
  }
  if (!proof.workerQueueCloudPolicyPassed) reasons.push("Worker queue cloud integration policy contract has not passed")
  if (proof.workerQueueCloudIntegrationRequired && !proof.workerQueueExternalOidcPassed) {
    reasons.push("Worker queue external OIDC integration has not passed")
  }
  if (proof.workerQueueCloudIntegrationRequired && !proof.workerQueueRemoteSecretManagerPassed) {
    reasons.push("Worker queue remote AWS Secrets Manager integration has not passed")
  }
  if (!proof.workerQueueAdminPolicyPassed) reasons.push("Worker queue management authorization policy has not passed")
  if (!proof.workerQueueApprovalPassed) reasons.push("Worker queue two-person approval validation has not passed")
  if (!proof.sessionProjectionShadowPassed) reasons.push("Session projection shadow validation has not passed")
  if (!proof.projectorDisabled) reasons.push("PostgreSQL alpha projector path must be disabled")
  if (!proof.streamDisabled) reasons.push("PostgreSQL alpha stream path must be disabled")
  if (!proof.sessionProjectionDisabled) reasons.push("PostgreSQL alpha session projection path must be disabled")
  return reasons
}

export const passingPostgresReleaseProof = {
  migrationsApplied: true,
  migrationAdvisoryLockPassed: true,
  rlsReady: true,
  rlsNegativeTestsPassed: true,
  appRoleSubjectToRls: true,
  appRoleIsNotSuperuser: true,
  appRoleHasNoBypassRls: true,
  eventStoreDualRunPassed: true,
  eventV2FacadeContractPassed: true,
  workerEventAlignmentPassed: true,
  workerLeaseFencingPassed: true,
  workerLeaseChaosPassed: true,
  workerCoordinationAdapterPassed: true,
  workerFencePropagationPassed: true,
  workerQueueAdapterPassed: true,
  workerQueueRecoveryPassed: true,
  workerQueueOperationsPassed: true,
  workerQueueOperatorRlsPassed: true,
  workerQueueTelemetryPassed: true,
  workerQueueOtlpExportPassed: true,
  workerQueuePrometheusProxyPassed: true,
  workerQueueOidcIdentityPassed: true,
  workerQueueBetterAuthIdentityPassed: true,
  workerQueueActorKeyRotationPassed: true,
  workerQueueSecretManagerPassed: true,
  workerQueueRateLimitPassed: true,
  workerQueueApprovalLifecyclePassed: true,
  workerQueueBreakGlassPassed: true,
  workerQueueIdentityAuditPassed: true,
  workerQueueMultiInstanceRateLimitPassed: true,
  workerQueueApprovalContentionPassed: true,
  workerQueueProcessTakeoverSoakPassed: true,
  workerQueueLiveIntegrationRequired: true,
  workerQueueLiveIdentityPassed: true,
  workerQueueLiveSecretManagerPassed: true,
  workerQueueCloudPolicyPassed: true,
  workerQueueCloudIntegrationRequired: true,
  workerQueueExternalOidcPassed: true,
  workerQueueRemoteSecretManagerPassed: true,
  workerQueueAdminPolicyPassed: true,
  workerQueueApprovalPassed: true,
  sessionProjectionShadowPassed: true,
  projectorDisabled: true,
  streamDisabled: true,
  sessionProjectionDisabled: true,
} satisfies PostgresReleaseProof

/**
 * P5.0 cloud acceptance switch.
 *
 * Cloud identity and Secret Manager proof is sealed by default while the
 * external KMS/OIDC rollout is deferred. SaaS startup remains fail-closed;
 * self-hosted and development flows remain governed by their existing gates.
 */
export const SaaSCloudAcceptance = {
  mode() {
    const value = process.env.OPENCODE_SAAS_CLOUD_ACCEPTANCE?.trim().toLowerCase()
    if (!value || value === "deferred") return "deferred" as const
    if (value === "required") return "required" as const
    return "invalid" as const
  },
  blocker() {
    const mode = SaaSCloudAcceptance.mode()
    if (mode === "required") return "saas-startup-cloud-proof-required"
    if (mode === "deferred") return "saas-cloud-acceptance-deferred"
    return "saas-cloud-acceptance-mode-invalid"
  },
  isRequired() {
    return SaaSCloudAcceptance.mode() === "required"
  },
}
