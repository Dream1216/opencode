import type { Sql } from "postgres"
import { applyMigrations, assertRlsReady } from "./migration"
import { makeClient } from "./client"
import { runRlsSmoke } from "./smoke-service"
import { runEventStoreDualRun } from "./event-store-dual-run"
import { runEventV2FacadeContract } from "./event-v2-facade-contract"
import { runWorkerEventAlignment } from "./worker-event-alignment"
import { runSessionProjectionDryRun } from "./session-projection-dry-run"
import { runWorkerLeaseChaos } from "./worker-lease-chaos"
import { runWorkerCoordinationSmoke } from "../../session/worker-coordination-smoke"
import { runWorkerQueueSmoke } from "../../session/worker-queue-smoke"
import { runWorkerJobChaos } from "./worker-job-chaos"
import { runWorkerQueueOperationsSmoke } from "./worker-queue-operations-smoke"
import { runWorkerQueueAdminSmoke } from "./worker-queue-admin-smoke"
import { runWorkerQueueTelemetryExportSmoke } from "../../observability/worker-queue-telemetry-export-smoke"
import { runWorkerQueueIdentitySmoke } from "./worker-queue-identity-smoke"
import { runWorkerQueueAdminGovernanceSmoke } from "./worker-queue-admin-governance-smoke"
import { runWorkerQueueMultiInstanceSoak } from "./worker-queue-multi-instance-soak"
import { runWorkerQueueLiveIntegration } from "./worker-queue-live-integration"
import { runWorkerQueueCloudContractSmoke } from "./worker-queue-cloud-contract-smoke"
import {
  createReleaseProofArtifact,
  databaseFingerprint,
  postgresSchemaHash,
  type ReleaseProofArtifact,
} from "./release-proof"

export type GenerateReleaseProofInput = {
  readonly sql: Sql
  readonly url: string
  readonly environment: string
  readonly buildID: string
  readonly key: string
  readonly ttlMs?: number
  readonly env?: NodeJS.ProcessEnv
}

export async function generateReleaseProof(input: GenerateReleaseProofInput): Promise<ReleaseProofArtifact> {
  const generatedAt = Date.now()
  const migration = await applyMigrations(input.sql)
  await assertRlsReady(input.sql)
  await verifyMigrationAdvisoryLock(input.url)
  const rls = await runRlsSmoke(input.sql)
  const dualRun = await runEventStoreDualRun(input.sql)
  const facade = await runEventV2FacadeContract(input.sql)
  const worker = await runWorkerEventAlignment(input.sql)
  const workerLease = await runWorkerLeaseChaos(input.sql, { url: input.url })
  const workerCoordination = await runWorkerCoordinationSmoke(input.sql, { url: input.url })
  const workerQueue = await runWorkerQueueSmoke(input.sql, { url: input.url })
  const workerJob = await runWorkerJobChaos(input.sql, { url: input.url })
  const workerQueueOperations = await runWorkerQueueOperationsSmoke(input.sql)
  const workerQueueAdmin = await runWorkerQueueAdminSmoke(input.sql, { url: input.url })
  const workerQueueTelemetryExport = await runWorkerQueueTelemetryExportSmoke()
  const workerQueueIdentity = await runWorkerQueueIdentitySmoke(input.sql, { url: input.url })
  const workerQueueAdminGovernance = await runWorkerQueueAdminGovernanceSmoke(input.sql, { url: input.url })
  const workerQueueMultiInstance = await runWorkerQueueMultiInstanceSoak(input.sql, {
    url: input.url,
    env: input.env,
  })
  const releaseEnv = input.env ?? process.env
  const cloudRequired = releaseEnv.OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED === "1"
  const liveRequired =
    releaseEnv.OPENCODE_P4_41_LIVE_INTEGRATION_REQUIRED === "1" ||
    cloudRequired
  const workerQueueCloudContract = await runWorkerQueueCloudContractSmoke()
  const workerQueueLiveIntegration = liveRequired
    ? await runWorkerQueueLiveIntegration(input.sql, { url: input.url, env: releaseEnv })
    : {
        status: "not-required" as const,
        checks: ["live-integration-not-required-for-this-proof"],
      }
  const shadow = await runSessionProjectionDryRun(input.sql)
  const identity = await runtimeIdentity(input.sql)

  return createReleaseProofArtifact(
    {
      environment: input.environment,
      buildID: input.buildID,
      databaseFingerprint: databaseFingerprint(input.url),
      databaseSchema: identity.schema,
      databaseRole: identity.role,
      schemaHash: postgresSchemaHash(),
      generatedAt,
      expiresAt: generatedAt + (input.ttlMs ?? 6 * 60 * 60 * 1000),
      proof: {
        migrationsApplied: true,
        migrationAdvisoryLockPassed: true,
        rlsReady: true,
        rlsNegativeTestsPassed: true,
        appRoleSubjectToRls: true,
        appRoleIsNotSuperuser: !identity.superuser,
        appRoleHasNoBypassRls: !identity.bypassRls,
        eventStoreDualRunPassed: dualRun.status === "ok",
        eventV2FacadeContractPassed: facade.status === "ok",
        workerEventAlignmentPassed: worker.status === "ok",
        workerLeaseFencingPassed: workerLease.status === "ok",
        workerLeaseChaosPassed: workerLease.status === "ok",
        workerCoordinationAdapterPassed: workerCoordination.status === "ok",
        workerFencePropagationPassed: workerCoordination.status === "ok",
        workerQueueAdapterPassed: workerQueue.status === "ok",
        workerQueueRecoveryPassed: workerJob.status === "ok",
        workerQueueOperationsPassed: workerQueueOperations.status === "ok",
        workerQueueOperatorRlsPassed: workerQueueOperations.status === "ok",
        workerQueueTelemetryPassed: workerQueueAdmin.status === "ok",
        workerQueueOtlpExportPassed: workerQueueTelemetryExport.status === "ok",
        workerQueuePrometheusProxyPassed: workerQueueTelemetryExport.status === "ok",
        workerQueueOidcIdentityPassed: workerQueueIdentity.status === "ok",
        workerQueueBetterAuthIdentityPassed: workerQueueIdentity.status === "ok",
        workerQueueActorKeyRotationPassed: workerQueueIdentity.status === "ok",
        workerQueueSecretManagerPassed: workerQueueIdentity.status === "ok",
        workerQueueRateLimitPassed: workerQueueAdminGovernance.status === "ok",
        workerQueueApprovalLifecyclePassed: workerQueueAdminGovernance.status === "ok",
        workerQueueBreakGlassPassed: workerQueueAdminGovernance.status === "ok",
        workerQueueIdentityAuditPassed: workerQueueAdminGovernance.status === "ok",
        workerQueueMultiInstanceRateLimitPassed: workerQueueMultiInstance.status === "ok",
        workerQueueApprovalContentionPassed: workerQueueMultiInstance.status === "ok",
        workerQueueProcessTakeoverSoakPassed: workerQueueMultiInstance.status === "ok",
        workerQueueLiveIntegrationRequired: liveRequired,
        workerQueueLiveIdentityPassed: workerQueueLiveIntegration.status === "ok",
        workerQueueLiveSecretManagerPassed: workerQueueLiveIntegration.status === "ok",
        workerQueueCloudPolicyPassed: workerQueueCloudContract.status === "ok",
        workerQueueCloudIntegrationRequired: cloudRequired,
        workerQueueExternalOidcPassed:
          workerQueueLiveIntegration.status === "ok" &&
          workerQueueLiveIntegration.provider === "oidc" &&
          workerQueueLiveIntegration.identityEndpointClass === "remote",
        workerQueueRemoteSecretManagerPassed:
          workerQueueLiveIntegration.status === "ok" &&
          workerQueueLiveIntegration.secretManagerScheme === "aws-sm",
        workerQueueAdminPolicyPassed: workerQueueAdmin.status === "ok",
        workerQueueApprovalPassed: workerQueueAdmin.status === "ok",
        sessionProjectionShadowPassed: shadow.status === "ok",
        projectorDisabled: true,
        streamDisabled: true,
        sessionProjectionDisabled: true,
      },
      evidence: {
        migrations: [
          ...migration.applied.map((id) => `applied:${id}`),
          ...migration.skipped.map((id) => `present:${id}`),
        ],
        rls: rls.checks,
        eventStoreDualRun: dualRun.checks,
        eventV2Facade: facade.checks,
        workerAlignment: worker.checks,
        workerLeaseFencing: workerLease.checks,
        workerCoordination: workerCoordination.checks,
        workerQueue: workerQueue.checks,
        workerQueueRecovery: workerJob.checks,
        workerQueueOperations: workerQueueOperations.checks,
        workerQueueAdmin: workerQueueAdmin.checks,
        workerQueueTelemetryExport: workerQueueTelemetryExport.checks,
        workerQueueIdentity: workerQueueIdentity.checks,
        workerQueueAdminGovernance: workerQueueAdminGovernance.checks,
        workerQueueMultiInstance: workerQueueMultiInstance.checks,
        workerQueueLiveIntegration: workerQueueLiveIntegration.checks,
        workerQueueCloudContract: workerQueueCloudContract.checks,
        sessionProjectionShadow: shadow.checks,
      },
    },
    input.key,
  )
}

async function verifyMigrationAdvisoryLock(url: string) {
  const clients = [makeClient({ url, max: 1 }), makeClient({ url, max: 1 })]
  try {
    await Promise.all(clients.map((sql) => applyMigrations(sql)))
  } finally {
    await Promise.all(clients.map((sql) => sql.end({ timeout: 5 })))
  }
}

async function runtimeIdentity(sql: Sql) {
  const rows = await sql<
    { role: string; schema: string | null; superuser: boolean; bypass_rls: boolean }[]
  >`
    select
      current_user as role,
      current_schema() as schema,
      r.rolsuper as superuser,
      r.rolbypassrls as bypass_rls
    from pg_roles r
    where r.rolname = current_user
  `
  const row = rows[0]
  if (row === undefined) throw new Error("Unable to inspect PostgreSQL release proof runtime identity")
  if (row.superuser || row.bypass_rls) throw new Error("PostgreSQL release proof requires a runtime role subject to RLS")
  return { role: row.role, schema: row.schema ?? "public", superuser: row.superuser, bypassRls: row.bypass_rls }
}
