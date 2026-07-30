import type { Sql } from "postgres"
import { setTenantContext, type TenantContext } from "./client"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  claimNextWorkerJob,
  cleanupWorkerJob,
  completeWorkerJobClaim,
  enqueueWorkerJob,
  failWorkerJobClaim,
  WorkerJobRequeueRejectedError,
} from "./worker-job"
import {
  listWorkerQueueRecoverable,
  operatorRequeueWorkerJob,
  readWorkerQueueReadiness,
} from "./worker-queue-operations"

export async function runWorkerQueueOperationsSmoke(sql: Sql) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenant = {
    tenantID: `tenant_worker_queue_ops_${suffix}`,
    actorID: `actor_worker_queue_ops_${suffix}`,
  } satisfies TenantContext & { actorID: string }
  const otherTenant = {
    tenantID: `tenant_worker_queue_ops_other_${suffix}`,
    actorID: `actor_worker_queue_ops_other_${suffix}`,
  } satisfies TenantContext & { actorID: string }
  const runID = `ses_worker_queue_ops_${suffix}`
  const checks: string[] = []
  try {
    await enqueueWorkerJob(sql, { tenant, runID, reason: "wake" })
    const claim = await claimNextWorkerJob(sql, {
      tenant,
      runID,
      ownerID: "worker-queue-ops-failing",
      claimMs: 5_000,
    })
    if (claim === undefined) throw new Error("Worker queue operations fixture was not claimed")
    const failed = await failWorkerJobClaim(sql, {
      claim,
      error: "operator recovery fixture",
      maxAttempts: 1,
      retryDelayMs: 0,
    })
    if (failed.status !== "failed") throw new Error("Worker job did not enter failed state")
    const degraded = await readWorkerQueueReadiness(sql, { tenant })
    if (!degraded.degraded || degraded.metrics.failed !== 1) {
      throw new Error("Worker queue readiness did not expose the failed job")
    }
    checks.push("failed-job-readiness-degraded")

    const recoverable = await listWorkerQueueRecoverable(sql, { tenant })
    if (recoverable.length !== 1 || recoverable[0]?.runID !== runID) {
      throw new Error("Recoverable worker job listing did not return the failed job")
    }
    checks.push("failed-job-listed")

    const otherReadiness = await readWorkerQueueReadiness(sql, { tenant: otherTenant })
    const otherRecoverable = await listWorkerQueueRecoverable(sql, { tenant: otherTenant })
    if (
      otherReadiness.metrics.failed !== 0 ||
      otherReadiness.metrics.pending !== 0 ||
      otherReadiness.metrics.running !== 0 ||
      otherRecoverable.length !== 0
    ) {
      throw new Error("Other tenant observed worker queue operational state")
    }
    await expectRequeueRejected(
      operatorRequeueWorkerJob(sql, {
        tenant: otherTenant,
        runID,
        expectedGeneration: failed.requestedGeneration,
        expectedClaimToken: failed.claimToken,
      }),
    )
    checks.push("queue-operations-rls-isolated")

    await expectRequeueRejected(
      operatorRequeueWorkerJob(sql, {
        tenant,
        runID,
        expectedGeneration: failed.requestedGeneration,
        expectedClaimToken: failed.claimToken - 1,
      }),
    )
    checks.push("stale-operator-token-rejected")

    const attempts = await Promise.allSettled([
      operatorRequeueWorkerJob(sql, {
        tenant,
        runID,
        expectedGeneration: failed.requestedGeneration,
        expectedClaimToken: failed.claimToken,
        requestID: `request-a-${suffix}`,
      }),
      operatorRequeueWorkerJob(sql, {
        tenant,
        runID,
        expectedGeneration: failed.requestedGeneration,
        expectedClaimToken: failed.claimToken,
        requestID: `request-b-${suffix}`,
      }),
    ])
    const winners = attempts.filter(
      (result): result is PromiseFulfilledResult<Awaited<ReturnType<typeof operatorRequeueWorkerJob>>> =>
        result.status === "fulfilled",
    )
    if (winners.length !== 1) throw new Error(`Expected one operator requeue winner, got ${winners.length}`)
    const requeued = winners[0]!.value
    if (requeued.status !== "pending" || requeued.requestedGeneration !== failed.requestedGeneration + 1) {
      throw new Error("Operator requeue did not create the next pending generation")
    }
    checks.push("concurrent-operator-requeue-single-winner")

    const auditCount = await countRequeueAudit(sql, tenant, runID)
    if (auditCount !== 1) throw new Error(`Expected one atomic requeue audit row, got ${auditCount}`)
    checks.push("operator-requeue-audited-atomically")

    const recovered = await claimNextWorkerJob(sql, {
      tenant,
      runID,
      ownerID: "worker-queue-ops-recovered",
      claimMs: 5_000,
    })
    if (
      recovered === undefined ||
      recovered.reason !== "recovery" ||
      recovered.claimedGeneration !== requeued.requestedGeneration
    ) {
      throw new Error("Operator-requeued generation was not recovered")
    }
    await completeWorkerJobClaim(sql, recovered)
    const ready = await readWorkerQueueReadiness(sql, { tenant })
    if (ready.degraded || ready.metrics.completed !== 1 || ready.metrics.failed !== 0) {
      throw new Error("Worker queue readiness did not recover after completion")
    }
    checks.push("failed-requeue-completed")
    checks.push("queue-readiness-recovered")
    return { status: "ok" as const, checks }
  } finally {
    await cleanupWorkerJob(sql, tenant, runID)
  }
}

async function countRequeueAudit(sql: Sql, tenant: TenantContext, runID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, tenant)
    const rows = await tx<{ count: string | number }[]>`
      select count(*) as count
      from audit_event
      where action = 'worker_job.requeue'
        and resource_type = 'worker_job'
        and resource_id = ${runID}
    `
    return Number(rows[0]?.count ?? 0)
  })
}

async function expectRequeueRejected(promise: Promise<unknown>) {
  try {
    await promise
  } catch (error) {
    if (error instanceof WorkerJobRequeueRejectedError) return
    throw error
  }
  throw new Error("Expected worker job operator requeue to be rejected")
}
