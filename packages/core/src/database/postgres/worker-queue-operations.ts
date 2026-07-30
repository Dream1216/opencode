import type { Sql } from "postgres"
import type { TenantContext } from "./client"
import { assertRlsReady } from "./migration"
import {
  listRecoverableWorkerJobs,
  readWorkerQueueMetrics,
  requeueWorkerJob,
  type WorkerJob,
  type WorkerQueueMetrics,
} from "./worker-job"

export type WorkerQueueReadiness = {
  readonly ready: true
  readonly degraded: boolean
  readonly reasons: readonly string[]
  readonly metrics: WorkerQueueMetrics
}

export async function readWorkerQueueReadiness(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly maxPendingAgeMs?: number
  },
): Promise<WorkerQueueReadiness> {
  await assertRlsReady(sql)
  const metrics = await readWorkerQueueMetrics(sql, input.tenant)
  const reasons: string[] = []
  if (metrics.failed > 0) reasons.push(`${metrics.failed} worker jobs are failed`)
  if (metrics.expiredRunning > 0) reasons.push(`${metrics.expiredRunning} worker job claims are expired`)
  if (metrics.oldestPendingAgeMs > (input.maxPendingAgeMs ?? 60_000)) {
    reasons.push(`oldest pending worker job is ${metrics.oldestPendingAgeMs}ms old`)
  }
  return {
    ready: true,
    degraded: reasons.length > 0,
    reasons,
    metrics,
  }
}

export async function listWorkerQueueRecoverable(
  sql: Sql,
  input: {
    readonly tenant: TenantContext
    readonly limit?: number
  },
): Promise<readonly WorkerJob[]> {
  await assertRlsReady(sql)
  return await listRecoverableWorkerJobs(sql, input)
}

export async function operatorRequeueWorkerJob(
  sql: Sql,
  input: {
    readonly tenant: TenantContext & { readonly actorID: string }
    readonly runID: string
    readonly expectedGeneration: number
    readonly expectedClaimToken: number
    readonly requestID?: string
  },
) {
  await assertRlsReady(sql)
  return await requeueWorkerJob(sql, input)
}
