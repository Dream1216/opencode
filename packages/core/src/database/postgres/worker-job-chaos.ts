import type { Sql } from "postgres"
import { fileURLToPath } from "node:url"
import type { TenantContext } from "./client"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  claimNextWorkerJob,
  cleanupWorkerJob,
  completeWorkerJobClaim,
  enqueueWorkerJob,
  readWorkerJob,
  WorkerJobClaimRejectedError,
  type WorkerJobClaim,
} from "./worker-job"

export type WorkerJobChaosResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
  readonly firstToken: number
  readonly takeoverToken: number
  readonly finalToken: number
  readonly completedGeneration: number
}

type ChildResult =
  | { readonly claimed: false }
  | ({
      readonly claimed: true
    } & WorkerJobClaim)

const childScript = fileURLToPath(new URL("../../../script/postgres-worker-job-child.ts", import.meta.url))

export async function runWorkerJobChaos(sql: Sql, input: { readonly url: string }): Promise<WorkerJobChaosResult> {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenant = {
    tenantID: `tenant_worker_job_${suffix}`,
    actorID: `actor_worker_job_${suffix}`,
  } satisfies TenantContext
  const otherTenant = {
    tenantID: `tenant_worker_job_other_${suffix}`,
    actorID: `actor_worker_job_other_${suffix}`,
  } satisfies TenantContext
  const runID = `ses_worker_job_chaos_${suffix}`
  const checks: string[] = []
  try {
    const generation = await enqueueWorkerJob(sql, { tenant, runID, reason: "wake" })
    if (generation.requestedGeneration !== 1) throw new Error("Initial worker job generation is not one")
    checks.push("durable-job-enqueued")

    const first = await startChild({
      url: input.url,
      mode: "hold",
      tenant,
      runID,
      ownerID: "worker-job-first",
      claimMs: 5_000,
    })
    if (!first.result.claimed) throw new Error("First worker process did not claim the durable job")
    const firstClaim = first.result
    checks.push("first-process-claimed")

    const blocked = await runChild({
      url: input.url,
      mode: "once",
      tenant,
      runID,
      ownerID: "worker-job-before-expiry",
      claimMs: 5_000,
    })
    if (blocked.claimed) throw new Error("Contender claimed a live worker job")
    checks.push("live-job-contender-blocked")

    first.process.kill(9)
    await first.process.exited
    checks.push("job-owner-killed")
    await Bun.sleep(Math.max(0, firstClaim.claimExpiresAt - Date.now() + 250))

    const contenders = await Promise.all([
      runChild({
        url: input.url,
        mode: "once",
        tenant,
        runID,
        ownerID: "worker-job-takeover-a",
        claimMs: 5_000,
      }),
      runChild({
        url: input.url,
        mode: "once",
        tenant,
        runID,
        ownerID: "worker-job-takeover-b",
        claimMs: 5_000,
      }),
    ])
    const winners = contenders.filter((item): item is Extract<ChildResult, { claimed: true }> => item.claimed)
    if (winners.length !== 1) throw new Error(`Expected one worker job takeover winner, got ${winners.length}`)
    const winner = winners[0]!
    if (winner.claimToken <= firstClaim.claimToken) throw new Error("Worker job claim token did not increase")
    if (winner.reason !== "recovery") throw new Error("Expired worker job takeover was not marked as recovery")
    checks.push("expired-job-single-winner-takeover")
    checks.push("claim-token-incremented")
    checks.push("restart-recovery-marked")

    const second = await enqueueWorkerJob(sql, { tenant, runID, reason: "resume" })
    if (second.requestedGeneration !== 2) throw new Error("Running worker job did not increment generation")
    checks.push("running-job-generation-incremented")

    await expectClaimRejected(completeWorkerJobClaim(sql, firstClaim))
    checks.push("stale-claim-completion-rejected")
    const afterTakeover = await completeWorkerJobClaim(sql, winner)
    if (afterTakeover.status !== "pending" || afterTakeover.completedGeneration !== 1) {
      throw new Error("Worker job did not requeue the generation requested during execution")
    }
    checks.push("new-generation-requeued-after-complete")

    const final = await claimNextWorkerJob(sql, {
      tenant,
      runID,
      ownerID: "worker-job-final",
      claimMs: 5_000,
    })
    if (final === undefined || final.claimedGeneration !== 2 || final.claimToken <= winner.claimToken) {
      throw new Error("Final worker job generation was not claimed with a new token")
    }
    const completed = await completeWorkerJobClaim(sql, final)
    if (completed.status !== "completed" || completed.completedGeneration !== 2) {
      throw new Error("Final worker job generation did not complete")
    }
    checks.push("pending-generation-completed-once")

    if ((await readWorkerJob(sql, otherTenant, runID)) !== undefined) {
      throw new Error("Other tenant read a worker job through forced RLS")
    }
    checks.push("worker-job-rls-isolated")
    return {
      status: "ok",
      checks,
      firstToken: firstClaim.claimToken,
      takeoverToken: winner.claimToken,
      finalToken: final.claimToken,
      completedGeneration: completed.completedGeneration,
    }
  } finally {
    await cleanupWorkerJob(sql, tenant, runID)
  }
}

async function startChild(input: {
  readonly url: string
  readonly mode: "hold" | "once"
  readonly tenant: TenantContext
  readonly runID: string
  readonly ownerID: string
  readonly claimMs: number
}) {
  const process = Bun.spawn({
    cmd: [globalThis.process.execPath, childScript],
    env: {
      ...globalThis.process.env,
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_WORKER_JOB_CHILD_MODE: input.mode,
      OPENCODE_TENANT_ID: input.tenant.tenantID,
      OPENCODE_ACTOR_ID: input.tenant.actorID ?? "",
      OPENCODE_WORKER_JOB_RUN_ID: input.runID,
      OPENCODE_WORKER_JOB_OWNER_ID: input.ownerID,
      OPENCODE_WORKER_JOB_CLAIM_MS: String(input.claimMs),
    },
    stdout: "pipe",
    stderr: "pipe",
  })
  if (!(process.stdout instanceof ReadableStream)) throw new Error("Worker job child stdout pipe is unavailable")
  const result = await readChildResult(process, process.stdout)
  return { process, result }
}

async function runChild(input: Parameters<typeof startChild>[0]) {
  const child = await startChild(input)
  const exit = await child.process.exited
  if (exit !== 0) {
    const error = await new Response(child.process.stderr).text()
    throw new Error(`Worker job child failed with exit ${exit}: ${error}`)
  }
  return child.result
}

async function readChildResult(
  process: ReturnType<typeof Bun.spawn>,
  stdout: ReadableStream<Uint8Array>,
): Promise<ChildResult> {
  const reader = stdout.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  const timeout = setTimeout(() => process.kill(9), 15_000)
  try {
    while (true) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error("Worker job child exited before reporting its claim")
      buffer += decoder.decode(chunk.value, { stream: true })
      const newline = buffer.indexOf("\n")
      if (newline < 0) continue
      return JSON.parse(buffer.slice(0, newline)) as ChildResult
    }
  } finally {
    clearTimeout(timeout)
    reader.releaseLock()
  }
}

async function expectClaimRejected(promise: Promise<unknown>) {
  try {
    await promise
  } catch (error) {
    if (error instanceof WorkerJobClaimRejectedError) return
    throw error
  }
  throw new Error("Expected stale worker job claim to be rejected")
}
