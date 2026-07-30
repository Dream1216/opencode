import type { Sql } from "postgres"
import { fileURLToPath } from "node:url"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  acquireWorkerLease,
  cleanupWorkerRun,
  commitWorkerEffect,
  completeWorkerLease,
  countWorkerEffects,
  heartbeatWorkerLease,
  readWorkerLease,
  releaseWorkerLease,
  WorkerFenceRejectedError,
  type WorkerFence,
} from "./worker-lease"
import type { TenantContext } from "./client"

export type WorkerLeaseChaosResult = {
  readonly status: "ok"
  readonly checks: readonly string[]
  readonly firstToken: number
  readonly takeoverToken: number
  readonly effectCount: number
}

type ChildResult = {
  readonly acquired: boolean
  readonly ownerID: string
  readonly fencingToken: number
  readonly leaseExpiresAt: number
  readonly effectCommitted?: boolean
}

const childScript = fileURLToPath(new URL("../../../script/postgres-worker-lease-child.ts", import.meta.url))

export async function runWorkerLeaseChaos(sql: Sql, input: { readonly url: string }): Promise<WorkerLeaseChaosResult> {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const checks: string[] = []
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenant = {
    tenantID: `tenant_worker_lease_${suffix}`,
    actorID: `actor_worker_lease_${suffix}`,
  } satisfies TenantContext
  const otherTenant = {
    tenantID: `tenant_worker_lease_other_${suffix}`,
    actorID: `actor_worker_lease_other_${suffix}`,
  } satisfies TenantContext
  const contractRun = `ses_worker_lease_contract_${suffix}`
  const chaosRun = `ses_worker_lease_chaos_${suffix}`
  await seedTenant(sql, tenant)
  await seedTenant(sql, otherTenant)
  try {
    await runLeaseContract(sql, tenant, otherTenant, contractRun, checks)
    const first = await startChild({
      url: input.url,
      mode: "hold",
      tenant,
      runID: chaosRun,
      ownerID: "worker-chaos-first",
      leaseMs: 1_200,
    })
    if (!first.result.acquired) throw new Error("First chaos worker did not acquire the lease")
    checks.push("first-process-acquired")

    const blocked = await runChild({
      url: input.url,
      mode: "once",
      tenant,
      runID: chaosRun,
      ownerID: "worker-chaos-before-expiry",
      leaseMs: 5_000,
    })
    if (blocked.acquired) throw new Error("Second process acquired a live lease before expiry")
    checks.push("live-lease-contender-blocked")

    first.process.kill(9)
    await first.process.exited
    checks.push("lease-owner-killed")

    await Bun.sleep(Math.max(0, first.result.leaseExpiresAt - Date.now() + 250))
    const contenders = await Promise.all([
      runChild({
        url: input.url,
        mode: "once",
        tenant,
        runID: chaosRun,
        ownerID: "worker-chaos-takeover-a",
        leaseMs: 5_000,
        effectKey: "external-effect",
      }),
      runChild({
        url: input.url,
        mode: "once",
        tenant,
        runID: chaosRun,
        ownerID: "worker-chaos-takeover-b",
        leaseMs: 5_000,
        effectKey: "external-effect",
      }),
    ])
    const winners = contenders.filter((item) => item.acquired)
    if (winners.length !== 1) throw new Error(`Expected one takeover winner, got ${winners.length}`)
    const winner = winners[0]!
    if (winner.fencingToken <= first.result.fencingToken) throw new Error("Takeover fencing token did not increase")
    if (winner.effectCommitted !== true) throw new Error("Takeover winner did not commit the guarded effect")
    checks.push("expired-lease-single-winner-takeover")
    checks.push("fencing-token-incremented")

    await expectFenceRejected(
      commitWorkerEffect(sql, {
        tenant,
        runID: chaosRun,
        ownerID: first.result.ownerID,
        fencingToken: first.result.fencingToken,
        effectKey: "stale-effect",
        payload: { source: "stale-owner" },
      }),
    )
    checks.push("stale-owner-effect-rejected")

    const duplicate = await commitWorkerEffect(sql, {
      tenant,
      runID: chaosRun,
      ownerID: winner.ownerID,
      fencingToken: winner.fencingToken,
      effectKey: "external-effect",
      payload: { source: "chaos-winner" },
    })
    if (duplicate.committed) throw new Error("Duplicate worker effect was committed twice")
    const effectCount = await countWorkerEffects(sql, tenant, chaosRun)
    if (effectCount !== 1) throw new Error(`Expected one worker effect row, got ${effectCount}`)
    checks.push("duplicate-side-effect-idempotent")
    checks.push("single-effect-row-verified")

    return {
      status: "ok",
      checks,
      firstToken: first.result.fencingToken,
      takeoverToken: winner.fencingToken,
      effectCount,
    }
  } finally {
    await cleanupWorkerRun(sql, tenant, contractRun)
    await cleanupWorkerRun(sql, tenant, chaosRun)
    await sql`delete from tenant where id in (${tenant.tenantID}, ${otherTenant.tenantID})`
  }
}

async function runLeaseContract(
  sql: Sql,
  tenant: TenantContext,
  otherTenant: TenantContext,
  runID: string,
  checks: string[],
) {
  const acquired = await acquireWorkerLease(sql, { tenant, runID, ownerID: "contract-owner-a", leaseMs: 5_000 })
  if (!acquired.acquired || acquired.lease.fencingToken !== 1) throw new Error("Initial worker lease acquisition failed")
  const fence = toFence(tenant, acquired.lease)
  const heartbeat = await heartbeatWorkerLease(sql, fence, 5_000)
  if (heartbeat.leaseExpiresAt <= acquired.lease.leaseExpiresAt) throw new Error("Worker heartbeat did not extend lease")
  checks.push("acquire-heartbeat-ready")

  const contender = await acquireWorkerLease(sql, {
    tenant,
    runID,
    ownerID: "contract-owner-b",
    leaseMs: 5_000,
  })
  if (contender.acquired) throw new Error("Contender acquired a live contract lease")
  await expectFenceRejected(
    heartbeatWorkerLease(sql, { ...fence, ownerID: "wrong-owner" }, 5_000),
  )
  checks.push("wrong-owner-fenced")

  if ((await readWorkerLease(sql, otherTenant, runID)) !== undefined) {
    throw new Error("Other tenant read a worker lease through RLS")
  }
  checks.push("worker-lease-rls-isolated")

  await releaseWorkerLease(sql, fence)
  const takeover = await acquireWorkerLease(sql, { tenant, runID, ownerID: "contract-owner-b", leaseMs: 5_000 })
  if (!takeover.acquired || takeover.lease.fencingToken !== 2) throw new Error("Released lease takeover failed")
  const takeoverFence = toFence(tenant, takeover.lease)
  const firstEffect = await commitWorkerEffect(sql, {
    ...takeoverFence,
    effectKey: "contract-effect",
    payload: { action: "contract" },
  })
  const duplicateEffect = await commitWorkerEffect(sql, {
    ...takeoverFence,
    effectKey: "contract-effect",
    payload: { action: "contract" },
  })
  if (!firstEffect.committed || duplicateEffect.committed) throw new Error("Worker effect idempotency contract failed")
  await completeWorkerLease(sql, takeoverFence)
  checks.push("release-takeover-complete-ready")
  checks.push("effect-idempotency-ready")
}

async function seedTenant(sql: Sql, tenant: TenantContext) {
  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values (${tenant.tenantID}, ${tenant.tenantID}, ${Date.now()}, ${Date.now()})
    on conflict (id) do nothing
  `
}

function toFence(tenant: TenantContext, lease: { runID: string; ownerID: string; fencingToken: number }): WorkerFence {
  return {
    tenant,
    runID: lease.runID,
    ownerID: lease.ownerID,
    fencingToken: lease.fencingToken,
  }
}

async function expectFenceRejected(promise: Promise<unknown>) {
  try {
    await promise
  } catch (error) {
    if (error instanceof WorkerFenceRejectedError) return
    throw error
  }
  throw new Error("Expected stale worker fence to be rejected")
}

async function startChild(input: {
  readonly url: string
  readonly mode: "hold" | "once"
  readonly tenant: TenantContext
  readonly runID: string
  readonly ownerID: string
  readonly leaseMs: number
  readonly effectKey?: string
}) {
  const process = Bun.spawn({
    cmd: [globalThis.process.execPath, childScript],
    env: {
      ...globalThis.process.env,
      OPENCODE_DATABASE_URL: input.url,
      OPENCODE_WORKER_LEASE_CHILD_MODE: input.mode,
      OPENCODE_TENANT_ID: input.tenant.tenantID,
      OPENCODE_ACTOR_ID: input.tenant.actorID ?? "",
      OPENCODE_WORKER_LEASE_RUN_ID: input.runID,
      OPENCODE_WORKER_LEASE_OWNER_ID: input.ownerID,
      OPENCODE_WORKER_LEASE_MS: String(input.leaseMs),
      ...(input.effectKey === undefined ? {} : { OPENCODE_WORKER_EFFECT_KEY: input.effectKey }),
    },
    stdout: "pipe",
    stderr: "pipe",
  })
  if (!(process.stdout instanceof ReadableStream)) throw new Error("Worker lease child stdout pipe is unavailable")
  const result = await readChildResult(process, process.stdout)
  return { process, result }
}

async function runChild(input: Parameters<typeof startChild>[0]) {
  const child = await startChild(input)
  const exit = await child.process.exited
  if (exit !== 0) {
    const error = await new Response(child.process.stderr).text()
    throw new Error(`Worker lease child failed with exit ${exit}: ${error}`)
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
      if (chunk.done) throw new Error("Worker lease child exited before reporting acquisition")
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
