import type { Sql } from "postgres"
import { applyMigrations, assertRlsReady } from "./migration"
import {
  makePostgresWorkspaceRecoveryOwnershipStore,
  type WorkspaceRecoveryOwnershipAcquireResult,
} from "./workspace-recovery-ownership"

export async function runWorkspaceRecoveryOwnershipSmoke(
  sql: Sql,
  input: { readonly url: string },
) {
  await applyMigrations(sql)
  await assertRlsReady(sql)
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  const tenant = { tenantID: `tenant_workspace_owner_${suffix}` }
  const otherTenant = { tenantID: `tenant_workspace_owner_other_${suffix}` }
  const workspaceID = `workspace-${suffix}`
  const workspaceDirectory = `/tmp/opencode-workspace-owner-${suffix}`
  const leaseMs = 500
  const first = makePostgresWorkspaceRecoveryOwnershipStore({ url: input.url, max: 1 })
  const second = makePostgresWorkspaceRecoveryOwnershipStore({ url: input.url, max: 1 })
  const third = makePostgresWorkspaceRecoveryOwnershipStore({ url: input.url, max: 1 })
  const checks: string[] = []

  await sql`
    insert into tenant (id, name, time_created, time_updated)
    values
      (${tenant.tenantID}, ${tenant.tenantID}, ${Date.now()}, ${Date.now()}),
      (${otherTenant.tenantID}, ${otherTenant.tenantID}, ${Date.now()}, ${Date.now()})
  `
  try {
    const initial = await first.acquire({
      tenant,
      workspaceID,
      workspaceDirectory,
      ownerID: "worker-a",
      leaseMs,
    })
    if (!initial.acquired || initial.disposition !== "acquired" || initial.lease.epoch !== 1) {
      throw new Error("initial workspace owner acquisition failed")
    }
    checks.push("initial-owner-acquired")

    const blocked = await second.acquire({
      tenant,
      workspaceID,
      workspaceDirectory,
      ownerID: "worker-b",
      leaseMs,
    })
    if (blocked.acquired || blocked.current.ownerID !== "worker-a") {
      throw new Error("live workspace owner did not block a contender")
    }
    checks.push("live-owner-contender-blocked")

    const renewed = await first.renew(initial.lease, leaseMs)
    if (renewed === undefined || renewed.epoch !== 1) {
      throw new Error("workspace owner heartbeat changed the epoch")
    }
    checks.push("owner-heartbeat-renewed")
    await Bun.sleep(Math.max(0, renewed.leaseExpiresAt - Date.now() + 100))

    const contenders = await Promise.all([
      second.acquire({
        tenant,
        workspaceID,
        workspaceDirectory,
        ownerID: "worker-b",
        leaseMs,
      }),
      third.acquire({
        tenant,
        workspaceID,
        workspaceDirectory,
        ownerID: "worker-c",
        leaseMs,
      }),
    ])
    const winners = contenders.filter(
      (result): result is Extract<WorkspaceRecoveryOwnershipAcquireResult, { acquired: true }> =>
        result.acquired,
    )
    if (winners.length !== 1 || winners[0]!.disposition !== "takeover" || winners[0]!.lease.epoch !== 2) {
      throw new Error(`expected one epoch-two takeover winner, got ${winners.length}`)
    }
    const winner = winners[0]!
    checks.push("expired-owner-single-winner-takeover")
    checks.push("ownership-epoch-incremented")

    if ((await first.renew(initial.lease, leaseMs)) !== undefined) {
      throw new Error("stale workspace owner heartbeat succeeded")
    }
    if (await first.release(initial.lease)) {
      throw new Error("stale workspace owner release succeeded")
    }
    checks.push("stale-owner-fenced")

    const isolated = await first.acquire({
      tenant: otherTenant,
      workspaceID,
      workspaceDirectory,
      ownerID: "worker-other-tenant",
      leaseMs,
    })
    if (!isolated.acquired || isolated.lease.epoch !== 1) {
      throw new Error("other tenant could not independently own the same workspace identifier")
    }
    const tenantView = await first.read(tenant, workspaceID)
    const otherTenantView = await first.read(otherTenant, workspaceID)
    if (
      tenantView?.ownerID !== winner.lease.ownerID ||
      otherTenantView?.ownerID !== "worker-other-tenant"
    ) {
      throw new Error("tenant-scoped workspace ownership read crossed the RLS boundary")
    }
    checks.push("workspace-owner-rls-isolated")

    const winnerStore = winner.lease.ownerID === "worker-b" ? second : third
    if (!(await winnerStore.release(winner.lease))) {
      throw new Error("takeover winner could not release workspace ownership")
    }
    const reacquired = await first.acquire({
      tenant,
      workspaceID,
      workspaceDirectory,
      ownerID: "worker-a",
      leaseMs,
    })
    if (!reacquired.acquired || reacquired.disposition !== "takeover" || reacquired.lease.epoch !== 3) {
      throw new Error("released workspace ownership did not advance the epoch")
    }
    checks.push("released-owner-reacquired-with-new-epoch")
    return {
      status: "ok" as const,
      checks,
      initialEpoch: initial.lease.epoch,
      takeoverEpoch: winner.lease.epoch,
      reacquiredEpoch: reacquired.lease.epoch,
      takeoverOwnerID: winner.lease.ownerID,
    }
  } finally {
    await Promise.allSettled([
      first.destroy(tenant, workspaceID),
      first.destroy(otherTenant, workspaceID),
    ])
    await Promise.allSettled([first.close(), second.close(), third.close()])
    await sql`delete from tenant where id in (${tenant.tenantID}, ${otherTenant.tenantID})`
  }
}
