import { afterEach, describe, expect, test } from "bun:test"
import {
  makeMemoryWorkspaceRecoveryOwnershipStore,
  type WorkspaceRecoveryOwnershipAcquireResult,
} from "../src/database/postgres/workspace-recovery-ownership"
import {
  observeWorkerQueueRecoveryOwnership,
  recordWorkerQueueRecoveryOwnershipAttempt,
  renderWorkerQueueRecoveryPrometheus,
  resetWorkerQueueRecoveryTelemetry,
} from "../src/database/postgres/worker-queue-telemetry"

afterEach(() => resetWorkerQueueRecoveryTelemetry())

describe("WorkspaceRecoveryOwnership", () => {
  test("elects one owner and fences an expired owner with an increasing epoch", async () => {
    let now = 1_000
    const store = makeMemoryWorkspaceRecoveryOwnershipStore({ now: () => now })
    const target = {
      tenant: { tenantID: "tenant-a" },
      workspaceID: "workspace-a",
      workspaceDirectory: "/srv/workspaces/a",
      leaseMs: 500,
    }
    const first = await store.acquire({ ...target, ownerID: "worker-a" })
    expect(first).toMatchObject({
      acquired: true,
      disposition: "acquired",
      lease: { ownerID: "worker-a", epoch: 1 },
    })
    if (!first.acquired) throw new Error("first owner was not acquired")

    expect(await store.acquire({ ...target, ownerID: "worker-b" })).toMatchObject({
      acquired: false,
      disposition: "contended",
      current: { ownerID: "worker-a", epoch: 1 },
    })
    expect(await store.renew(first.lease, 500)).toMatchObject({ epoch: 1 })

    now = 1_501
    const contenders = await Promise.all([
      store.acquire({ ...target, ownerID: "worker-b" }),
      store.acquire({ ...target, ownerID: "worker-c" }),
    ])
    const winners = contenders.filter(
      (result): result is Extract<WorkspaceRecoveryOwnershipAcquireResult, { acquired: true }> =>
        result.acquired,
    )
    expect(winners).toHaveLength(1)
    expect(winners[0]).toMatchObject({
      disposition: "takeover",
      lease: { epoch: 2 },
    })
    expect(await store.renew(first.lease, 500)).toBeUndefined()
    expect(await store.release(first.lease)).toBe(false)
    await store.close()
  })

  test("isolates tenant ownership and rejects workspace ID collisions", async () => {
    const store = makeMemoryWorkspaceRecoveryOwnershipStore()
    const first = await store.acquire({
      tenant: { tenantID: "tenant-a" },
      workspaceID: "workspace-a",
      workspaceDirectory: "/srv/workspaces/a",
      ownerID: "worker-a",
      leaseMs: 500,
    })
    const isolated = await store.acquire({
      tenant: { tenantID: "tenant-b" },
      workspaceID: "workspace-a",
      workspaceDirectory: "/srv/workspaces/a",
      ownerID: "worker-b",
      leaseMs: 500,
    })
    expect(first.acquired).toBe(true)
    expect(isolated.acquired).toBe(true)
    expect((await store.read({ tenantID: "tenant-a" }, "workspace-a"))?.ownerID).toBe("worker-a")
    expect((await store.read({ tenantID: "tenant-b" }, "workspace-a"))?.ownerID).toBe("worker-b")
    await expect(
      store.acquire({
        tenant: { tenantID: "tenant-a" },
        workspaceID: "workspace-a",
        workspaceDirectory: "/srv/workspaces/collision",
        ownerID: "worker-c",
        leaseMs: 500,
      }),
    ).rejects.toThrow("collision")
    await store.close()
  })

  test("exports ownership attempts and current epoch to Prometheus", () => {
    const labels = {
      tenantID: "tenant-a",
      scope: "p7.7.5",
      workspaceID: "workspace-a",
      instanceID: "worker-a",
    }
    recordWorkerQueueRecoveryOwnershipAttempt({ ...labels, outcome: "acquired" })
    observeWorkerQueueRecoveryOwnership({
      ...labels,
      held: true,
      epoch: 3,
      leaseExpiresAt: 2_000,
    })
    const metrics = renderWorkerQueueRecoveryPrometheus({ tenantID: "tenant-a" })
    expect(metrics).toContain('instance_id="worker-a",outcome="acquired"} 1')
    expect(metrics).toContain("opencode_worker_queue_recovery_owner_held")
    expect(metrics).toContain("opencode_worker_queue_recovery_ownership_epoch")
    expect(metrics).toContain(" 3")
  })
})
