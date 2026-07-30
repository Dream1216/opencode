import { afterEach, describe, expect, test } from "bun:test"
import {
  makeWorkerRecoveryGovernance,
  workerRecoveryGovernanceConfig,
  workerRecoveryPartitionID,
  workerRecoveryPartitionSelected,
} from "../src/session/worker-recovery-governance"
import { makeMemoryShadowCanaryBreakerStore } from "../src/session/runner/shadow-canary-breaker-store"
import {
  renderWorkerQueueRecoveryPrometheus,
  resetWorkerQueueRecoveryTelemetry,
} from "../src/database/postgres/worker-queue-telemetry"

afterEach(() => resetWorkerQueueRecoveryTelemetry())

const partition = {
  tenantID: "tenant-a",
  workspaceDirectory: "/srv/workspaces/alpha",
}

describe("WorkerRecoveryGovernance", () => {
  test("uses a stable tenant/workspace partition for canary sampling", async () => {
    expect(workerRecoveryPartitionID(partition)).toHaveLength(16)
    expect(workerRecoveryPartitionSelected(partition, 0)).toBe(false)
    expect(workerRecoveryPartitionSelected(partition, 1)).toBe(true)
    expect(workerRecoveryPartitionSelected(partition, 0.25)).toBe(
      workerRecoveryPartitionSelected(partition, 0.25),
    )

    const config = workerRecoveryGovernanceConfig({
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_SAMPLE_RATE: "0",
    })
    const governance = await makeWorkerRecoveryGovernance(config, {
      partition,
      instanceID: "worker-a",
    })
    expect(await governance.admit()).toMatchObject({
      allowed: false,
      reason: "sampled_out",
    })
    await governance.close()
  })

  test("shares breaker decisions across instances and exports recovery metrics", async () => {
    const config = workerRecoveryGovernanceConfig({
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_SAMPLE_RATE: "1",
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_WINDOW_SIZE: "4",
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_MIN_SAMPLES: "2",
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_ERROR_RATE: "0.4",
    })
    const store = makeMemoryShadowCanaryBreakerStore(config.policy)
    const first = await makeWorkerRecoveryGovernance(config, {
      partition,
      instanceID: "worker-a",
      store,
    })
    const second = await makeWorkerRecoveryGovernance(config, {
      partition,
      instanceID: "worker-b",
      store,
    })

    expect((await first.admit()).allowed).toBe(true)
    await first.record("completed")
    expect((await second.admit()).allowed).toBe(true)
    const opened = await second.record("failed")
    expect(opened).toMatchObject({
      open: true,
      sampleCount: 2,
      failureRate: 0.5,
    })
    expect(await first.admit()).toMatchObject({
      allowed: false,
      reason: "breaker_open",
    })

    const metrics = renderWorkerQueueRecoveryPrometheus({ tenantID: "tenant-a" })
    expect(metrics).toContain('instance_id="worker-a",outcome="completed"} 1')
    expect(metrics).toContain('instance_id="worker-b",outcome="failed"} 1')
    expect(metrics).toContain("opencode_worker_queue_recovery_breaker_open")
    expect(metrics).toContain(" 1")

    await first.close()
    await second.close()
    await store.close()
  })

  test("fails closed when PostgreSQL breaker settings are incomplete", async () => {
    const config = workerRecoveryGovernanceConfig({
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_SAMPLE_RATE: "2",
      OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_BACKEND: "postgres",
    })
    expect(config.invalidReason).toContain("RECOVERY_SAMPLE_RATE")
    expect(config.invalidReason).toContain("requires OPENCODE_DATABASE_URL")

    const governance = await makeWorkerRecoveryGovernance(config, {
      partition,
      instanceID: "worker-a",
      store: makeMemoryShadowCanaryBreakerStore(config.policy),
    })
    expect(await governance.admit()).toMatchObject({
      allowed: false,
      reason: "config_error",
    })
    await governance.close()
  })
})
