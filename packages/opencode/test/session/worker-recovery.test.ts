import { describe, expect, test } from "bun:test"
import { settingsFromEnv } from "@/session/worker-recovery"

const enabled = {
  OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_ENABLED: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_CONSUMER: "legacy-prompt",
  OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS: "tenant-a,tenant-b",
}

describe("SessionWorkerRecovery settings", () => {
  test("is disabled by default", () => {
    expect(settingsFromEnv({})).toBeUndefined()
  })

  test("normalizes and deduplicates absolute workspaces", () => {
    expect(settingsFromEnv({
      ...enabled,
      OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "/tmp/alpha,/tmp/alpha/../alpha,/tmp/beta",
      OPENCODE_WORKER_ID: "worker-a",
    })).toMatchObject({
      workspaces: ["/tmp/alpha", "/tmp/beta"],
      tenants: ["tenant-a", "tenant-b"],
      partitions: [
        { tenantID: "tenant-a", workspaceDirectory: "/tmp/alpha" },
        { tenantID: "tenant-a", workspaceDirectory: "/tmp/beta" },
        { tenantID: "tenant-b", workspaceDirectory: "/tmp/alpha" },
        { tenantID: "tenant-b", workspaceDirectory: "/tmp/beta" },
      ],
      retryMs: 1_000,
      instanceID: "worker-a",
    })
  })

  test("rejects relative workspaces", () => {
    expect(() =>
      settingsFromEnv({
        ...enabled,
        OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "relative/path",
      }),
    ).toThrow("must be absolute")
  })

  test("requires the SaaS legacy Prompt queue", () => {
    expect(() =>
      settingsFromEnv({
        ...enabled,
        OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR: "0",
        OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "/tmp/alpha",
      }),
    ).toThrow("requires the SaaS legacy Prompt worker queue")
  })

  test("requires tenant-scoped recovery partitions", () => {
    expect(() =>
      settingsFromEnv({
        ...enabled,
        OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS: "",
        OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "/tmp/alpha",
      }),
    ).toThrow("non-empty tenant allowlist")
  })

  test("fails closed on invalid shared breaker configuration", () => {
    expect(
      settingsFromEnv({
        ...enabled,
        OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "/tmp/alpha",
        OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_SAMPLE_RATE: "2",
        OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_BREAKER_BACKEND: "postgres",
        OPENCODE_DATABASE_URL: "",
      })?.governance.invalidReason,
    ).toContain("RECOVERY_SAMPLE_RATE")
  })
})
