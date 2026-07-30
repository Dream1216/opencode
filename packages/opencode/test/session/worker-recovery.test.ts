import { describe, expect, test } from "bun:test"
import { settingsFromEnv } from "@/session/worker-recovery"

const enabled = {
  OPENCODE_POSTGRES_WORKER_QUEUE_RECOVERY_ENABLED: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_SAAS_SIDECAR: "1",
  OPENCODE_POSTGRES_WORKER_QUEUE_CONSUMER: "legacy-prompt",
}

describe("SessionWorkerRecovery settings", () => {
  test("is disabled by default", () => {
    expect(settingsFromEnv({})).toBeUndefined()
  })

  test("normalizes and deduplicates absolute workspaces", () => {
    expect(
      settingsFromEnv({
        ...enabled,
        OPENCODE_POSTGRES_WORKER_QUEUE_WORKSPACES: "/tmp/alpha,/tmp/alpha/../alpha,/tmp/beta",
      }),
    ).toEqual({
      workspaces: ["/tmp/alpha", "/tmp/beta"],
      retryMs: 1_000,
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
})
