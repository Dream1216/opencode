import { describe, expect, test } from "bun:test"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { ShadowAgentRunEventStore, type ShadowAgentRunSnapshot } from "@/shadow-agent-run/event-store"

const previousStoreDir = process.env.OPENCODE_SHADOW_EVENT_STORE_DIR
const previousSaasMode = process.env.OPENCODE_SAAS_MODE

async function cleanup(dir: string) {
  if (previousStoreDir === undefined) delete process.env.OPENCODE_SHADOW_EVENT_STORE_DIR
  else process.env.OPENCODE_SHADOW_EVENT_STORE_DIR = previousStoreDir
  if (previousSaasMode === undefined) delete process.env.OPENCODE_SAAS_MODE
  else process.env.OPENCODE_SAAS_MODE = previousSaasMode
  await rm(dir, { recursive: true, force: true })
}

function snapshot(runID: string): ShadowAgentRunSnapshot {
  return {
    version: "vibecode-shadow-agent-run.v1",
    runID,
    sessionID: runID.replace(/^run_/, ""),
    status: "running",
    sourceType: "session.created",
    time: {
      updated: Date.now(),
    },
  }
}

describe("ShadowAgentRunEventStore.flush", () => {
  test("waits for all writes queued before and during the barrier", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "opencode-shadow-flush-"))
    try {
      process.env.OPENCODE_SHADOW_EVENT_STORE_DIR = dir
      process.env.OPENCODE_SAAS_MODE = "false"
      const store = new ShadowAgentRunEventStore()

      store.write({ run: snapshot("run_first") })
      const flushing = store.flush()
      store.write({ run: snapshot("run_second") })
      await flushing
      await store.flush()

      const lines = (await Bun.file(store.runsFile).text()).trim().split("\n")
      expect(lines).toHaveLength(2)
      expect(lines.map((line) => JSON.parse(line).runID)).toEqual(["run_first", "run_second"])
    } finally {
      await cleanup(dir)
    }
  })

  test("propagates queued persistence failures to the cleanup barrier", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "opencode-shadow-flush-failure-"))
    try {
      const blocked = path.join(dir, "not-a-directory")
      await writeFile(blocked, "blocked")
      process.env.OPENCODE_SHADOW_EVENT_STORE_DIR = blocked
      process.env.OPENCODE_SAAS_MODE = "false"
      const store = new ShadowAgentRunEventStore()

      store.write({ run: snapshot("run_failure") })

      await expect(store.flush()).rejects.toThrow("failed to persist 1 queued write")
    } finally {
      await cleanup(dir)
    }
  })
})
