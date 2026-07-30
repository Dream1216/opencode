import { Global } from "@opencode-ai/core/global"
import { mkdir, appendFile } from "node:fs/promises"
import path from "node:path"
import { ExecutionResourceBinding } from "@opencode-ai/core/identity/execution-resource-binding"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

export type ShadowAgentRunStatus = "running" | "completed" | "failed" | "cancelled" | "deleted" | "unknown"

export type ShadowAgentRunSnapshot = {
  version: "vibecode-shadow-agent-run.v1"
  runID: string
  sessionID: string
  organizationID?: string
  tenantID?: string
  projectID?: string
  workspaceID?: string
  directory?: string
  title?: string
  agent?: string
  model?: {
    id?: string
    providerID?: string
    variant?: string
  }
  status: ShadowAgentRunStatus
  sourceType: string
  sourceEventID?: string
  time: {
    created?: number
    updated: number
  }
}

export type ShadowAgentRunEvent = {
  version: "vibecode-shadow-agent-run-event.v1"
  eventID: string
  runID: string
  sessionID: string
  organizationID?: string
  tenantID?: string
  projectID?: string
  workspaceID?: string
  directory?: string
  sequence: number
  type: string
  source: {
    eventID?: string
    type: string
    properties: unknown
  }
  time: {
    occurred: number
    stored: number
  }
}

const defaultStoreDir = () => path.join(Global.Path.data, "shadow-agent-runs")

function configuredStoreDir() {
  return process.env.OPENCODE_SHADOW_EVENT_STORE_DIR || defaultStoreDir()
}

export class ShadowAgentRunEventStore {
  private queue = Promise.resolve()
  private failures: unknown[] = []
  private initialized = false

  readonly dir = configuredStoreDir()
  readonly runsFile = path.join(this.dir, "agent_runs.jsonl")
  readonly eventsFile = path.join(this.dir, "agent_run_events.jsonl")

  write(input: { run?: ShadowAgentRunSnapshot; event?: ShadowAgentRunEvent }) {
    this.queue = this.queue
      .then(async () => {
        const runID = input.run?.runID ?? input.event?.runID
        const binding = runID && SaasIdentity.enabled() ? await executionBinding(runID) : undefined
        if (SaasIdentity.enabled() && !binding) {
          throw new Error(`AgentRun ${runID ?? "unknown"} has no execution resource binding`)
        }
        const run = input.run && binding
          ? { ...input.run, organizationID: binding.organizationID, tenantID: binding.tenantID }
          : input.run
        const event = input.event && binding
          ? { ...input.event, organizationID: binding.organizationID, tenantID: binding.tenantID }
          : input.event
        if (!this.initialized) {
          await mkdir(this.dir, { recursive: true })
          this.initialized = true
        }
        if (run) {
          await appendFile(this.runsFile, JSON.stringify(run) + "\n")
        }
        if (event) {
          await appendFile(this.eventsFile, JSON.stringify(event) + "\n")
        }
      })
      .catch((error) => {
        this.failures.push(error)
        console.warn("[shadow-agent-run] failed to persist event", error)
      })
  }

  async flush() {
    while (true) {
      const barrier = this.queue
      await barrier
      if (barrier === this.queue) break
    }
    const failures = this.failures.splice(0)
    if (failures.length > 0) {
      throw new AggregateError(failures, `Shadow AgentRun projector failed to persist ${failures.length} queued write(s)`)
    }
  }
}

export const ShadowAgentRunEventStoreDefault = new ShadowAgentRunEventStore()

async function executionBinding(runID: string) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const binding = await ExecutionResourceBinding.lookup("agent_run", runID)
    if (binding) return binding
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
