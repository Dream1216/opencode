export * as SessionWorkflow from "./workflow"

import { createHash } from "node:crypto"
import { DateTime, Effect } from "effect"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import type { Prompt } from "./prompt"
import { Database } from "../database/database"
import { EventV2 } from "../event"
import { SessionEvent } from "./event"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"

export type Complexity = {
  readonly score: number
  readonly reasons: ReadonlyArray<string>
}

export type WorkflowNode = {
  readonly id: string
  readonly type: string
  readonly label: string
  readonly config?: Record<string, unknown>
}

export type WorkflowEdge = {
  readonly id: string
  readonly source: string
  readonly target: string
  readonly condition?: string
}

export type WorkflowDefinition = {
  readonly id: string
  readonly name: string
  readonly nodes: ReadonlyArray<WorkflowNode>
  readonly edges: ReadonlyArray<WorkflowEdge>
  readonly settings: Record<string, unknown>
}

export type WorkflowVersion = {
  readonly id: string
  readonly definitionID: string
  readonly version: string
  readonly runtime: "opencode-main-loop-sidecar"
  readonly checksum: string
}

export const bindComplexPrompt = Effect.fn("SessionWorkflow.bindComplexPrompt")(function* (input: {
  readonly db: Database.Interface["db"]
  readonly events: EventV2.Interface
  readonly sessionID: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly prompt: Prompt
}) {
  const complexity = classifyComplexity(input.prompt)
  if (complexity.score < 3) return undefined
  if (yield* hasWorkflowBinding(input.db, input.sessionID, input.messageID)) return undefined

  const definition = defaultDefinition()
  const version = defaultVersion(definition)
  return yield* input.events.publish(SessionEvent.Workflow.Bound, {
    sessionID: input.sessionID,
    timestamp: yield* DateTime.now,
    messageID: input.messageID,
    complexity,
    definition,
    version,
  })
})

function classifyComplexity(prompt: Prompt): Complexity {
  const text = prompt.text.trim()
  const reasons: string[] = []
  if (text.length >= 300) reasons.push("long_prompt")
  if (/[。；;]\s*[^。；;]+[。；;]/.test(text)) reasons.push("multi_sentence")
  if (/(计划|方案|架构|迭代|执行|验证|测试|workflow|plan|architecture|iterate|implement|validate|test)/i.test(text))
    reasons.push("planning_language")
  if (/(P0|P1|P2|P3|P4|P5|phase|阶段)/i.test(text)) reasons.push("phased_delivery")
  if (prompt.files && prompt.files.length > 0) reasons.push("attachments")

  return {
    score: reasons.length,
    reasons,
  }
}

function defaultDefinition(): WorkflowDefinition {
  return {
    id: "wf_vibecode_complex_task_default",
    name: "VibeCode complex task default workflow",
    nodes: [
      { id: "intake", type: "input", label: "Task intake" },
      { id: "plan", type: "planner", label: "Plan and decompose" },
      { id: "execute", type: "runner", label: "Execute OpenCode main loop" },
      { id: "verify", type: "verification", label: "Run scoped validation" },
      { id: "summarize", type: "summary", label: "Summarize durable result" },
    ],
    edges: [
      { id: "intake-plan", source: "intake", target: "plan" },
      { id: "plan-execute", source: "plan", target: "execute" },
      { id: "execute-verify", source: "execute", target: "verify" },
      { id: "verify-summarize", source: "verify", target: "summarize" },
    ],
    settings: {
      configurationSource: "workflow_definition_json",
      runtimeTarget: "opencode_main_loop",
      langgraphCompile: false,
    },
  }
}

function defaultVersion(definition: WorkflowDefinition): WorkflowVersion {
  return {
    id: "wfv_vibecode_complex_task_default_1",
    definitionID: definition.id,
    version: "1.0.0",
    runtime: "opencode-main-loop-sidecar",
    checksum: checksum(definition),
  }
}

const hasWorkflowBinding = Effect.fn("SessionWorkflow.hasWorkflowBinding")(function* (
  db: Database.Interface["db"],
  sessionID: SessionSchema.ID,
  messageID: SessionMessage.ID,
) {
  let after: number | undefined
  while (true) {
    const page = yield* EventV2.readAggregate(db, {
      aggregateID: sessionID,
      after,
      limit: 500,
      manifest: SessionDurable,
    })
    if (
      page.events.some(
        (event) => event.type === "session.next.workflow.bound" && event.data.messageID === messageID,
      )
    )
      return true
    if (!page.hasMore) return false
    const next = page.events.at(-1)?.durable?.seq
    if (next === undefined) return false
    after = next
  }
})

function checksum(definition: WorkflowDefinition) {
  return createHash("sha256").update(JSON.stringify(definition)).digest("hex")
}
