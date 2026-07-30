import { GlobalBus, type GlobalEvent } from "@/bus/global"
import { Identifier } from "@/id/id"
import {
  ShadowAgentRunEventStoreDefault,
  type ShadowAgentRunEvent,
  type ShadowAgentRunSnapshot,
  type ShadowAgentRunStatus,
} from "./event-store"

type SourcePayload = {
  id?: string
  type?: string
  properties?: Record<string, unknown>
  syncEvent?: unknown
}

type SessionInfo = {
  id?: string
  title?: string
  agent?: string
  model?: {
    id?: string
    providerID?: string
    variant?: string
  }
  projectID?: string
  workspaceID?: string
  directory?: string
  time?: {
    created?: number
    updated?: number
  }
}

type MessageInfo = {
  id?: string
  role?: string
  sessionID?: string
  parentID?: string
  providerID?: string
  modelID?: string
  error?: unknown
  finish?: boolean | string
  time?: {
    created?: number
  }
}

type PartInfo = {
  id?: string
  type?: string
  sessionID?: string
  messageID?: string
  tool?: string
  callID?: string
  state?: {
    status?: string
    input?: unknown
    output?: unknown
    error?: unknown
    time?: unknown
  }
}

const state = {
  initialized: false,
  sequence: 0,
  runs: new Map<string, ShadowAgentRunSnapshot>(),
  assistantStarted: new Set<string>(),
  userSubmitted: new Set<string>(),
}

function enabled() {
  const value = process.env.OPENCODE_SHADOW_AGENT_RUN_EVENT_STORE
  return value !== "0" && value !== "false"
}

export function initShadowAgentRunProjector() {
  if (state.initialized || !enabled()) return
  state.initialized = true
  GlobalBus.on("event", handleGlobalEvent)
}

export async function flushShadowAgentRunProjector() {
  await ShadowAgentRunEventStoreDefault.flush()
}

function handleGlobalEvent(event: GlobalEvent) {
  const payload = event.payload as SourcePayload
  if (!payload || payload.type === "sync" || !payload.type) return

  const properties = record(payload.properties)
  const sessionID = firstString(properties.sessionID, sessionInfo(properties)?.id)
  if (!sessionID) return

  const sourceType = payload.type
  const sourceEventID = payload.id
  const now = Date.now()
  const normalized = normalizeEventType(sourceType, properties)
  const current = state.runs.get(sessionID)
  const run = snapshot({
    current,
    event,
    properties,
    sessionID,
    sourceType,
    sourceEventID,
    status: statusFrom(normalized, current?.status),
    now,
  })

  state.runs.set(sessionID, run)

  const projected: ShadowAgentRunEvent = {
    version: "vibecode-shadow-agent-run-event.v1",
    eventID: sourceEventID ? `shadow_${sourceEventID}` : Identifier.create("evt", "ascending"),
    runID: run.runID,
    sessionID,
    projectID: run.projectID,
    workspaceID: run.workspaceID,
    directory: run.directory,
    sequence: ++state.sequence,
    type: normalized,
    source: {
      eventID: sourceEventID,
      type: sourceType,
      properties,
    },
    time: {
      occurred: eventTime(properties) ?? now,
      stored: now,
    },
  }

  ShadowAgentRunEventStoreDefault.write({ run, event: projected })
}

function snapshot(input: {
  current?: ShadowAgentRunSnapshot
  event: GlobalEvent
  properties: Record<string, unknown>
  sessionID: string
  sourceType: string
  sourceEventID?: string
  status: ShadowAgentRunStatus
  now: number
}): ShadowAgentRunSnapshot {
  const info = sessionInfo(input.properties)
  const existing = input.current
  return {
    version: "vibecode-shadow-agent-run.v1",
    runID: existing?.runID ?? `run_${input.sessionID}`,
    sessionID: input.sessionID,
    projectID: firstString(info?.projectID, existing?.projectID, input.event.project),
    workspaceID: firstString(info?.workspaceID, existing?.workspaceID, input.event.workspace),
    directory: firstString(info?.directory, existing?.directory, input.event.directory),
    title: firstString(info?.title, existing?.title),
    agent: firstString(info?.agent, existing?.agent),
    model: info?.model ?? existing?.model,
    status: input.status,
    sourceType: input.sourceType,
    sourceEventID: input.sourceEventID,
    time: {
      created: info?.time?.created ?? existing?.time.created,
      updated: info?.time?.updated ?? input.now,
    },
  }
}

function normalizeEventType(type: string, properties: Record<string, unknown>) {
  if (type === "tool.governance.evaluated") return "tool.policy.evaluated"
  if (type.includes("shadow.canary") && type.endsWith("dispatched")) return "shadow.canary.dispatched"
  if (type.includes("shadow.canary") && type.endsWith("accepted")) return "shadow.canary.accepted"
  if (type.includes("shadow.canary") && type.endsWith("failed")) return "shadow.canary.failed"
  if (type.includes("shadow.canary") && type.endsWith("skipped")) return "shadow.canary.skipped"
  if (type.includes("model.invocation") && type.endsWith("attempted")) return "provider.request.attempt"
  if (type.includes("model.invocation") && type.endsWith("retried")) return "provider.request.retry"
  if (type.includes("model.invocation") && type.endsWith("usage")) return "provider.request.usage"
  if (type.includes("model.invocation") && type.endsWith("completed")) return "provider.request.completed"
  if (type.includes("model.invocation") && type.endsWith("failed")) return "provider.request.failed"
  if (type.includes("worker") && type.endsWith("scheduled")) return "worker.scheduled"
  if (type.includes("worker") && type.endsWith("resumed")) return "worker.resumed"
  if (type.includes("worker") && type.endsWith("started")) return "worker.started"
  if (type.includes("worker") && type.endsWith("stop_requested")) return "worker.stop.requested"
  if (type.includes("worker") && type.endsWith("stopped")) return "worker.stopped"
  if (type.includes("worker") && type.endsWith("completed")) return "worker.completed"
  if (type.includes("worker") && type.endsWith("failed")) return "worker.failed"
  if (type.includes("worker") && type.endsWith("lease")) return "worker.lease"
  if (type.includes("workflow") && type.endsWith("bound")) return "workflow.bound"
  if (type.includes("tenant") && type.endsWith("bound")) return "tenant.bound"
  if (type.includes("rls") && type.endsWith("evaluated")) return "rls.evaluated"
  if (type.includes("audit") && type.endsWith("recorded")) return "audit.recorded"
  if (type.includes("release.governance") && type.endsWith("evaluated")) return "release.governance.evaluated"
  if (type.includes("permission") && type.endsWith("asked")) return "permission.requested"
  if (type.includes("permission") && type.endsWith("replied")) return "permission.replied"
  if (type.includes("session") && type.endsWith("created")) return "run.started"
  if (type.includes("session") && type.endsWith("deleted")) return "run.deleted"
  if (type.includes("session") && type.endsWith("error")) return "run.failed"
  if (type.includes("session") && type.endsWith("updated")) return "run.updated"
  if (type.includes("session") && type.endsWith("diff")) return "run.diff.updated"
  if (type.includes("session") && type.endsWith("idle")) return "run.idle"
  if (type.includes("session") && type.endsWith("status")) {
    const status = record(properties.status)
    const statusType = firstString(status.type)
    return statusType ? `run.status.${statusType}` : "run.status.updated"
  }

  const info = messageInfo(properties)
  if (info?.role === "user") {
    if (info.id && !state.userSubmitted.has(info.id)) {
      state.userSubmitted.add(info.id)
      return "message.submitted"
    }
    return "message.updated"
  }
  if (info?.role === "assistant") {
    if (info.error) return "provider.request.failed"
    if (info.finish) return "provider.request.completed"
    if (info.id && !state.assistantStarted.has(info.id)) {
      state.assistantStarted.add(info.id)
      return "provider.request.started"
    }
    return "assistant.message.updated"
  }

  const part = partInfo(properties)
  if (part?.type === "tool") {
    const status = part.state?.status
    if (status === "completed") return "tool.completed"
    if (status === "error") return "tool.failed"
    if (status === "running") return "tool.started"
    return "tool.updated"
  }
  if (part?.type === "patch") return "file.patch.updated"
  if (part?.type === "text" && type.includes("delta")) return "assistant.delta"
  if (part?.type) return `message.part.${part.type}.updated`

  return `opencode.${type}`
}

function statusFrom(type: string, current?: ShadowAgentRunStatus): ShadowAgentRunStatus {
  if (type === "run.failed" || type === "provider.request.failed") return "failed"
  if (type === "run.deleted") return "deleted"
  if (type === "provider.request.completed") return "completed"
  if (type === "run.started") return "running"
  return current ?? "running"
}

function sessionInfo(properties: Record<string, unknown>): SessionInfo | undefined {
  return record(properties.info) as SessionInfo | undefined
}

function messageInfo(properties: Record<string, unknown>): MessageInfo | undefined {
  return record(properties.info) as MessageInfo | undefined
}

function partInfo(properties: Record<string, unknown>): PartInfo | undefined {
  return record(properties.part) as PartInfo | undefined
}

function eventTime(properties: Record<string, unknown>) {
  const info = sessionInfo(properties) ?? messageInfo(properties)
  return firstNumber(properties.time, timeUpdated(info), info?.time?.created)
}

function timeUpdated(info: SessionInfo | MessageInfo | undefined) {
  return "updated" in (info?.time ?? {}) ? (info?.time as { updated?: number }).updated : undefined
}

function firstString(...values: unknown[]) {
  return values.find((value): value is string => typeof value === "string" && value.length > 0)
}

function firstNumber(...values: unknown[]) {
  return values.find((value): value is number => typeof value === "number" && Number.isFinite(value))
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}
