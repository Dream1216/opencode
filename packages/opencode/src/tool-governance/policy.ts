import { GlobalBus } from "@/bus/global"
import type { PermissionV1 } from "@opencode-ai/core/v1/permission"

export type ToolGovernanceMode = "observe" | "enforce"
export type ToolGovernanceIntent = "allow" | "ask" | "deny"
export type ToolGovernanceRiskLevel = "low" | "medium" | "high" | "critical"
export type ToolGovernanceCategory = "read" | "edit" | "execute" | "network" | "mcp" | "unknown"

export type ToolGovernanceInput = Pick<
  PermissionV1.AskInput,
  "sessionID" | "permission" | "patterns" | "metadata" | "tool"
>

export type ToolGovernanceDecision = {
  version: "vibecode-tool-governance-policy.v1"
  mode: ToolGovernanceMode
  intent: ToolGovernanceIntent
  riskLevel: ToolGovernanceRiskLevel
  category: ToolGovernanceCategory
  sideEffects: string[]
  reason: string
}

const destructiveCommandPatterns = [
  /\brm\s+(-[^\s]*r[^\s]*f|-rf|-fr)\s+(\/|~|\$HOME)(\s|$)/,
  /\bsudo\s+/,
  /\bchmod\s+-?R?\s+777\b/,
  /\bmkfs\b/,
  /\bdd\s+.*\bof=\/dev\//,
  /\bshutdown\b/,
  /\breboot\b/,
  /\bkill\s+-9\b/,
  /\bcurl\b[\s\S]*\|\s*(sh|bash)\b/,
  /\bwget\b[\s\S]*\|\s*(sh|bash)\b/,
]

export function toolGovernanceMode(): ToolGovernanceMode {
  return process.env.OPENCODE_TOOL_GOVERNANCE_MODE === "enforce" ? "enforce" : "observe"
}

export function evaluateToolGovernance(input: ToolGovernanceInput): ToolGovernanceDecision {
  const mode = toolGovernanceMode()
  const category = classifyCategory(input)
  const text = requestText(input)
  const critical = destructiveCommandPatterns.some((pattern) => pattern.test(text))
  const riskLevel = critical ? "critical" : riskForCategory(category)
  const intent = riskLevel === "critical" ? "deny" : "ask"

  return {
    version: "vibecode-tool-governance-policy.v1",
    mode,
    intent,
    riskLevel,
    category,
    sideEffects: sideEffectsForCategory(category),
    reason: reasonFor({ category, riskLevel, critical }),
  }
}

export function emitToolGovernanceDecision(decision: ToolGovernanceDecision, input: ToolGovernanceInput) {
  GlobalBus.emit("event", {
    payload: {
      type: "tool.governance.evaluated",
      properties: {
        sessionID: input.sessionID,
        permission: input.permission,
        patterns: input.patterns,
        metadata: input.metadata,
        tool: input.tool,
        decision,
      },
    },
  })
}

function classifyCategory(input: ToolGovernanceInput): ToolGovernanceCategory {
  const permission = input.permission.toLowerCase()
  const text = requestText(input)

  if (permission.includes("mcp") || text.includes("mcp__")) return "mcp"
  if (matchesAny(permission, ["read", "list", "glob", "grep"])) return "read"
  if (matchesAny(permission, ["bash", "shell", "terminal", "execute", "run"])) return "execute"
  if (matchesAny(permission, ["edit", "write", "patch", "delete", "create"])) return "edit"
  if (matchesAny(permission, ["web", "fetch", "search", "http", "network"])) return "network"
  return "unknown"
}

function riskForCategory(category: ToolGovernanceCategory): ToolGovernanceRiskLevel {
  if (category === "read") return "low"
  if (category === "network" || category === "edit" || category === "mcp") return "medium"
  if (category === "execute") return "high"
  return "medium"
}

function sideEffectsForCategory(category: ToolGovernanceCategory) {
  if (category === "read") return []
  if (category === "network") return ["network_access"]
  if (category === "edit") return ["filesystem_write"]
  if (category === "execute") return ["process_execution", "filesystem_write"]
  if (category === "mcp") return ["external_system_access"]
  return ["unknown_side_effects"]
}

function reasonFor(input: {
  category: ToolGovernanceCategory
  riskLevel: ToolGovernanceRiskLevel
  critical: boolean
}) {
  if (input.critical) return "Matched destructive command guardrail."
  return `Classified ${input.category} permission as ${input.riskLevel} risk.`
}

function requestText(input: ToolGovernanceInput) {
  const metadata = Object.entries(input.metadata ?? {}).map(([key, value]) => `${key}:${stringify(value)}`)
  return [input.permission, ...input.patterns, ...metadata, input.tool?.messageID, input.tool?.callID]
    .filter(Boolean)
    .join("\n")
    .toLowerCase()
}

function stringify(value: unknown) {
  if (typeof value === "string") return value
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function matchesAny(value: string, needles: string[]) {
  return needles.some((needle) => value.includes(needle))
}
