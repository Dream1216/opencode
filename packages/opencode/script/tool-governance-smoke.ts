import assert from "node:assert/strict"
import type { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { evaluateToolGovernance } from "../src/tool-governance/policy"

const sessionID = "ses_tool_governance_smoke" as PermissionV1.AskInput["sessionID"]

const safeRead = evaluateToolGovernance({
  sessionID,
  permission: "read",
  patterns: ["README.md"],
  metadata: {},
})
assert.equal(safeRead.category, "read")
assert.equal(safeRead.riskLevel, "low")
assert.equal(safeRead.intent, "ask")

const edit = evaluateToolGovernance({
  sessionID,
  permission: "edit",
  patterns: ["src/index.ts"],
  metadata: {},
})
assert.equal(edit.category, "edit")
assert.equal(edit.riskLevel, "medium")
assert.equal(edit.intent, "ask")

const destructive = evaluateToolGovernance({
  sessionID,
  permission: "bash",
  patterns: ["rm -rf /"],
  metadata: {},
})
assert.equal(destructive.category, "execute")
assert.equal(destructive.riskLevel, "critical")
assert.equal(destructive.intent, "deny")

console.log(
  JSON.stringify({
    ok: true,
    decisions: [safeRead, edit, destructive],
  }),
)
