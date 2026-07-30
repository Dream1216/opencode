# Tool governance policy adapter

This adapter adds a VibeCode-style governance layer above OpenCode permissions without replacing the native `allow` / `ask` / `deny` rules.

## Runtime mode

- `OPENCODE_TOOL_GOVERNANCE_MODE=observe` records governance decisions and keeps native OpenCode permission behavior unchanged.
- `OPENCODE_TOOL_GOVERNANCE_MODE=enforce` denies requests classified as `critical`; all other requests continue through native OpenCode permission evaluation.

## Shadow events

Every permission ask emits a global `tool.governance.evaluated` event. The shadow AgentRun/EventStore projects it as `tool.policy.evaluated`.

## Current policy scope

- Classifies requests into `read`, `edit`, `execute`, `network`, `mcp`, or `unknown`.
- Assigns `low`, `medium`, `high`, or `critical` risk.
- Flags destructive shell patterns such as `rm -rf /`, `sudo`, `mkfs`, `dd of=/dev/*`, `curl | sh`, and `wget | sh` as `critical`.
