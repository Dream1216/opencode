# Shadow AgentRun Event Store

This package adds a low-intrusion VibeCode-style shadow event projector for OpenCode.

It listens to OpenCode's existing `GlobalBus` event stream and writes append-only JSONL files without replacing OpenCode session, message, permission, or tool storage.

## Files

- `agent_runs.jsonl`: latest observed run snapshots, appended as state changes arrive.
- `agent_run_events.jsonl`: normalized AgentRun events, appended in observed order.

The default directory is under OpenCode's data directory:

```text
<OpenCode data dir>/shadow-agent-runs
```

In code this resolves to `path.join(Global.Path.data, "shadow-agent-runs")`.

## Environment

Disable the projector:

```bash
OPENCODE_SHADOW_AGENT_RUN_EVENT_STORE=0
```

Override the output directory:

```bash
OPENCODE_SHADOW_EVENT_STORE_DIR=/absolute/path/to/shadow-agent-runs
```

## Normalized Events

- `run.started`
- `run.deleted`
- `run.failed`
- `run.updated`
- `run.diff.updated`
- `run.idle`
- `run.status.<status>`
- `message.submitted`
- `message.updated`
- `provider.request.started`
- `provider.request.completed`
- `provider.request.failed`
- `assistant.message.updated`
- `assistant.delta`
- `file.patch.updated`
- `tool.started`
- `tool.completed`
- `tool.failed`
- `tool.updated`
- `permission.requested`
- `permission.replied`

Each event keeps the original OpenCode event type, event id, and properties under `source`.

## P0 Boundary

This is intentionally a shadow store. It does not change OpenCode's durable session store, agent loop, model execution, tool execution, or permission decisions.

The next migration step is replacing the JSONL writer with a real repository interface backed by SQLite or PostgreSQL while keeping the same projector boundary.
