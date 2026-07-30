# P5.3.1 Shadow Canary hardening

P5.3.1 keeps the OpenCode main session loop unchanged. The shadow executor remains an external HTTP process.

## Outcome transport

- Request and outcome URLs are validated independently.
- Remote URLs require HTTPS and a bearer token.
- URL credentials are rejected.
- The outcome URL must share the request URL origin unless
  `OPENCODE_SHADOW_CANARY_ALLOW_CROSS_ORIGIN_OUTCOME=1` is explicitly set.

## Shared breaker

The default remains process-local memory. Enable PostgreSQL sharing explicitly:

```bash
OPENCODE_SHADOW_CANARY_BREAKER_BACKEND=postgres
OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL=postgresql://...
OPENCODE_SHADOW_CANARY_BREAKER_SCOPE=tenant-id:v1.18.8
```

Schema creation is protected by a transaction-scoped PostgreSQL advisory lock. Breaker updates are serialized by
scope, deduplicated by request digest, and survive executor restarts.

## v1.18.8 model-only candidate

Set `OPENCODE_SHADOW_CANDIDATE_CHECKOUT` to a no-merge OpenCode v1.18.8 checkout and provide provider config via
`OPENCODE_SHADOW_CANDIDATE_CONFIG_CONTENT`. The executor verifies the checkout package version, launches its real
TypeScript CLI, forces one step, disables all permissions/tools, disables project config and recursive canary, and
uses isolated HOME/XDG directories.

This is process isolation, not an OS security boundary. Production execution still belongs in a container or
microVM with network and filesystem policy.
