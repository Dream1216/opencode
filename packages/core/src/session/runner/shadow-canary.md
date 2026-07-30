# P5.2 Shadow Canary

P5.2 mirrors sampled real model requests to an isolated OpenCode candidate
without transferring primary execution ownership. The v1.18.7 request remains
authoritative and the candidate response is never returned to the user.

## Runtime contract

Set:

- `OPENCODE_SHADOW_CANARY_ENABLED=true`
- `OPENCODE_SHADOW_CANARY_URL=https://canary.example/internal/opencode-shadow`
- `OPENCODE_SHADOW_CANARY_TOKEN=<secret-manager-reference>`
- `OPENCODE_SHADOW_CANARY_SAMPLE_RATE=0.01`
- `OPENCODE_SHADOW_CANARY_TARGET_VERSION=v1.18.8`

Optional controls:

- `OPENCODE_SHADOW_CANARY_TIMEOUT_MS`, default `2000`
- `OPENCODE_SHADOW_CANARY_MAX_BYTES`, default `1048576`
- `OPENCODE_SHADOW_CANARY_SOURCE_VERSION`, default `v1.18.7`

Remote endpoints require HTTPS and a bearer token. Plain HTTP is accepted only
for loopback smoke services. The request envelope is
`opencode-shadow-canary-request.v1`, carries `shadowOnly: true`, and preserves
real prompt/model/tool-schema content after removing credential, header,
function, signal, and binary fields.

The canary must respond with any 2xx status. A JSON response may contain
`receiptID` or `id`; it is recorded for later comparison.

## Isolation guarantees

- HTTP dispatch runs in a daemon fiber after the primary attempted event.
- Timeout, network failure, non-2xx response, and invalid configuration never
  fail or delay the primary model stream.
- The canary endpoint must use an isolated workspace and must not publish tool
  side effects back to the primary runtime.
- Durable `dispatched`, `accepted`, `failed`, and `skipped` events provide
  evidence without changing session projection ownership.

P5.2 proves ingress and isolation. Candidate output equivalence, side-effect
diffing, promotion thresholds, and automatic rollback remain later gates.
