# P5.3 Shadow Canary Executor

The executor accepts `opencode-shadow-canary-request.v1` envelopes, runs a
configured candidate command in a fresh temporary working directory, collects
primary and candidate outcomes, emits Prometheus diff metrics, and opens a
shadow-only circuit breaker when regression thresholds are exceeded.

The service does not become the primary request path and never stores prompt
payloads. Its JSONL store contains correlation metadata, outcomes, diffs, and
breaker transitions only.

Required runtime settings:

- `OPENCODE_SHADOW_CANARY_EXECUTOR_COMMAND_JSON`
- `OPENCODE_SHADOW_CANARY_TOKEN` for non-loopback binding
- `OPENCODE_SHADOW_CANARY_URL=<executor>/v1/shadow/requests`
- `OPENCODE_SHADOW_CANARY_OUTCOME_URL=<executor>/v1/shadow/outcomes/primary`

The candidate command receives the envelope on stdin and must return one
`opencode-shadow-canary-outcome.v1` JSON object on stdout. It runs with a
minimal environment, `OPENCODE_SHADOW_ONLY=1`,
`OPENCODE_SHADOW_TOOLS_DISABLED=1`, and a disposable working directory.
Production deployment should additionally place the command in an OS-level
container or sandbox.

Operational endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /v1/shadow/diffs/:requestDigest`
- `POST /v1/shadow/requests`
- `POST /v1/shadow/outcomes/primary`

Breaker defaults:

- window: 100 paired outcomes
- minimum samples: 20
- candidate failure rate: 10%
- structural status/tool-plan mismatch rate: 20%
- slow outcome rate: 25%
- slow threshold: candidate latency above 2x primary
- cooldown: 5 minutes

Exact output digest mismatch is exported for analysis but does not open the
breaker because model output may be nondeterministic.
