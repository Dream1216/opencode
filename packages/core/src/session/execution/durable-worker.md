# Durable session worker

P2 adds a durable worker ledger around OpenCode's existing local `SessionRunner` execution.

The implementation intentionally keeps the runner and provider/tool semantics unchanged. It records lifecycle state at the `SessionExecution` boundary:

- `session.next.worker.scheduled`
- `session.next.worker.resumed`
- `session.next.worker.started`
- `session.next.worker.stop_requested`
- `session.next.worker.stopped`
- `session.next.worker.completed`
- `session.next.worker.failed`
- `session.next.worker.lease` with `phase=acquired|heartbeat|released`

`resume` and `wake` still delegate to `SessionRunCoordinator`; `interrupt` is the stop boundary. `replay(sessionID)` reads the durable session aggregate and returns the worker lifecycle events in order.

P2.1 adds a lease token and periodic heartbeat while a local drain is active. The lease is still advisory and local-owner based; it does not yet enforce multi-process exclusion. A later phase can replace the local coordinator owner with a shared lease/TTL check while preserving the same event contract.

Environment overrides:

- `OPENCODE_WORKER_ID`: explicit worker owner ID.
- `OPENCODE_WORKER_LEASE_TTL_MS`: advertised lease TTL. Heartbeat interval is one third of this value, clamped to at least 1 second.

P4.30 connects the Session execution boundary to the real PostgreSQL worker lease adapter. The adapter remains opt-in and tenant-scoped:

- `OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED=1`
- `OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS=tenant-a,tenant-b`

When enabled for the current tenant, a Session drain must acquire the PostgreSQL run lease before `SessionRunner` starts. A contended lease records a `phase=contended` lifecycle event and leaves execution to the current owner. Heartbeat failure races and interrupts the active runner; successful drains complete the lease, while interrupted or failed drains release it.

P4.31 propagates the current PostgreSQL fencing token through `SessionRunner` to every local tool context. Model turns and local tool settlement revalidate the fence before side effects start. Tool contexts include:

- `runID`
- `ownerID`
- `fencingToken`
- deterministic `idempotencyKey`

SQLite/local execution remains unchanged when the feature is disabled or the current tenant is outside the allowlist.

P4.32 adds a tenant-scoped PostgreSQL `worker_job` queue in front of the existing Session execution boundary. Queue rows use a monotonic requested generation so a `wake` or `resume` received while a generation is running is preserved and requeued after the current claim completes.

Enable the queue only with the existing PostgreSQL coordination adapter:

- `OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED=1`
- `OPENCODE_POSTGRES_WORKER_QUEUE_ENABLED=1`
- `OPENCODE_POSTGRES_WORKER_QUEUE_TENANTS=tenant-a,tenant-b`

Queue consumers claim pending or expired jobs with `FOR UPDATE SKIP LOCKED`. Claim tokens are monotonic and independent from the execution fencing token. The queue token protects job completion ownership; the execution fencing token continues to protect model/tool side effects.

P4.33 starts a scoped queue consumer with each enabled SessionExecution service. On startup it scans pending jobs and expired running jobs, marks expired claims as recovery work, and resumes them through the existing coordinator. `SIGKILL` leaves the claim durable until expiry so another process can take over. A stale process cannot complete a newer claim.

Additional settings:

- `OPENCODE_POSTGRES_WORKER_JOB_CLAIM_TTL_MS`
- `OPENCODE_POSTGRES_WORKER_QUEUE_POLL_MS`
- `OPENCODE_POSTGRES_WORKER_JOB_MAX_ATTEMPTS`
- `OPENCODE_POSTGRES_WORKER_JOB_RETRY_DELAY_MS`

The queue remains disabled by default. It assumes all participating processes can access the same authoritative OpenCode SQLite/session storage; moving that projection to PostgreSQL is still outside this phase.

P7.7.1 separates tenant-aware worker coordination from the broader PostgreSQL
alpha database sidecar. A SaaS server can retain SQLite as the primary Session
projection while resolving the worker lease tenant from the PostgreSQL Session
resource binding:

- `OPENCODE_POSTGRES_WORKER_COORDINATION_ENABLED=1`
- `OPENCODE_POSTGRES_WORKER_COORDINATION_SAAS_SIDECAR=1`
- `OPENCODE_POSTGRES_WORKER_COORDINATION_TENANTS=tenant-canary`
- `OPENCODE_DATABASE_BACKEND=sqlite`
- `OPENCODE_SAAS_MODE=true`

The tenant allowlist is mandatory and evaluated for every Session acquire and
fence assertion. An allowlisted Session without a fencing token fails closed;
an unlisted tenant remains on the unchanged local execution path. The worker
queue remains disabled because its consumer is still statically tenant scoped.

The OpenCode legacy HTTP prompt path keeps its existing `SessionPrompt` loop,
message projection, model selection, and tool behavior. P7.7.1 wraps only the
`SessionRunState.ensureRunning` work boundary with the same PostgreSQL
coordination service. The first local runner acquires the lease, each model-loop
iteration validates the current fence, heartbeat loss interrupts the work, and
the terminal path completes or releases the lease. This is deliberately not a
second runner implementation.

P4.34 adds tenant-scoped queue operations without exposing a new HTTP administration surface:

- readiness and backlog metrics
- failed/cancelled job listing
- operator requeue with expected generation and claim token
- atomic `worker_job.requeue` audit rows

Operator requeue increments the requested generation and resets attempts. It rejects running jobs, stale tokens, stale generations, duplicate concurrent operators, and cross-tenant access.

P4.35 adds fail-closed operational validation. The smoke service drives a job to terminal failure, verifies degraded readiness, proves tenant isolation, races two operator requeues, verifies one winner and one audit row, then completes the recovered generation and verifies healthy readiness.

```sh
OPENCODE_DATABASE_URL=postgres://... \
OPENCODE_TENANT_ID=tenant-a \
OPENCODE_ACTOR_ID=operator-a \
OPENCODE_WORKER_QUEUE_OPERATION=readiness \
bun run --cwd packages/core postgres:worker-queue:operations
```

Use `OPENCODE_WORKER_QUEUE_OPERATION=list` to inspect recoverable jobs. Requeue additionally requires `OPENCODE_WORKER_JOB_RUN_ID`, `OPENCODE_WORKER_JOB_EXPECTED_GENERATION`, and `OPENCODE_WORKER_JOB_EXPECTED_CLAIM_TOKEN`.

P4.36 registers OpenTelemetry observable gauges and counters for queue status, expired claims, pending age, readiness, degradation, and operator outcomes. The existing OTLP setup now configures an OTLP HTTP metric reader at `/v1/metrics`. A signed management endpoint also exposes Prometheus text format.

P4.37 adds a disabled-by-default management API under `/experimental/worker-queue`. Basic Auth remains the outer server boundary. Management requests additionally require:

- per-actor HMAC signature
- timestamp and durable nonce replay protection
- tenant membership
- configured team membership and role
- two-person approval before requeue execution

Required settings:

- `OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED=1`
- `OPENCODE_WORKER_QUEUE_ADMIN_KEYS={"actor-id":"actor-specific-secret"}`
- `OPENCODE_TENANT_ID`
- `OPENCODE_TEAM_ID`
- `OPENCODE_ACTOR_ID`

Read-only endpoints permit `viewer`, `operator`, `admin`, and `owner`. Recoverable job listing and action inspection require `operator` or higher. Requeue requests require `operator` or higher. Approval requires `admin` or `owner`.

P4.38 adds a loopback-first Prometheus signing proxy and wire-level OTLP metrics validation. The proxy converts a normal Prometheus bearer-authenticated scrape into a fresh actor HMAC request for the protected OpenCode management endpoint. Every upstream request has a unique nonce and timestamp. Actor IDs and upstream errors are not exposed downstream.

Required proxy settings:

- `OPENCODE_WORKER_QUEUE_PROMETHEUS_PROXY_ENABLED=1`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_URL=http://127.0.0.1:4096`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_ID=metrics-viewer`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_ACTOR_SECRET=...`

Optional settings:

- `OPENCODE_WORKER_QUEUE_PROMETHEUS_HOST`, default `127.0.0.1`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_PORT`, default `9465`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_BEARER_TOKEN`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_USERNAME`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_UPSTREAM_PASSWORD`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_TIMEOUT_MS`
- `OPENCODE_WORKER_QUEUE_PROMETHEUS_MAX_RESPONSE_BYTES`

Binding outside loopback fails closed unless a bearer token is configured. Prometheus scrapes `/metrics`; `/healthz` checks the proxy process and `/readyz` verifies a newly signed upstream request.

```sh
bun run --cwd packages/core worker-queue:prometheus-proxy
```

OpenCode OTLP metrics continue to use `OTEL_EXPORTER_OTLP_ENDPOINT`, with queue metrics sent to `/v1/metrics`. P4.38 uses the same metric reader factory in production and release-proof smoke, which verifies an actual OTLP HTTP payload containing queue instruments and tenant attributes.

P4.39 adds an external identity adapter without trusting external tenant or role claims. OIDC and Better Auth only establish the actor ID. PostgreSQL `tenant_member`, `team_member`, forced RLS, and the existing role policy remain authoritative.

Identity modes:

- `OPENCODE_WORKER_QUEUE_IDENTITY_MODE=hmac`
- `OPENCODE_WORKER_QUEUE_IDENTITY_MODE=oidc`
- `OPENCODE_WORKER_QUEUE_IDENTITY_MODE=better-auth`
- `OPENCODE_WORKER_QUEUE_IDENTITY_MODE=hybrid`

OIDC settings:

- `OPENCODE_WORKER_QUEUE_OIDC_ISSUER`
- `OPENCODE_WORKER_QUEUE_OIDC_AUDIENCE`
- `OPENCODE_WORKER_QUEUE_OIDC_JWKS_URL`, optional discovery override
- `OPENCODE_WORKER_QUEUE_OIDC_ACTOR_CLAIM`, default `sub`
- `OPENCODE_WORKER_QUEUE_OIDC_ALGORITHMS`, default `RS256,ES256`

The adapter verifies the JWT signature, issuer, audience, expiry, not-before time, and actor claim. JWKS is cached and refreshed once when an unknown key ID is encountered.

Better Auth settings:

- `OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL`
- `OPENCODE_WORKER_QUEUE_BETTER_AUTH_SESSION_PATH`, default `/api/auth/get-session`
- `OPENCODE_WORKER_QUEUE_BETTER_AUTH_SERVICE_TOKEN_REF`, optional gateway service token

The HTTP API accepts OIDC tokens through `x-opencode-identity-token` and Better Auth sessions through the normal `cookie` header. `x-opencode-identity-provider` may explicitly select `oidc` or `better-auth`. This avoids conflict with the existing outer Basic Auth `Authorization` header.

Versioned HMAC keyrings are configured through `OPENCODE_WORKER_QUEUE_ADMIN_KEYRING_REF`. The referenced JSON contains per-actor `activeKeyID` and keys with `active`, `retiring`, or `revoked` status. Retiring keys require `notAfter`; revoked and expired keys fail closed. Clients send `x-opencode-request-key-id`. Legacy `OPENCODE_WORKER_QUEUE_ADMIN_KEYS` remains supported for migration.

Secret references support:

- `env://VARIABLE`
- `file:///mounted/secret`
- `aws-sm://region/secret-id#json-field`

Secret values and keyring manifests are TTL-cached. A failed HMAC verification forces one refresh, allowing Secret Manager rotation without restarting the OpenCode process. Configure the cache with `OPENCODE_SECRET_MANAGER_CACHE_TTL_MS`.

P4.40 adds multi-process operational governance to the management API:

- PostgreSQL fixed-window rate limits by tenant, actor, and normalized operation scope
- append-only approval `approve`, `revoke`, and `expire` events
- automatic and explicit approval timeout transitions
- owner-only break-glass requeue
- independent identity audit rows for every allow, deny, and throttle outcome

Rate limits are configured with `OPENCODE_WORKER_QUEUE_RATE_LIMITS`. The JSON object may override `observe`, `recover`, `mutate`, `approval`, and `break-glass` scopes with `limit` and `windowMs`. A rejected request returns HTTP 429 and `Retry-After`.

Approval management endpoints:

- `POST /experimental/worker-queue/actions/:actionID/approvals/:actorID/revoke`
- `POST /experimental/worker-queue/actions/expire`

Revocation requires `admin` or `owner` and a reason. Executed, rejected, and expired actions are immutable. Timeout transitions and revocations are written to both append-only approval evidence and the general audit log.

Break-glass is disabled by default. Enable it with:

- `OPENCODE_WORKER_QUEUE_BREAK_GLASS_ENABLED=1`
- `OPENCODE_WORKER_QUEUE_BREAK_GLASS_SECRET_REF=aws-sm://...`

`POST /experimental/worker-queue/actions/break-glass/requeue` requires an `owner` authenticated by OIDC or Better Auth, a valid `x-opencode-break-glass-token`, a unique incident ID, a reason, and the current generation and fencing claim token. HMAC identities cannot invoke break-glass. The operation preserves the normal worker-job CAS/fencing checks and records successful and denied emergency attempts.

P4.41 adds production acceptance gates without changing the Session runner or queue execution loop:

- independent processes race one shared PostgreSQL rate-limit budget
- duplicate approval processes race one action and must produce exactly two approval events and one execution
- worker lease and worker job owners are killed repeatedly; takeover tokens must increase and stale effects or completions must remain fenced
- a live OIDC or Better Auth credential is resolved from a real `aws-sm://` or mounted `file://` Secret Manager reference and bound to PostgreSQL membership

Run the multi-instance soak with:

```sh
OPENCODE_POSTGRES_WORKER_QUEUE_MULTI_INSTANCE_SOAK=1 \
OPENCODE_DATABASE_URL=postgres://... \
bun run --cwd packages/core test:postgres-worker-queue-multi-instance-soak
```

Tune it with `OPENCODE_P4_41_SOAK_CONCURRENCY` (default `12`) and `OPENCODE_P4_41_SOAK_ROUNDS` (default `2`).

The live integration gate requires:

- `OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER=oidc|better-auth`
- `OPENCODE_P4_41_LIVE_ACTOR_ID`
- `OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF=aws-sm://...|file://...`
- the corresponding P4.39 OIDC or Better Auth endpoint settings

Set `OPENCODE_P4_41_LIVE_INTEGRATION_REQUIRED=1` while generating a production ReleaseProof. A SaaS startup rejects a proof generated without that marker. `OPENCODE_P4_41_LIVE_SECRET_SCHEMES=aws-sm` can prohibit mounted-file credentials in environments that require a remote Secret Manager.

P4.42 separates developer live integration from cloud production evidence. A SaaS ReleaseProof must set `OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED=1`; this automatically requires:

- `OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER=oidc`
- a non-loopback HTTPS `OPENCODE_WORKER_QUEUE_OIDC_ISSUER`
- `OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF=aws-sm://...`
- valid cloud credentials for AWS Secrets Manager and a live OIDC token whose actor matches `OPENCODE_P4_41_LIVE_ACTOR_ID`

Better Auth on loopback and mounted `file://` secrets remain valid for P4.41 integration testing but cannot satisfy P4.42 SaaS startup. The cloud contract smoke verifies strict policy selection, forced-refresh rotation behavior, and fail-closed AWS dependency outages without claiming that a simulated adapter is live cloud evidence:

```sh
bun run --cwd packages/core test:worker-queue-cloud-contract
```

## P5.0: deferred cloud acceptance seal

The external KMS and HTTPS OIDC acceptance work is intentionally deferred.
No new server or paid KMS resource is required while this seal is active.

- `OPENCODE_SAAS_CLOUD_ACCEPTANCE` defaults to `deferred`.
- Development and self-hosted execution continue through their existing gates.
- SaaS production startup remains fail-closed with
  `saas-cloud-acceptance-deferred`; the seal never converts local or file-backed
  identity/secret evidence into production cloud proof.
- An invalid mode fails closed with `saas-cloud-acceptance-mode-invalid`.
- Set `OPENCODE_SAAS_CLOUD_ACCEPTANCE=required` only when resuming P4.42. This
  restores the `saas-startup-cloud-proof-required` gate and requires real remote
  HTTPS identity plus cloud Secret Manager evidence.
- The existing ECS RAM role may remain attached while deferred. It has no secret
  to read and does not represent completed cloud acceptance.
