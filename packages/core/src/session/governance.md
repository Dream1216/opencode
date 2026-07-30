# Session governance sidecar

P4 adds a SaaS governance contract around sessions without replacing OpenCode's local storage backend.

Durable events:

- `session.next.tenant.bound`
- `session.next.rls.evaluated`
- `session.next.audit.recorded`
- `session.next.release.governance.evaluated`

Tenant context is read from environment variables when present:

- `OPENCODE_TENANT_ID`
- `OPENCODE_TEAM_ID`
- `OPENCODE_ACTOR_ID`
- `OPENCODE_RELEASE_ENV`
- `OPENCODE_SAAS_RELEASE`

If no tenant is configured, the sidecar emits `tenant_local` with `single-tenant-local` mode.

RLS is intentionally recorded as `logical-sqlite` in this phase. It provides an explicit policy/audit boundary while avoiding a false claim of PostgreSQL RLS enforcement. A later backend swap can preserve the same events and implement `postgres-rls` enforcement with real tenant roles and policy checks.

Prompt-level governance must not be inserted between `prompt.admitted` and `prompted` events because OpenCode uses the durable sequence between those events as its inbox promotion boundary.

P4.1 attaches prompt-level RLS/audit after all `prompted` events in a promotion batch have been published. This preserves the prompt promotion sequence while still giving each promoted message a durable `session.prompt.prompted` governance record with `admittedSeq` and `promotedSeq` metadata.
