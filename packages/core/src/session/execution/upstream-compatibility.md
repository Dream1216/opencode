# OpenCode upstream compatibility baseline

P5.1 establishes an upgrade compatibility gate without merging, rebasing, or
replacing the current OpenCode checkout.

## Frozen baseline

- Repository: `anomalyco/opencode`
- Local baseline: `v1.18.7`
- Baseline commit: `02981844b88aed33f06f1527da6c58d137975069`
- Observed candidate: `v1.18.8`
- Candidate tag commit: `3c81a5d1ddceab377d9ad71c14899e6935333fdd`
- Observation date: `2026-07-28`

The machine-readable contract is in
`packages/core/compatibility/opencode-upstream-baseline.json`.

## What the gate checks

1. The baseline and candidate package versions match the manifest.
2. Official EventV2, prompt admission, SessionExecution, permission, model, and
   tool hook anchors still exist in the candidate.
3. Local workflow, governance, durable worker, ModelInvocationGateway,
   ReleaseGate, shadow AgentRun, and tool governance anchors still exist.
4. Local modifications stay inside the frozen file allowlist.
5. New files stay in sidecar/adapter directories or the explicit addition list.
6. Existing modifications do not exceed their frozen changed-line budgets.
7. Changes to protected official seams are classified as manual review or
   blockers before any upgrade work starts.

## Status meanings

- `ready`: no protected upstream drift and no local boundary violation.
- `review`: anchors remain compatible, but at least one protected upstream seam
  changed and requires a human patch review.
- `blocked`: a required anchor disappeared, a blocker seam changed, a local
  file left the allowlist, or a changed-line budget grew.

`review` is intentionally non-zero risk but does not fail the scanner process.
`blocked` exits with status 1.

## Run against official source archives

```bash
OPENCODE_UPSTREAM_BASELINE_SOURCE=/tmp/opencode-v1.18.7 \
OPENCODE_UPSTREAM_CANDIDATE_SOURCE=/tmp/opencode-v1.18.8 \
OPENCODE_LOCAL_SOURCE=/path/to/local/opencode \
bun packages/core/script/upstream-compatibility-smoke.ts
```

The generic scanner is:

```bash
bun packages/core/script/upstream-compatibility-baseline.ts \
  --baseline /tmp/opencode-v1.18.7 \
  --candidate /tmp/opencode-v1.18.8 \
  --local /path/to/local/opencode
```

## Current v1.18.8 result

The candidate keeps all required extension anchors. The official
`packages/opencode/src/session/tools.ts` file changed in v1.18.8, so the expected
status is `review`, not automatic upgrade approval. MCP, provider transform, and
tool registry release changes must be reviewed before applying the patch.

## Upgrade exit gate

Do not upgrade the working checkout until all of the following pass:

1. Compatibility status is `ready`, or every `review` finding is resolved.
2. Core, schema, and OpenCode package typechecks pass.
3. The targeted session regression suite passes with `108/108`.
4. Event sequence, durable worker fencing, permission governance, and release
   gate behavioral tests pass.
5. No new main-loop modification or patch-budget increase is accepted without
   an explicit architecture decision.
