import { resolve } from "node:path"
import {
  loadUpstreamCompatibilityManifest,
  runUpstreamCompatibilityBaseline,
} from "../src/compatibility/upstream-baseline"

const baseline = process.env.OPENCODE_UPSTREAM_BASELINE_SOURCE
const candidate = process.env.OPENCODE_UPSTREAM_CANDIDATE_SOURCE
const local = process.env.OPENCODE_LOCAL_SOURCE

if (!baseline) throw new Error("OPENCODE_UPSTREAM_BASELINE_SOURCE is required")
if (!candidate) throw new Error("OPENCODE_UPSTREAM_CANDIDATE_SOURCE is required")
if (!local) throw new Error("OPENCODE_LOCAL_SOURCE is required")

const manifest = await loadUpstreamCompatibilityManifest(
  resolve(import.meta.dir, "../compatibility/opencode-upstream-baseline.json"),
)
const report = await runUpstreamCompatibilityBaseline({
  manifest,
  baselineUpstreamRoot: resolve(baseline),
  candidateUpstreamRoot: resolve(candidate),
  localRoot: resolve(local),
})

assert(report.status === "ready", `Expected v1.18.8 compatibility status ready, received ${report.status}`)
assert(report.blockers.length === 0, `Expected no blockers, received ${report.blockers.length}`)
assert(report.candidate.packageVersion === "1.18.8", "Expected the real v1.18.8 archive")
assert(report.reviews.length === 0, `Expected no unresolved reviews, received ${report.reviews.length}`)
assert(
  report.summary.resolvedProtectedUpstreamChanges === 0,
  `Expected no protected upstream drift on the active baseline, received ${report.summary.resolvedProtectedUpstreamChanges}`,
)

console.log(
  JSON.stringify(
    {
      status: "ok",
      checks: [
        "official-baseline-version-verified",
        "candidate-version-verified",
        "upstream-seam-contracts-preserved",
        "local-extension-seams-preserved",
        "patch-surface-allowlist-preserved",
        "patch-line-budgets-preserved",
        "active-v1.18.8-baseline-has-no-unresolved-upstream-drift",
      ],
      report,
    },
    null,
    2,
  ),
)

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}
