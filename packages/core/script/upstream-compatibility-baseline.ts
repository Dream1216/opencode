import { resolve } from "node:path"
import {
  loadUpstreamCompatibilityManifest,
  runUpstreamCompatibilityBaseline,
} from "../src/compatibility/upstream-baseline"

const args = new Map<string, string>()
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index]
  const value = process.argv[index + 1]
  if (!key?.startsWith("--") || value === undefined) throw new Error(`Invalid argument near ${key ?? "<end>"}`)
  args.set(key.slice(2), value)
}

const repoRoot = resolve(import.meta.dir, "../../..")
const manifestPath = resolve(
  args.get("manifest") ?? resolve(import.meta.dir, "../compatibility/opencode-upstream-baseline.json"),
)
const baseline = args.get("baseline")
const candidate = args.get("candidate")
const local = resolve(args.get("local") ?? repoRoot)

if (!baseline) throw new Error("--baseline is required")
if (!candidate) throw new Error("--candidate is required")

const manifest = await loadUpstreamCompatibilityManifest(manifestPath)
const report = await runUpstreamCompatibilityBaseline({
  manifest,
  baselineUpstreamRoot: resolve(baseline),
  candidateUpstreamRoot: resolve(candidate),
  localRoot: local,
})

console.log(JSON.stringify(report, null, 2))
if (report.status === "blocked") process.exitCode = 1
