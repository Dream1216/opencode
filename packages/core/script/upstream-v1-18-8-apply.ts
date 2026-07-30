import { createHash } from "node:crypto"
import { cp, lstat, mkdir, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises"
import { dirname, join, relative, resolve } from "node:path"

const args = new Map<string, string>()
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index]
  const value = process.argv[index + 1]
  if (!key?.startsWith("--") || value === undefined) throw new Error(`invalid argument near ${key ?? "<end>"}`)
  args.set(key.slice(2), value)
}

const baselineRoot = required("baseline")
const candidateRoot = required("candidate")
const localRoot = resolve(args.get("local") ?? join(import.meta.dir, "../../.."))
const backupRoot = resolve(
  args.get("backup") ??
    join(
      "/tmp",
      `opencode-v1.18.7-preupgrade-${new Date().toISOString().replaceAll(":", "").replaceAll(".", "")}`,
    ),
)
const ignored = new Set([".git", "node_modules", "dist", ".turbo"])
const extraBackupPaths = [
  "packages/core/compatibility/opencode-upstream-baseline.json",
  "packages/core/script/upstream-compatibility-smoke.ts",
  "packages/core/src/session/runner/shadow-canary.ts",
]

assertVersion(baselineRoot, "1.18.7")
assertVersion(candidateRoot, "1.18.8")

const [baseline, candidate, local] = await Promise.all([
  collect(baselineRoot),
  collect(candidateRoot),
  collect(localRoot),
])
const paths = [...new Set([...baseline.keys(), ...candidate.keys()])].sort()
const changes = paths
  .filter((path) => baseline.get(path) !== candidate.get(path))
  .map((path) => ({
    path,
    kind: !baseline.has(path) ? "added" : !candidate.has(path) ? "deleted" : "modified",
  })) satisfies Array<{ path: string; kind: "added" | "modified" | "deleted" }>
const manifests = changes
  .map((change) => change.path)
  .filter((path) => path === "package.json" || path.endsWith("/package.json"))
const ordinary = changes.filter((change) => change.path !== "bun.lock" && !manifests.includes(change.path))
const conflicts: string[] = []
const mergedManifests = new Map<string, string>()

for (const change of ordinary) {
  const baselineSignature = baseline.get(change.path)
  const candidateSignature = candidate.get(change.path)
  const localSignature = local.get(change.path)
  if (baselineSignature === undefined) {
    if (localSignature !== undefined && localSignature !== candidateSignature) {
      conflicts.push(`${change.path}: candidate adds over a local file`)
    }
    continue
  }
  if (candidateSignature === undefined) {
    if (localSignature !== undefined && localSignature !== baselineSignature) {
      conflicts.push(`${change.path}: candidate deletes a locally changed file`)
    }
    continue
  }
  if (localSignature !== baselineSignature && localSignature !== candidateSignature) {
    conflicts.push(`${change.path}: official and local changes overlap`)
  }
}

for (const path of manifests) {
  const baselinePath = join(baselineRoot, path)
  const candidatePath = join(candidateRoot, path)
  const localPath = join(localRoot, path)
  const mergeConflicts: string[] = []
  const merged = mergeCandidateJson(
    JSON.parse(await readFile(baselinePath, "utf8")),
    JSON.parse(await readFile(candidatePath, "utf8")),
    JSON.parse(await readFile(localPath, "utf8")),
    path,
    mergeConflicts,
  )
  conflicts.push(...mergeConflicts)
  mergedManifests.set(path, `${JSON.stringify(merged, null, 2)}\n`)
}

if (conflicts.length > 0) throw new Error(`v1.18.8 upgrade conflicts:\n${conflicts.join("\n")}`)

await mkdir(backupRoot, { recursive: false, mode: 0o700 })
const backupPaths = [...new Set([...changes.map((change) => change.path), ...extraBackupPaths])].sort()
const absentBeforeUpgrade: string[] = []
for (const path of backupPaths) {
  const source = join(localRoot, path)
  if (!(await exists(source))) {
    absentBeforeUpgrade.push(path)
    continue
  }
  const destination = join(backupRoot, "files", path)
  await mkdir(dirname(destination), { recursive: true, mode: 0o700 })
  await cp(source, destination, {
    recursive: true,
    force: true,
    preserveTimestamps: true,
    verbatimSymlinks: true,
  })
}

await writeFile(
  join(backupRoot, "upgrade-plan.json"),
  `${JSON.stringify(
    {
      from: "v1.18.7",
      to: "v1.18.8",
      localRoot,
      changes,
      manifests,
      absentBeforeUpgrade,
      createdAt: new Date().toISOString(),
    },
    null,
    2,
  )}\n`,
  { mode: 0o600 },
)

for (const change of ordinary) {
  const destination = join(localRoot, change.path)
  if (change.kind === "deleted") {
    await rm(destination, { recursive: true, force: true })
    continue
  }
  await mkdir(dirname(destination), { recursive: true })
  await cp(join(candidateRoot, change.path), destination, {
    recursive: true,
    force: true,
    preserveTimestamps: true,
    verbatimSymlinks: true,
  })
}
for (const [path, content] of mergedManifests) await writeFile(join(localRoot, path), content)

const historicalManifest = join(
  localRoot,
  "packages/core/compatibility/opencode-upstream-v1.18.7-to-v1.18.8.json",
)
if (!(await exists(historicalManifest))) {
  await cp(
    join(localRoot, "packages/core/compatibility/opencode-upstream-baseline.json"),
    historicalManifest,
    { force: true },
  )
}

console.log(
  JSON.stringify(
    {
      status: "applied",
      from: "v1.18.7",
      to: "v1.18.8",
      backupRoot,
      changes: changes.length,
      ordinaryChanges: ordinary.length,
      mergedManifests: manifests.length,
      lockfileRegenerationRequired: changes.some((change) => change.path === "bun.lock"),
      conflicts: 0,
    },
    null,
    2,
  ),
)

function required(name: string) {
  const value = args.get(name)
  if (!value) throw new Error(`--${name} is required`)
  return resolve(value)
}

async function collect(root: string) {
  const result = new Map<string, string>()
  const walk = async (path: string): Promise<void> => {
    for (const entry of await readdir(path, { withFileTypes: true })) {
      if (ignored.has(entry.name)) continue
      const absolute = join(path, entry.name)
      const item = relative(root, absolute)
      if (entry.isDirectory()) {
        await walk(absolute)
        continue
      }
      if (entry.isSymbolicLink()) {
        result.set(item, `link:${await readlink(absolute)}`)
        continue
      }
      if (entry.isFile()) result.set(item, `file:${sha256(await readFile(absolute))}`)
    }
  }
  await walk(root)
  return result
}

async function assertVersion(root: string, expected: string) {
  const value = JSON.parse(await readFile(join(root, "packages/opencode/package.json"), "utf8")) as {
    version?: unknown
  }
  if (value.version !== expected) throw new Error(`${root} must contain OpenCode ${expected}`)
}

function mergeCandidateJson(
  baseline: unknown,
  candidate: unknown,
  local: unknown,
  path: string,
  conflicts: string[],
): unknown {
  if (sameJson(baseline, candidate)) return local
  if (isRecord(baseline) && isRecord(candidate) && isRecord(local)) {
    const result: Record<string, unknown> = { ...local }
    for (const key of new Set([...Object.keys(baseline), ...Object.keys(candidate)])) {
      const hasBaseline = Object.hasOwn(baseline, key)
      const hasCandidate = Object.hasOwn(candidate, key)
      const hasLocal = Object.hasOwn(local, key)
      const keyPath = `${path}.${key}`
      if (!hasCandidate) {
        if (!hasLocal || sameJson(local[key], baseline[key])) delete result[key]
        else conflicts.push(`${keyPath}: candidate deletes a locally changed value`)
        continue
      }
      if (!hasBaseline) {
        if (!hasLocal || sameJson(local[key], candidate[key])) result[key] = candidate[key]
        else conflicts.push(`${keyPath}: candidate adds a conflicting local value`)
        continue
      }
      result[key] = mergeCandidateJson(baseline[key], candidate[key], local[key], keyPath, conflicts)
    }
    return result
  }
  if (sameJson(local, baseline) || sameJson(local, candidate)) return candidate
  conflicts.push(`${path}: baseline, candidate, and local values all differ`)
  return local
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function sameJson(left: unknown, right: unknown) {
  return JSON.stringify(left) === JSON.stringify(right)
}

function sha256(value: Uint8Array) {
  return createHash("sha256").update(value).digest("hex")
}

async function exists(path: string) {
  try {
    await lstat(path)
    return true
  } catch {
    return false
  }
}
