import { createHash } from "node:crypto"
import { cp, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join, relative, resolve, sep } from "node:path"

const baselineRoot = requiredRoot("OPENCODE_UPSTREAM_BASELINE_ROOT")
const candidateRoot = requiredRoot("OPENCODE_UPSTREAM_CANDIDATE_ROOT")
const localRoot = resolve(process.env.OPENCODE_LOCAL_ROOT ?? join(import.meta.dir, "../../.."))

const toolsPath = "packages/opencode/src/session/tools.ts"
const pinned = {
  baseline: "ec76a577f6f68d44c6202dcd3fae1d3b859c8a3bda603939364048ce033be095",
  candidate: "9020dac499b135086258007c5352d94327cee75f624a7694b4d1156c82f0710c",
}

const baselineHash = await hash(join(baselineRoot, toolsPath))
const candidateHash = await hash(join(candidateRoot, toolsPath))
const localHash = await hash(join(localRoot, toolsPath))
if (baselineHash !== pinned.baseline || candidateHash !== pinned.candidate) {
  throw new Error(`upstream tools.ts hash drift: baseline=${baselineHash} candidate=${candidateHash}`)
}
if (localHash !== baselineHash) throw new Error(`local tools.ts already diverged from v1.18.7: local=${localHash}`)

const scopes = [
  "packages/core/src",
  "packages/schema/src",
  "packages/opencode/src",
  "packages/opencode/test",
  "patches",
]
const baselineFiles = await collectFiles(baselineRoot, scopes)
const candidateFiles = await collectFiles(candidateRoot, scopes)
const paths = [...new Set([...baselineFiles, ...candidateFiles])].sort()
const changes: Array<{ path: string; kind: "added" | "modified" | "deleted" }> = []

for (const path of paths) {
  const inBaseline = baselineFiles.has(path)
  const inCandidate = candidateFiles.has(path)
  if (!inBaseline) changes.push({ path, kind: "added" })
  else if (!inCandidate) changes.push({ path, kind: "deleted" })
  else if ((await hash(join(baselineRoot, path))) !== (await hash(join(candidateRoot, path)))) {
    changes.push({ path, kind: "modified" })
  }
}
if (!changes.some((change) => change.path === toolsPath)) {
  throw new Error(`${toolsPath} is absent from the official candidate diff`)
}

const overlaps: string[] = []
for (const change of changes) {
  const localPath = join(localRoot, change.path)
  if (change.kind === "added") {
    if (await exists(localPath)) overlaps.push(change.path)
    continue
  }
  if (!(await exists(localPath)) || (await hash(localPath)) !== (await hash(join(baselineRoot, change.path)))) {
    overlaps.push(change.path)
  }
}
if (overlaps.length > 0) throw new Error(`candidate changes overlap local modifications:\n${overlaps.join("\n")}`)

const baselineRepositoryFiles = await collectFiles(baselineRoot, ["."])
const candidateRepositoryFiles = await collectFiles(candidateRoot, ["."])
const packageManifests: string[] = []
for (const path of [...new Set([...baselineRepositoryFiles, ...candidateRepositoryFiles])]) {
  if (path !== "package.json" && !path.endsWith("/package.json")) continue
  if (!baselineRepositoryFiles.has(path) || !candidateRepositoryFiles.has(path)) {
    packageManifests.push(path)
    continue
  }
  if ((await hash(join(baselineRoot, path))) !== (await hash(join(candidateRoot, path)))) {
    packageManifests.push(path)
  }
}

const rehearsalRoot = await mkdtemp(join(tmpdir(), "opencode-v1.18.8-rehearsal-"))
await run(["rsync", "-a", "--exclude", ".git", "--exclude", "node_modules", "--exclude", "dist", "--exclude", ".turbo", `${localRoot}/`, `${rehearsalRoot}/`], localRoot)

for (const change of changes) {
  const destination = join(rehearsalRoot, change.path)
  if (change.kind === "deleted") {
    await rm(destination, { force: true })
    continue
  }
  await mkdir(dirname(destination), { recursive: true })
  await cp(join(candidateRoot, change.path), destination)
}

const manifestConflicts: string[] = []
for (const path of packageManifests) {
  const baselinePath = join(baselineRoot, path)
  const candidatePath = join(candidateRoot, path)
  const rehearsalPath = join(rehearsalRoot, path)
  if (!(await exists(baselinePath))) {
    if (await exists(rehearsalPath)) manifestConflicts.push(`${path}: candidate adds an existing local manifest`)
    else {
      await mkdir(dirname(rehearsalPath), { recursive: true })
      await cp(candidatePath, rehearsalPath)
    }
    continue
  }
  if (!(await exists(candidatePath))) {
    if ((await exists(rehearsalPath)) && (await hash(rehearsalPath)) === (await hash(baselinePath))) {
      await rm(rehearsalPath, { force: true })
    } else manifestConflicts.push(`${path}: candidate deletes a locally changed manifest`)
    continue
  }
  if (!(await exists(rehearsalPath))) {
    manifestConflicts.push(`${path}: local manifest is missing`)
    continue
  }
  const baseline = JSON.parse(await readFile(baselinePath, "utf8"))
  const candidate = JSON.parse(await readFile(candidatePath, "utf8"))
  const local = JSON.parse(await readFile(rehearsalPath, "utf8"))
  const merged = mergeCandidateJson(baseline, candidate, local, path, manifestConflicts)
  await writeFile(rehearsalPath, `${JSON.stringify(merged, null, 2)}\n`)
}
if (manifestConflicts.length > 0) throw new Error(`package manifest merge conflicts:\n${manifestConflicts.join("\n")}`)

const checks = [
  ["candidate-dependencies", ["bun", "install", "--ignore-scripts"], "."],
  ["core-typecheck", ["bun", "run", "--cwd", "packages/core", "typecheck"], "."],
  ["schema-typecheck", ["bun", "run", "--cwd", "packages/schema", "typecheck"], "."],
  ["opencode-typecheck", ["bun", "run", "--cwd", "packages/opencode", "typecheck"], "."],
  [
    "session-tests",
    [
      "bun",
      "test",
      "test/session-prompt.test.ts",
      "test/session-runner-recorded.test.ts",
      "test/session-runner.test.ts",
      "--timeout",
      "30000",
    ],
    "packages/core",
  ],
  [
    "candidate-tool-tests",
    [
      "bun",
      "test",
      "test/tool/registry.test.ts",
      "test/tool/code-mode.test.ts",
      "test/tool/code-mode-integration.test.ts",
      "--timeout",
      "30000",
    ],
    "packages/opencode",
  ],
  ["shadow-agent-run", ["bun", "run", "--cwd", "packages/opencode", "test:shadow-agent-run"], "."],
  ["tool-governance", ["bun", "run", "--cwd", "packages/opencode", "test:tool-governance"], "."],
  [
    "shadow-canary-executor",
    [
      "bun",
      "test",
      "test/shadow-canary.test.ts",
      "test/shadow-canary-executor.test.ts",
      "test/shadow-canary-v1-18-8-model-executor.test.ts",
      "--timeout",
      "60000",
    ],
    "packages/core",
  ],
] as const

const results: Array<{ name: string; status: "passed" }> = []
try {
  process.env.OPENCODE_SHADOW_CANDIDATE_CHECKOUT = rehearsalRoot
  for (const [name, command, cwd] of checks) {
    await run([...command], join(rehearsalRoot, cwd))
    results.push({ name, status: "passed" })
  }
  console.log(
    JSON.stringify(
      {
        status: "passed",
        mode: "no-merge",
        candidate: "v1.18.8",
        changedFiles: changes,
        overlapCount: overlaps.length,
        mergedPackageManifests: packageManifests,
        checks: results,
      },
      null,
      2,
    ),
  )
} finally {
  if (process.env.OPENCODE_KEEP_REHEARSAL !== "1") await rm(rehearsalRoot, { recursive: true, force: true })
}

function requiredRoot(name: string) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return resolve(value)
}

async function collectFiles(root: string, inputScopes: readonly string[]) {
  const files = new Set<string>()
  for (const scope of inputScopes) await walk(join(root, scope), root, files)
  return files
}

async function walk(path: string, root: string, files: Set<string>): Promise<void> {
  let info
  try {
    info = await stat(path)
  } catch {
    return
  }
  if (info.isFile()) {
    files.add(normalize(relative(root, path)))
    return
  }
  if (!info.isDirectory()) return
  for (const entry of await readdir(path, { withFileTypes: true })) await walk(join(path, entry.name), root, files)
}

async function exists(path: string) {
  try {
    await stat(path)
    return true
  } catch {
    return false
  }
}

async function hash(path: string) {
  return createHash("sha256").update(await readFile(path)).digest("hex")
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

function normalize(path: string) {
  return sep === "/" ? path : path.split(sep).join("/")
}

async function run(command: string[], cwd: string) {
  const child = Bun.spawn(command, {
    cwd,
    env: { ...process.env, OPENCODE_SHADOW_CANDIDATE_CHECKOUT: process.env.OPENCODE_SHADOW_CANDIDATE_CHECKOUT },
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  })
  const code = await child.exited
  if (code !== 0) throw new Error(`${command.join(" ")} exited with ${code}`)
}
