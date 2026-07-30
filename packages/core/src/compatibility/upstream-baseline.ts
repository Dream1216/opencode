import { spawnSync } from "node:child_process"
import { createHash } from "node:crypto"
import { readdir, readFile, stat } from "node:fs/promises"
import { dirname, join, relative, resolve, sep } from "node:path"

type Seam = {
  readonly name: string
  readonly path: string
  readonly anchors: readonly string[]
}

type ProtectedPath = {
  readonly path: string
  readonly severity: "blocker" | "review"
}

type ReviewResolution = {
  readonly path: string
  readonly baselineTag: string
  readonly candidateTag: string
  readonly baselineSha256: string
  readonly candidateSha256: string
  readonly resolution: "compatible"
  readonly evidence: string
}

export type UpstreamCompatibilityManifest = {
  readonly schemaVersion: 1
  readonly repository: string
  readonly baseline: {
    readonly tag: string
    readonly commit: string
    readonly packageVersion: string
  }
  readonly observedCandidate: {
    readonly tag: string
    readonly commit: string
    readonly packageVersion: string
    readonly publishedAt: string
  }
  readonly sourceScopes: readonly string[]
  readonly upstreamSeams: readonly Seam[]
  readonly localSeams: readonly Seam[]
  readonly protectedUpstreamPaths: readonly ProtectedPath[]
  readonly reviewResolutions?: readonly ReviewResolution[]
  readonly allowedModifiedFiles: readonly string[]
  readonly allowedAddedPaths: readonly string[]
  readonly allowedAddedPrefixes: readonly string[]
  readonly modifiedLineBudgets: Readonly<Record<string, number>>
}

type Finding = {
  readonly kind: string
  readonly path?: string
  readonly detail: string
}

export type UpstreamCompatibilityReport = {
  readonly status: "ready" | "review" | "blocked"
  readonly repository: string
  readonly baseline: UpstreamCompatibilityManifest["baseline"]
  readonly candidate: {
    readonly packageVersion: string
    readonly expectedVersion: string
  }
  readonly summary: {
    readonly upstreamSeamsPassed: number
    readonly localSeamsPassed: number
    readonly localAddedFiles: number
    readonly localModifiedFiles: number
    readonly localMissingFiles: number
    readonly protectedUpstreamChanges: number
    readonly resolvedProtectedUpstreamChanges: number
  }
  readonly blockers: readonly Finding[]
  readonly reviews: readonly Finding[]
}

export async function loadUpstreamCompatibilityManifest(path: string) {
  const value = JSON.parse(await readFile(path, "utf8")) as UpstreamCompatibilityManifest
  if (value.schemaVersion !== 1) throw new Error(`Unsupported compatibility manifest schema: ${value.schemaVersion}`)
  return value
}

export async function runUpstreamCompatibilityBaseline(input: {
  readonly manifest: UpstreamCompatibilityManifest
  readonly baselineUpstreamRoot: string
  readonly candidateUpstreamRoot: string
  readonly localRoot: string
}): Promise<UpstreamCompatibilityReport> {
  const baselineRoot = resolve(input.baselineUpstreamRoot)
  const candidateRoot = resolve(input.candidateUpstreamRoot)
  const localRoot = resolve(input.localRoot)
  const blockers: Finding[] = []
  const reviews: Finding[] = []

  const baselineVersion = await packageVersion(baselineRoot)
  if (baselineVersion !== input.manifest.baseline.packageVersion) {
    blockers.push({
      kind: "baseline-version-mismatch",
      detail: `Expected ${input.manifest.baseline.packageVersion}, received ${baselineVersion}`,
    })
  }

  const candidateVersion = await packageVersion(candidateRoot)
  if (candidateVersion !== input.manifest.observedCandidate.packageVersion) {
    reviews.push({
      kind: "candidate-version-drift",
      detail: `Manifest expects ${input.manifest.observedCandidate.packageVersion}, received ${candidateVersion}`,
    })
  }

  const upstreamSeamsPassed = await checkSeams(candidateRoot, input.manifest.upstreamSeams, blockers, "upstream")
  const localSeamsPassed = await checkSeams(localRoot, input.manifest.localSeams, blockers, "local")

  let protectedUpstreamChanges = 0
  let resolvedProtectedUpstreamChanges = 0
  for (const item of input.manifest.protectedUpstreamPaths) {
    const baselinePath = join(baselineRoot, item.path)
    const candidatePath = join(candidateRoot, item.path)
    if (!(await sameFile(baselinePath, candidatePath))) {
      protectedUpstreamChanges += 1
      const resolution = input.manifest.reviewResolutions?.find(
        (entry) =>
          entry.path === item.path &&
          entry.baselineTag === input.manifest.baseline.tag &&
          entry.candidateTag === input.manifest.observedCandidate.tag,
      )
      if (
        item.severity === "review" &&
        resolution &&
        (await sha256(baselinePath)) === resolution.baselineSha256 &&
        (await sha256(candidatePath)) === resolution.candidateSha256
      ) {
        resolvedProtectedUpstreamChanges += 1
        continue
      }
      const finding = {
        kind: "protected-upstream-drift",
        path: item.path,
        detail: `${item.path} changed between ${input.manifest.baseline.tag} and candidate ${candidateVersion}`,
      }
      if (item.severity === "blocker") blockers.push(finding)
      else reviews.push(finding)
    }
  }

  const baselineFiles = await collectFiles(baselineRoot, input.manifest.sourceScopes)
  const localFiles = await collectFiles(localRoot, input.manifest.sourceScopes)
  const allPaths = new Set([...baselineFiles, ...localFiles])
  const allowedModified = new Set(input.manifest.allowedModifiedFiles)
  const allowedAdded = new Set(input.manifest.allowedAddedPaths)
  let localAddedFiles = 0
  let localModifiedFiles = 0
  let localMissingFiles = 0

  for (const path of [...allPaths].sort()) {
    const inBaseline = baselineFiles.has(path)
    const inLocal = localFiles.has(path)
    if (!inBaseline && inLocal) {
      localAddedFiles += 1
      if (!allowedAdded.has(path) && !input.manifest.allowedAddedPrefixes.some((prefix) => under(path, prefix))) {
        blockers.push({ kind: "unapproved-added-file", path, detail: `${path} is outside the sidecar allowlist` })
      }
      continue
    }
    if (inBaseline && !inLocal) {
      localMissingFiles += 1
      blockers.push({ kind: "upstream-file-missing", path, detail: `${path} was removed from the local checkout` })
      continue
    }
    if (await sameFile(join(baselineRoot, path), join(localRoot, path))) continue
    localModifiedFiles += 1
    if (!allowedModified.has(path)) {
      blockers.push({ kind: "unapproved-modified-file", path, detail: `${path} is outside the frozen patch surface` })
      continue
    }
    const budget = input.manifest.modifiedLineBudgets[path]
    if (budget === undefined) continue
    const changed = changedLineCount(join(baselineRoot, path), join(localRoot, path))
    if (changed > budget) {
      blockers.push({
        kind: "patch-budget-exceeded",
        path,
        detail: `${path} uses ${changed} changed lines; frozen budget is ${budget}`,
      })
    }
  }

  return {
    status: blockers.length > 0 ? "blocked" : reviews.length > 0 ? "review" : "ready",
    repository: input.manifest.repository,
    baseline: input.manifest.baseline,
    candidate: {
      packageVersion: candidateVersion,
      expectedVersion: input.manifest.observedCandidate.packageVersion,
    },
    summary: {
      upstreamSeamsPassed,
      localSeamsPassed,
      localAddedFiles,
      localModifiedFiles,
      localMissingFiles,
      protectedUpstreamChanges,
      resolvedProtectedUpstreamChanges,
    },
    blockers,
    reviews,
  }
}

async function packageVersion(root: string) {
  const value = JSON.parse(await readFile(join(root, "packages/opencode/package.json"), "utf8")) as { version?: unknown }
  if (typeof value.version !== "string") throw new Error(`Missing opencode package version under ${root}`)
  return value.version
}

async function checkSeams(root: string, seams: readonly Seam[], blockers: Finding[], kind: string) {
  let passed = 0
  for (const seam of seams) {
    const path = join(root, seam.path)
    let content: string
    try {
      content = await readFile(path, "utf8")
    } catch {
      blockers.push({ kind: `${kind}-seam-file-missing`, path: seam.path, detail: `${seam.name} file is missing` })
      continue
    }
    const missing = seam.anchors.filter((anchor) => !content.includes(anchor))
    if (missing.length > 0) {
      blockers.push({
        kind: `${kind}-seam-anchor-missing`,
        path: seam.path,
        detail: `${seam.name} is missing anchors: ${missing.join(", ")}`,
      })
      continue
    }
    passed += 1
  }
  return passed
}

async function collectFiles(root: string, scopes: readonly string[]) {
  const files = new Set<string>()
  for (const scope of scopes) {
    const absolute = join(root, scope)
    await walk(absolute, async (file) => {
      files.add(normalize(relative(root, file)))
    })
  }
  return files
}

async function walk(path: string, visit: (path: string) => Promise<void>): Promise<void> {
  let info
  try {
    info = await stat(path)
  } catch {
    return
  }
  if (info.isFile()) {
    await visit(path)
    return
  }
  if (!info.isDirectory()) return
  const entries = await readdir(path, { withFileTypes: true })
  for (const entry of entries) {
    await walk(join(path, entry.name), visit)
  }
}

async function sameFile(left: string, right: string) {
  try {
    const [a, b] = await Promise.all([readFile(left), readFile(right)])
    return a.equals(b)
  } catch {
    return false
  }
}

async function sha256(path: string) {
  try {
    return createHash("sha256").update(await readFile(path)).digest("hex")
  } catch {
    return undefined
  }
}

function changedLineCount(left: string, right: string) {
  const result = spawnSync("diff", ["-U0", left, right], { encoding: "utf8" })
  if (result.status === 0) return 0
  if (result.status !== 1) throw new Error(result.stderr || `diff failed for ${left} and ${right}`)
  return result.stdout
    .split("\n")
    .filter((line) => (/^\+[^+]/.test(line) || /^-[^-]/.test(line)))
    .length
}

function under(path: string, prefix: string) {
  return path === prefix || path.startsWith(`${prefix}/`)
}

function normalize(path: string) {
  return sep === "/" ? path : path.split(sep).join("/")
}
