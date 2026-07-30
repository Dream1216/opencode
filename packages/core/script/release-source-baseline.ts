import { createHash } from "node:crypto"
import { createReadStream } from "node:fs"
import { lstat, mkdir, readdir, readlink, writeFile } from "node:fs/promises"
import path from "node:path"

type Entry =
  | {
      readonly path: string
      readonly type: "file"
      readonly bytes: number
      readonly executable: boolean
      readonly sha256: string
    }
  | {
      readonly path: string
      readonly type: "symlink"
      readonly target: string
      readonly sha256: string
    }

const excludedNames = new Set([".DS_Store", ".git", ".cache", ".turbo", "dist", "node_modules", "target"])
const excludedPaths = new Set([
  ".opencode/package-lock.json",
  ".opencode/package.json",
  "packages/core/compatibility/release-source-baseline.json",
])
const excludedPrefixes = new Set([".husky/_"])
const excludedSuffixes = [".tsbuildinfo"]

const root = path.resolve(requiredArgument("--root"))
const output = path.resolve(requiredArgument("--output"))
const entries: Entry[] = []

await visit(root, "")
entries.sort((left, right) => left.path.localeCompare(right.path, "en"))

const treeDigest = createHash("sha256")
for (const entry of entries) {
  treeDigest.update(JSON.stringify(entry))
  treeDigest.update("\n")
}

const result = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  algorithm: "sha256",
  exclusions: {
    names: [...excludedNames].sort(),
    paths: [...excludedPaths].sort(),
    prefixes: [...excludedPrefixes].sort(),
    suffixes: [...excludedSuffixes].sort(),
  },
  fileCount: entries.filter((entry) => entry.type === "file").length,
  symlinkCount: entries.filter((entry) => entry.type === "symlink").length,
  totalBytes: entries.reduce((total, entry) => total + (entry.type === "file" ? entry.bytes : 0), 0),
  treeDigest: treeDigest.digest("hex"),
  entries,
}

await mkdir(path.dirname(output), { recursive: true })
await writeFile(output, `${JSON.stringify(result, undefined, 2)}\n`)
console.log(
  JSON.stringify({
    output,
    fileCount: result.fileCount,
    symlinkCount: result.symlinkCount,
    totalBytes: result.totalBytes,
    treeDigest: result.treeDigest,
  }),
)

async function visit(absoluteDirectory: string, relativeDirectory: string) {
  const children = await readdir(absoluteDirectory, { withFileTypes: true })
  children.sort((left, right) => left.name.localeCompare(right.name, "en"))
  for (const child of children) {
    const relative = relativeDirectory ? `${relativeDirectory}/${child.name}` : child.name
    if (excluded(relative)) continue
    const absolute = path.join(absoluteDirectory, child.name)
    if (child.isDirectory()) {
      await visit(absolute, relative)
      continue
    }
    if (child.isSymbolicLink()) {
      const target = await readlink(absolute)
      entries.push({
        path: relative,
        type: "symlink",
        target,
        sha256: createHash("sha256").update(target).digest("hex"),
      })
      continue
    }
    if (!child.isFile()) continue
    const stat = await lstat(absolute)
    entries.push({
      path: relative,
      type: "file",
      bytes: stat.size,
      executable: (stat.mode & 0o111) !== 0,
      sha256: await fileHash(absolute),
    })
  }
}

function excluded(relative: string) {
  if (excludedPaths.has(relative)) return true
  if ([...excludedPrefixes].some((prefix) => relative === prefix || relative.startsWith(`${prefix}/`))) {
    return true
  }
  if (excludedSuffixes.some((suffix) => relative.endsWith(suffix))) return true
  return relative.split("/").some((segment) => excludedNames.has(segment))
}

async function fileHash(file: string) {
  const hash = createHash("sha256")
  await new Promise<void>((resolve, reject) => {
    const input = createReadStream(file)
    input.on("data", (chunk) => hash.update(chunk))
    input.on("error", reject)
    input.on("end", resolve)
  })
  return hash.digest("hex")
}

function requiredArgument(name: string) {
  const index = process.argv.indexOf(name)
  const value = process.argv[index + 1]?.trim()
  if (index < 0 || !value) throw new Error(`Missing ${name}`)
  return value
}
