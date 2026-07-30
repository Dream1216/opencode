import { createHash, createHmac, timingSafeEqual } from "node:crypto"
import fs from "node:fs/promises"
import type { PostgresReleaseProof } from "./release-gate"
import { postgresReleaseProofIssues } from "./release-gate"
import { migrations } from "./schema"

export type ReleaseProofPayload = {
  readonly environment: string
  readonly buildID: string
  readonly databaseFingerprint: string
  readonly databaseSchema: string
  readonly databaseRole: string
  readonly schemaHash: string
  readonly generatedAt: number
  readonly expiresAt: number
  readonly proof: PostgresReleaseProof
  readonly evidence: Readonly<Record<string, readonly string[]>>
}

export type ReleaseProofArtifact = {
  readonly version: 1
  readonly payload: ReleaseProofPayload
  readonly digest: string
  readonly signature: string
}

export type ReleaseProofExpectation = {
  readonly environment: string
  readonly buildID: string
  readonly databaseFingerprint: string
  readonly databaseSchema: string
  readonly databaseRole: string
  readonly schemaHash: string
  readonly key: string
  readonly now?: number
}

export function createReleaseProofArtifact(payload: ReleaseProofPayload, key: string): ReleaseProofArtifact {
  if (key.trim() === "") throw new Error("PostgreSQL release proof signing key is required")
  const digest = sha256(canonical(payload))
  return {
    version: 1,
    payload,
    digest,
    signature: createHmac("sha256", key).update(digest).digest("hex"),
  }
}

export function verifyReleaseProofArtifact(
  artifact: ReleaseProofArtifact,
  expected: ReleaseProofExpectation,
): ReleaseProofArtifact {
  if (artifact.version !== 1) throw new Error(`Unsupported PostgreSQL release proof version ${String(artifact.version)}`)
  const digest = sha256(canonical(artifact.payload))
  if (!safeEqual(artifact.digest, digest)) throw new Error("PostgreSQL release proof digest mismatch")
  const signature = createHmac("sha256", expected.key).update(digest).digest("hex")
  if (!safeEqual(artifact.signature, signature)) throw new Error("PostgreSQL release proof signature mismatch")
  const now = expected.now ?? Date.now()
  if (artifact.payload.generatedAt > now + 60_000) throw new Error("PostgreSQL release proof was generated in the future")
  if (artifact.payload.expiresAt <= now) throw new Error("PostgreSQL release proof has expired")
  assertEqual("environment", artifact.payload.environment, expected.environment)
  assertEqual("build ID", artifact.payload.buildID, expected.buildID)
  assertEqual("database fingerprint", artifact.payload.databaseFingerprint, expected.databaseFingerprint)
  assertEqual("database schema", artifact.payload.databaseSchema, expected.databaseSchema)
  assertEqual("database role", artifact.payload.databaseRole, expected.databaseRole)
  assertEqual("schema hash", artifact.payload.schemaHash, expected.schemaHash)
  const issues = postgresReleaseProofIssues(artifact.payload.proof)
  if (issues.length > 0) throw new Error(`PostgreSQL release proof checks failed: ${issues.join("; ")}`)
  return artifact
}

export async function loadAndVerifyReleaseProof(path: string, expected: ReleaseProofExpectation) {
  const artifact = JSON.parse(await fs.readFile(path, "utf8")) as ReleaseProofArtifact
  return verifyReleaseProofArtifact(artifact, expected)
}

export function postgresSchemaHash() {
  return sha256(canonical(migrations))
}

export function databaseFingerprint(url: string) {
  const parsed = new URL(url)
  parsed.username = ""
  parsed.password = ""
  parsed.hash = ""
  for (const key of Array.from(parsed.searchParams.keys())) {
    if (/password|secret|token|key/i.test(key)) parsed.searchParams.set(key, "[redacted]")
  }
  return sha256(parsed.toString())
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`)
      .join(",")}}`
  }
  return JSON.stringify(value)
}

function sha256(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function safeEqual(left: string, right: string) {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.length === b.length && timingSafeEqual(a, b)
}

function assertEqual(label: string, actual: string, expected: string) {
  if (actual !== expected) throw new Error(`PostgreSQL release proof ${label} mismatch`)
}
