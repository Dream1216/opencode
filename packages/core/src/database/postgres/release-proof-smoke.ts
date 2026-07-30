import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { DatabaseBackend } from "../backend"
import { assertAlphaStartup } from "./alpha-startup"
import { makeClient } from "./client"
import {
  createReleaseProofArtifact,
  databaseFingerprint,
  loadAndVerifyReleaseProof,
  postgresSchemaHash,
  verifyReleaseProofArtifact,
  type ReleaseProofPayload,
} from "./release-proof"
import { passingPostgresReleaseProof } from "./release-gate"

export async function runReleaseProofSmoke(env: NodeJS.ProcessEnv = process.env) {
  const checks: string[] = []
  const key = "p4.24-proof-smoke-key"
  const now = Date.now()
  const payload = samplePayload(now)
  const artifact = createReleaseProofArtifact(payload, key)
  const expected = expectation(payload, key, now)
  verifyReleaseProofArtifact(artifact, expected)
  checks.push("signed-proof-verified")

  expectRejected(() =>
    verifyReleaseProofArtifact(
      { ...artifact, payload: { ...artifact.payload, buildID: "tampered-build" } },
      expected,
    ),
  )
  checks.push("tampered-proof-blocked")

  expectRejected(() => verifyReleaseProofArtifact(artifact, { ...expected, environment: "wrong-environment" }))
  checks.push("environment-mismatch-blocked")

  const expired = createReleaseProofArtifact({ ...payload, generatedAt: now - 2_000, expiresAt: now - 1_000 }, key)
  expectRejected(() => verifyReleaseProofArtifact(expired, expected))
  checks.push("expired-proof-blocked")

  if (env.OPENCODE_POSTGRES_RELEASE_PROOF_SMOKE !== "1") return { status: "ok" as const, checks }
  const url = required(env.OPENCODE_DATABASE_URL, "OPENCODE_DATABASE_URL")
  const realKey = env.OPENCODE_POSTGRES_RELEASE_PROOF_KEY ?? "p4.24-real-smoke-key"
  const environment = env.OPENCODE_RELEASE_ENV ?? "p4-release-proof-smoke"
  const buildID = env.OPENCODE_BUILD_ID ?? "p4.24-p4.25-smoke-build"
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-release-proof-"))
  const output = env.OPENCODE_POSTGRES_RELEASE_PROOF_OUTPUT ?? path.join(temporary, "release-proof.json")
  const sql = makeClient({ url, max: 4 })
  try {
    const { generateReleaseProof } = await import("./release-proof-generator")
    const real = await generateReleaseProof({ sql, url, environment, buildID, key: realKey })
    await fs.mkdir(path.dirname(output), { recursive: true })
    await fs.writeFile(output, `${JSON.stringify(real, undefined, 2)}\n`, { mode: 0o600 })
    await loadAndVerifyReleaseProof(output, {
      environment,
      buildID,
      databaseFingerprint: real.payload.databaseFingerprint,
      databaseSchema: real.payload.databaseSchema,
      databaseRole: real.payload.databaseRole,
      schemaHash: postgresSchemaHash(),
      key: realKey,
    })
    checks.push("real-proof-generated-and-verified")

    const cloudRequired = env.OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED === "1"
    const alpha = DatabaseBackend.fromEnv(() => ":memory:", {
      ...env,
      OPENCODE_DATABASE_BACKEND: "postgres-alpha",
      OPENCODE_DATABASE_URL: url,
      OPENCODE_TENANT_ID: env.OPENCODE_TENANT_ID ?? "tenant_release_proof_smoke",
      OPENCODE_ACTOR_ID: env.OPENCODE_ACTOR_ID ?? "actor_release_proof_smoke",
      OPENCODE_POSTGRES_REQUIRE_RLS: "1",
      OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED: "1",
    })
    if (alpha.type !== "postgres-alpha") throw new Error("Release proof smoke did not select postgres-alpha")
    const startupEnv = {
      ...env,
      OPENCODE_SAAS_RELEASE: "1",
      OPENCODE_RELEASE_ENV: environment,
      OPENCODE_BUILD_ID: buildID,
      OPENCODE_POSTGRES_RELEASE_PROOF_PATH: output,
      OPENCODE_POSTGRES_RELEASE_PROOF_KEY: realKey,
    }
    if (cloudRequired) {
      const startup = await assertAlphaStartup(alpha, startupEnv)
      if (startup.proof.status !== "verified") throw new Error("SaaS startup did not verify the cloud release proof")
      checks.push("saas-startup-cloud-proof-gate-ready")
    } else {
      try {
        await assertAlphaStartup(alpha, startupEnv)
      } catch {
        checks.push("saas-startup-cloud-proof-required")
      }
      if (!checks.includes("saas-startup-cloud-proof-required")) {
        throw new Error("SaaS startup accepted a release proof without P4.42 cloud evidence")
      }
    }
    return { status: "ok" as const, checks, proofPath: output, digest: real.digest }
  } finally {
    await sql.end({ timeout: 5 })
    if (env.OPENCODE_POSTGRES_RELEASE_PROOF_OUTPUT === undefined) {
      await fs.rm(temporary, { recursive: true, force: true })
    }
  }
}

function samplePayload(now: number): ReleaseProofPayload {
  return {
    environment: "proof-smoke",
    buildID: "proof-smoke-build",
    databaseFingerprint: databaseFingerprint("postgres://user:password@example.test/opencode"),
    databaseSchema: "proof_smoke",
    databaseRole: "proof_role",
    schemaHash: postgresSchemaHash(),
    generatedAt: now,
    expiresAt: now + 60_000,
    proof: passingPostgresReleaseProof,
    evidence: { smoke: ["all-checks-passed"] },
  }
}

function expectation(payload: ReleaseProofPayload, key: string, now: number) {
  return {
    environment: payload.environment,
    buildID: payload.buildID,
    databaseFingerprint: payload.databaseFingerprint,
    databaseSchema: payload.databaseSchema,
    databaseRole: payload.databaseRole,
    schemaHash: payload.schemaHash,
    key,
    now,
  }
}

function expectRejected(run: () => unknown) {
  try {
    run()
  } catch {
    return
  }
  throw new Error("Expected PostgreSQL release proof verification to fail")
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
