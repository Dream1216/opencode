import fs from "node:fs/promises"
import path from "node:path"
import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { generateReleaseProof } from "../src/database/postgres/release-proof-generator"

const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const output = required(process.env.OPENCODE_POSTGRES_RELEASE_PROOF_OUTPUT, "OPENCODE_POSTGRES_RELEASE_PROOF_OUTPUT")
const key = required(process.env.OPENCODE_POSTGRES_RELEASE_PROOF_KEY, "OPENCODE_POSTGRES_RELEASE_PROOF_KEY")
const environment = required(process.env.OPENCODE_RELEASE_ENV, "OPENCODE_RELEASE_ENV")
const buildID = required(process.env.OPENCODE_BUILD_ID, "OPENCODE_BUILD_ID")
const ttlMs = Number(process.env.OPENCODE_POSTGRES_RELEASE_PROOF_TTL_MS ?? 6 * 60 * 60 * 1000)
const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 4) })
try {
  const artifact = await generateReleaseProof({ sql, url: config.url, environment, buildID, key, ttlMs })
  await fs.mkdir(path.dirname(output), { recursive: true })
  await fs.writeFile(output, `${JSON.stringify(artifact, undefined, 2)}\n`, { mode: 0o600 })
  console.log(
    JSON.stringify(
      {
        status: "ok",
        output,
        digest: artifact.digest,
        generatedAt: artifact.payload.generatedAt,
        expiresAt: artifact.payload.expiresAt,
      },
      undefined,
      2,
    ),
  )
} finally {
  await sql.end({ timeout: 5 })
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
