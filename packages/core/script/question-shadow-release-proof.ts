import fs from "node:fs/promises"
import path from "node:path"
import {
  createQuestionShadowAcceptanceArtifact,
  type QuestionShadowLiveAcceptance,
} from "../src/question-shadow-release-gate"
import {
  readQuestionShadowReleaseInput,
  resolveQuestionShadowReleaseSecret,
} from "../src/question-shadow-release-secrets"

const input = await readQuestionShadowReleaseInput({
  path: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_RESULT,
  reference: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_RESULT_REF,
  label: "Question shadow acceptance result",
})
const output = required("OPENCODE_QUESTION_SHADOW_ACCEPTANCE_PROOF")
const key = await resolveQuestionShadowReleaseSecret({
  direct: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_KEY,
  reference: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_KEY_REF,
  label: "Question shadow acceptance key",
})
const environment = required("OPENCODE_RELEASE_ENV")
const buildID = required("OPENCODE_BUILD_ID")
const ttlMs = positiveInteger(
  process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_TTL_MS,
  6 * 60 * 60 * 1000,
)
const generatedAt = Date.now()
const acceptance = JSON.parse(input) as QuestionShadowLiveAcceptance
const artifact = createQuestionShadowAcceptanceArtifact(
  {
    environment,
    buildID,
    generatedAt,
    expiresAt: generatedAt + ttlMs,
    acceptance,
  },
  key,
)
await fs.mkdir(path.dirname(output), { recursive: true })
await fs.writeFile(output, `${JSON.stringify(artifact, undefined, 2)}\n`, { mode: 0o600 })
console.log(
  JSON.stringify(
    {
      status: "ok",
      output,
      digest: artifact.digest,
      generatedAt,
      expiresAt: artifact.payload.expiresAt,
    },
    undefined,
    2,
  ),
)

function required(name: string) {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}

function positiveInteger(value: string | undefined, fallback: number) {
  if (value === undefined) return fallback
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < 1) throw new Error("Acceptance proof TTL must be a positive integer")
  return result
}
