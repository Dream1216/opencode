import {
  evaluateQuestionShadowRollout,
  questionShadowRolloutStages,
  verifyQuestionShadowAcceptanceArtifact,
  type QuestionShadowAcceptanceArtifact,
  type QuestionShadowRolloutStage,
  type QuestionShadowTelemetryGateInput,
} from "../src/question-shadow-release-gate"
import {
  readQuestionShadowReleaseInput,
  resolveQuestionShadowReleaseSecret,
} from "../src/question-shadow-release-secrets"

const proof = await readQuestionShadowReleaseInput({
  path: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_PROOF,
  reference: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_PROOF_REF,
  label: "Question shadow acceptance proof",
})
const key = await resolveQuestionShadowReleaseSecret({
  direct: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_KEY,
  reference: process.env.OPENCODE_QUESTION_SHADOW_ACCEPTANCE_KEY_REF,
  label: "Question shadow acceptance key",
})
const environment = required("OPENCODE_RELEASE_ENV")
const buildID = required("OPENCODE_BUILD_ID")
const currentSampleRate = stage(required("OPENCODE_QUESTION_SHADOW_CURRENT_SAMPLE_RATE"))
const requestedSampleRate = stage(required("OPENCODE_QUESTION_SHADOW_REQUESTED_SAMPLE_RATE"))
const stageStartedAt = timestamp(process.env.OPENCODE_QUESTION_SHADOW_STAGE_STARTED_AT)
const telemetrySource =
  process.env.OPENCODE_QUESTION_SHADOW_TELEMETRY_SNAPSHOT ||
  process.env.OPENCODE_QUESTION_SHADOW_TELEMETRY_SNAPSHOT_REF
    ? await readQuestionShadowReleaseInput({
        path: process.env.OPENCODE_QUESTION_SHADOW_TELEMETRY_SNAPSHOT,
        reference: process.env.OPENCODE_QUESTION_SHADOW_TELEMETRY_SNAPSHOT_REF,
        label: "Question shadow telemetry snapshot",
      })
    : undefined
const telemetry = telemetrySource
  ? (JSON.parse(telemetrySource) as QuestionShadowTelemetryGateInput)
  : undefined
const expectation = { environment, buildID, key }
const artifact = verifyQuestionShadowAcceptanceArtifact(
  JSON.parse(proof) as QuestionShadowAcceptanceArtifact,
  expectation,
)
const result = evaluateQuestionShadowRollout({
  currentSampleRate,
  requestedSampleRate,
  stageStartedAt,
  artifact,
  expectation,
  telemetry,
})
console.log(JSON.stringify(result, undefined, 2))
if (result.decision !== "promote") process.exitCode = result.decision === "rollback" ? 2 : 1

function required(name: string) {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}

function stage(value: string): QuestionShadowRolloutStage {
  const result = Number(value)
  if (!questionShadowRolloutStages.includes(result as QuestionShadowRolloutStage)) {
    throw new Error(`Question shadow sample rate must be one of ${questionShadowRolloutStages.join(", ")}`)
  }
  return result as QuestionShadowRolloutStage
}

function timestamp(value: string | undefined) {
  if (!value) return Date.now()
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < 0) {
    throw new Error("Question shadow stage start must be a Unix millisecond timestamp")
  }
  return result
}
