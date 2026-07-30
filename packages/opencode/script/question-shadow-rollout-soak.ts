import fs from "node:fs/promises"
import path from "node:path"
import { AbsolutePath } from "@opencode-ai/core/schema"
import {
  makeQuestionShadowBreakerStore,
  questionShadowGovernanceConfig,
} from "@opencode-ai/core/question-shadow-governance"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import { Effect, Layer, ManagedRuntime } from "effect"
import { RuntimeFlags } from "../src/effect/runtime-flags"
import { layer as baseLayer } from "../src/question/command-shadow"
import {
  QuestionCommandShadow,
  layerWith,
} from "../src/question/command-shadow-governed"

if (process.env.OPENCODE_QUESTION_SHADOW_SOAK !== "1") {
  console.log(JSON.stringify({ status: "skipped", reason: "set OPENCODE_QUESTION_SHADOW_SOAK=1" }))
  process.exit(0)
}

const config = questionShadowGovernanceConfig()
if (config.invalidReason) throw new Error(config.invalidReason)
const expectedSampleRate = Number(process.env.OPENCODE_QUESTION_SHADOW_SOAK_EXPECTED_SAMPLE_RATE ?? "0.01")
if (expectedSampleRate !== 0.01 && expectedSampleRate !== 0.05) {
  throw new Error("Question shadow soak expected sample rate must be 0.01 or 0.05")
}
if (config.sampleRate !== expectedSampleRate) {
  throw new Error(`Question shadow soak requires a ${expectedSampleRate} sample rate`)
}
if (config.breakerBackend !== "postgres") throw new Error("P7.3.7 soak requires the PostgreSQL breaker")
const allowShort = process.env.OPENCODE_QUESTION_SHADOW_SOAK_ALLOW_SHORT === "1"
const minimumDurationMs = integer("OPENCODE_QUESTION_SHADOW_SOAK_DURATION_MS", 30 * 60 * 1000)
const maximumDurationMs = integer("OPENCODE_QUESTION_SHADOW_SOAK_MAX_DURATION_MS", 45 * 60 * 1000)
const targetSamples = integer("OPENCODE_QUESTION_SHADOW_SOAK_TARGET_SAMPLES", 100)
const intervalMs = integer("OPENCODE_QUESTION_SHADOW_SOAK_INTERVAL_MS", 150)
if (!allowShort && minimumDurationMs < 30 * 60 * 1000) {
  throw new Error("P7.3.7 production soak cannot be shorter than 30 minutes")
}
if (maximumDurationMs < minimumDurationMs) throw new Error("Soak maximum duration is below its minimum")
if (targetSamples < 100 && !allowShort) throw new Error("P7.3.7 production soak requires at least 100 samples")
const output = required("OPENCODE_QUESTION_SHADOW_SOAK_OUTPUT")
const directory = AbsolutePath.make(process.env.OPENCODE_QUESTION_SHADOW_SOAK_DIRECTORY || process.cwd())
const base = baseLayer.pipe(
  Layer.provide(RuntimeFlags.layer({ experimentalQuestionCommandGatewayShadow: true })),
)
const runtime = ManagedRuntime.make(layerWith({ config }).pipe(Layer.provide(base)))
const breakerStore = await makeQuestionShadowBreakerStore(config)
const startedAt = Date.now()
const prefix = `p737_${startedAt}`
let candidateCount = 0
let lastEvidenceWrite = 0

try {
  while (true) {
    const requestID = QuestionV1.ID.ascending(`que_${prefix}_${candidateCount++}`)
    const sessionID = `ses_${prefix}_${candidateCount}` as QuestionV1.AskedData["sessionID"]
    const data: QuestionV1.AskedData = {
      id: requestID,
      sessionID,
      questions: [
        {
          header: "P7.3.7",
          question: "Validate the governed Question shadow path",
          options: [{ label: "Continue", description: "Record a matched canary comparison" }],
        },
      ],
      location: { directory },
    }
    await runtime.runPromise(
      QuestionCommandShadow.Service.use((shadow) =>
        shadow.observeAsk({ data, expected: "appended" }).pipe(
          Effect.andThen(
            shadow.observeReply({
              requestID,
              sessionID,
              answers: [["Continue"]],
              expected: "appended",
            }),
          ),
        ),
      ),
    )
    const now = Date.now()
    const breaker = await breakerStore.read()
    if (breaker.open) throw new Error(`Question shadow breaker opened during soak: ${breaker.reason}`)
    if (now - lastEvidenceWrite >= 60_000) {
      await writeEvidence(output, {
        status: "running",
        startedAt,
        observedAt: now,
        elapsedMs: now - startedAt,
        candidateCount,
        targetSamples,
        telemetry: telemetry(breaker),
      })
      lastEvidenceWrite = now
    }
    if (now - startedAt >= minimumDurationMs && breaker.sampleCount >= targetSamples) {
      const snapshot = await runtime.runPromise(
        QuestionCommandShadow.Service.use((shadow) => Effect.sync(() => shadow.snapshot())),
      )
      const result = {
        status: "passed",
        startedAt,
        completedAt: now,
        elapsedMs: now - startedAt,
        candidateCount,
        targetSamples,
        compared: snapshot.compared,
        matched: snapshot.matched,
        diverged: snapshot.diverged,
        errors: snapshot.errors,
        telemetry: telemetry(breaker),
      }
      await writeEvidence(output, result)
      console.log(JSON.stringify(result, undefined, 2))
      break
    }
    if (now - startedAt >= maximumDurationMs) {
      throw new Error(
        `Question shadow soak timed out with ${breaker.sampleCount}/${targetSamples} samples`,
      )
    }
    await Bun.sleep(intervalMs)
  }
} finally {
  await breakerStore.close()
  await runtime.dispose()
}

function telemetry(breaker: Awaited<ReturnType<typeof breakerStore.read>>) {
  return {
    ready: !breaker.open,
    breakerOpen: breaker.open,
    sampleCount: breaker.sampleCount,
    errorRate: breaker.failureRate,
    divergenceRate: breaker.structuralMismatchRate,
  }
}

async function writeEvidence(target: string, value: unknown) {
  await fs.mkdir(path.dirname(target), { recursive: true })
  const temporary = `${target}.${process.pid}.tmp`
  await fs.writeFile(temporary, `${JSON.stringify(value, undefined, 2)}\n`, { mode: 0o600 })
  await fs.rename(temporary, target)
}

function integer(name: string, fallback: number) {
  const raw = process.env[name]
  if (!raw) return fallback
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`)
  return value
}

function required(name: string) {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}
