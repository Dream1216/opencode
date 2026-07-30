import { randomUUID } from "node:crypto"
import { spawn } from "node:child_process"
import { makePostgresShadowCanaryBreakerStore } from "../src/session/runner/shadow-canary-breaker-store"

const url =
  process.env.OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL?.trim() ||
  process.env.OPENCODE_DATABASE_URL?.trim()
if (!url) throw new Error("OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL is required")
const scope = process.env.OPENCODE_SHADOW_CANARY_BREAKER_SCOPE?.trim() || `p5.3.1-smoke:${randomUUID()}`
const policy = {
  windowSize: 8,
  minimumSamples: 2,
  failureRateThreshold: 0.4,
  structuralMismatchRateThreshold: 1,
  slowRateThreshold: 1,
  latencyRatioThreshold: 2,
  cooldownMs: 60_000,
}

if (process.env.OPENCODE_SHADOW_CANARY_BREAKER_CHILD === "1") {
  const store = await makePostgresShadowCanaryBreakerStore({ url, scope, policy })
  try {
    const digest = process.env.OPENCODE_SHADOW_CANARY_BREAKER_DIGEST
    if (!digest) throw new Error("child digest is required")
    const state = await store.record({
      requestDigest: digest,
      candidateFailed: true,
      statusMatch: false,
      toolPlanDigestMatch: false,
      latencyRatio: 1,
      comparedAt: Date.now(),
    })
    console.log(JSON.stringify({ child: process.pid, revision: state.revision }))
  } finally {
    await store.close()
  }
  process.exit(0)
}

const setup = await makePostgresShadowCanaryBreakerStore({ url, scope, policy })
await setup.destroy?.()
await setup.close()

const children = ["candidate-a", "candidate-b"].map(
  (digest) =>
    new Promise<void>((resolve, reject) => {
      const child = spawn(process.execPath, [import.meta.path], {
        env: {
          ...process.env,
          OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL: url,
          OPENCODE_SHADOW_CANARY_BREAKER_SCOPE: scope,
          OPENCODE_SHADOW_CANARY_BREAKER_CHILD: "1",
          OPENCODE_SHADOW_CANARY_BREAKER_DIGEST: digest,
        },
        stdio: ["ignore", "pipe", "pipe"],
      })
      child.stdout.resume()
      const stderr: Buffer[] = []
      child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk))
      child.once("error", reject)
      child.once("close", (code) =>
        code === 0
          ? resolve()
          : reject(new Error(`breaker child exited ${code}: ${Buffer.concat(stderr).toString("utf8").slice(0, 2_048)}`)),
      )
    }),
)
await Promise.all(children)

const first = await makePostgresShadowCanaryBreakerStore({ url, scope, policy })
const firstState = await first.read()
if (!firstState.open || firstState.sampleCount !== 2 || firstState.failureRate !== 1) {
  throw new Error(`shared breaker did not open: ${JSON.stringify(firstState)}`)
}
await first.close()

const restarted = await makePostgresShadowCanaryBreakerStore({ url, scope, policy })
try {
  const restartedState = await restarted.read()
  if (!restartedState.open || restartedState.sampleCount !== 2) {
    throw new Error(`breaker state did not survive restart: ${JSON.stringify(restartedState)}`)
  }
  console.log(
    JSON.stringify(
      {
        status: "passed",
        scope,
        processes: 2,
        persistedAcrossRestart: true,
        breaker: restartedState,
      },
      null,
      2,
    ),
  )
} finally {
  await restarted.destroy?.()
  await restarted.close()
}
