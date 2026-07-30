import { afterEach, describe, expect, test } from "bun:test"
import type { LLMClientShape } from "@opencode-ai/llm"
import { createHash } from "node:crypto"
import { Effect } from "effect"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { EventV2 } from "../src/event"
import { ModelInvocationGateway } from "../src/session/runner/model-invocation-gateway"
import { ShadowCanary, type ShadowCanaryOutcome } from "../src/session/runner/shadow-canary"
import { startShadowCanaryExecutor } from "../src/session/runner/shadow-canary-executor"

const services: Array<{ close: () => Promise<void> }> = []

afterEach(async () => {
  await Promise.all(services.splice(0).map((service) => service.close()))
})

describe("ShadowCanaryExecutor", () => {
  test("runs a candidate subprocess, pairs outcomes, persists no prompt, and exports diff metrics", async () => {
    const temporary = await mkdtemp(join(tmpdir(), "opencode-shadow-executor-test-"))
    const script = join(temporary, "candidate.ts")
    const store = join(temporary, "events.jsonl")
    await writeFile(
      script,
      [
        "const body = await new Response(Bun.stdin.stream()).text()",
        "const input = JSON.parse(body)",
        "console.log(JSON.stringify({",
        '  version: "opencode-shadow-canary-outcome.v1", source: "candidate",',
        "  requestDigest: process.env.OPENCODE_SHADOW_REQUEST_DIGEST,",
        "  invocationID: input.invocation.id, status: \"completed\", durationMs: 12,",
        '  outputDigest: "same-output", toolPlanDigest: "same-tools", eventCount: 2, toolCalls: 0,',
        '  usage: { input: 10, output: 5, reasoning: 0, total: 15 }, cost: { total: 0, currency: "USD" }',
        "}))",
      ].join("\n"),
    )
    const service = await startShadowCanaryExecutor({
      host: "127.0.0.1",
      port: 0,
      bearerToken: "executor-test-token-123",
      candidateCommand: [process.execPath, script],
      storePath: store,
      minimumSamples: 2,
      windowSize: 10,
    })
    services.push(service)
    const config = sender(service.url, "executor-test-token-123")
    const prepared = ShadowCanary.prepare(config, request("inv_subprocess"))
    expect(prepared.status).toBe("ready")
    if (prepared.status !== "ready") return
    expect((await ShadowCanary.dispatch(config, prepared)).status).toBe("accepted")
    expect(
      (
        await ShadowCanary.reportOutcome(
          config,
          outcome(prepared.requestDigest, "inv_subprocess", "primary", "completed", 10),
        )
      ).status,
    ).toBe("accepted")
    await waitFor(() => service.snapshot().diffs.length === 1)
    expect(service.snapshot().diffs[0]).toMatchObject({
      statusMatch: true,
      outputDigestMatch: true,
      toolPlanDigestMatch: true,
      regression: false,
    })
    const metrics = await authorizedFetch(`${service.url}/metrics`, "executor-test-token-123")
    expect(await metrics.text()).toContain('opencode_shadow_canary_diffs_total{result="matched"} 1')
    expect(await readFile(store, "utf8")).not.toContain("REAL_P5_3_PROMPT")
    await rm(temporary, { recursive: true, force: true })
  })

  test("opens the breaker after candidate failures and rejects later shadow requests", async () => {
    const service = await startShadowCanaryExecutor(
      {
        host: "127.0.0.1",
        port: 0,
        bearerToken: "breaker-test-token-1234",
        minimumSamples: 2,
        windowSize: 4,
        failureRateThreshold: 0.4,
        structuralMismatchRateThreshold: 1,
        slowRateThreshold: 1,
      },
      {
        executeCandidate: async (input) =>
          outcome(input.requestDigest, input.invocationID, "candidate", "failed", 1),
      },
    )
    services.push(service)
    const config = sender(service.url, "breaker-test-token-1234")
    for (const id of ["inv_breaker_1", "inv_breaker_2"]) {
      const prepared = ShadowCanary.prepare(config, request(id))
      expect(prepared.status).toBe("ready")
      if (prepared.status !== "ready") continue
      expect((await ShadowCanary.dispatch(config, prepared)).status).toBe("accepted")
      await ShadowCanary.reportOutcome(config, outcome(prepared.requestDigest, id, "primary", "completed", 10))
    }
    await waitFor(() => service.snapshot().breaker.open)
    expect(service.snapshot().breaker.reason).toContain("candidate_failure_rate")
    const blocked = ShadowCanary.prepare(config, request("inv_breaker_3"))
    expect(blocked.status).toBe("ready")
    if (blocked.status !== "ready") return
    const result = await ShadowCanary.dispatch(config, blocked)
    expect(result).toMatchObject({ status: "failed", failureType: "http", statusCode: 503 })
    const metrics = await authorizedFetch(`${service.url}/metrics`, "breaker-test-token-1234")
    expect(await metrics.text()).toContain("opencode_shadow_canary_breaker_open 1")
  })

  test("pairs the primary outcome reported automatically by ModelInvocationGateway", async () => {
    const emptyDigest = createHash("sha256").update("").digest("hex")
    const service = await startShadowCanaryExecutor(
      {
        host: "127.0.0.1",
        port: 0,
        bearerToken: "gateway-pair-token-1234",
        minimumSamples: 2,
        windowSize: 4,
      },
      {
        executeCandidate: async (input) => ({
          ...outcome(input.requestDigest, input.invocationID, "candidate", "completed", 10),
          outputDigest: emptyDigest,
          toolPlanDigest: emptyDigest,
          eventCount: 0,
        }),
      },
    )
    services.push(service)
    const config = sender(service.url, "gateway-pair-token-1234")
    const events = {
      publish: () => Effect.void,
    } as unknown as EventV2.Interface
    const llm = {
      stream: () => {
        throw new Error("stream is not required for gateway outcome pairing")
      },
    } as unknown as LLMClientShape
    const gateway = ModelInvocationGateway.make({ events, llm, shadowCanary: config })
    const invocation = await Effect.runPromise(
      gateway.start({
        sessionID: "ses_gateway_pair" as never,
        agent: "build",
        model: { providerID: "test", modelID: "test" } as never,
        request: request("ignored").request,
      }),
    )
    await Effect.runPromise(invocation.complete())
    await waitFor(() => service.snapshot().diffs.length === 1)
    expect(service.snapshot()).toMatchObject({
      primaryOutcomes: 1,
      candidateOutcomes: 1,
    })
    expect(service.snapshot().diffs[0]).toMatchObject({
      statusMatch: true,
      outputDigestMatch: true,
      toolPlanDigestMatch: true,
    })
  })
})

function sender(url: string, token: string) {
  return ShadowCanary.config({
    OPENCODE_SHADOW_CANARY_ENABLED: "true",
    OPENCODE_SHADOW_CANARY_URL: `${url}/v1/shadow/requests`,
    OPENCODE_SHADOW_CANARY_OUTCOME_URL: `${url}/v1/shadow/outcomes/primary`,
    OPENCODE_SHADOW_CANARY_TOKEN: token,
    OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
  })
}

function request(invocationID: string) {
  return {
    invocationID,
    sessionID: "ses_p5_3",
    agent: "build",
    model: { providerID: "test", modelID: "test" },
    request: {
      model: { provider: "test", route: { id: "test-route" } },
      messages: [{ role: "user", content: [{ type: "text", text: "REAL_P5_3_PROMPT" }] }],
    } as never,
  }
}

function outcome(
  requestDigest: string,
  invocationID: string,
  source: "primary" | "candidate",
  status: "completed" | "failed",
  durationMs: number,
): ShadowCanaryOutcome {
  return {
    version: "opencode-shadow-canary-outcome.v1",
    source,
    requestDigest,
    invocationID,
    status,
    durationMs,
    outputDigest: source === "candidate" && status === "failed" ? "candidate-failed" : "same-output",
    toolPlanDigest: source === "candidate" && status === "failed" ? "candidate-failed-tools" : "same-tools",
    eventCount: 2,
    toolCalls: 0,
    usage: { input: 10, output: 5, reasoning: 0, total: 15 },
    cost: { total: 0, currency: "USD" },
    ...(status === "failed" ? { error: "candidate failure" } : {}),
  }
}

async function authorizedFetch(url: string, token: string) {
  return fetch(url, { headers: { authorization: `Bearer ${token}` } })
}

async function waitFor(check: () => boolean) {
  const deadline = Date.now() + 5_000
  while (!check()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for shadow canary executor")
    await Bun.sleep(20)
  }
}
