import { afterEach, expect, test } from "bun:test"
import { ShadowCanary, type ShadowCanaryOutcome } from "../src/session/runner/shadow-canary"
import { startShadowCanaryExecutor } from "../src/session/runner/shadow-canary-executor"

const candidateCheckout = process.env.OPENCODE_SHADOW_CANDIDATE_CHECKOUT
const services: Array<{ close(): Promise<void> }> = []

afterEach(async () => {
  await Promise.all(services.splice(0).map((service) => service.close()))
})

;(candidateCheckout ? test : test.skip)("runs the v1.18.8 no-merge OpenCode CLI with model access and no tools", async () => {
  const modelRequests: Record<string, unknown>[] = []
  const modelServer = Bun.serve({
    port: 0,
    async fetch(request) {
      modelRequests.push(JSON.parse(await request.text()) as Record<string, unknown>)
      const created = Math.floor(Date.now() / 1_000)
      const chunks = [
        {
          id: "chatcmpl-shadow",
          object: "chat.completion.chunk",
          created,
          model: "test-model",
          choices: [{ index: 0, delta: { role: "assistant", content: "candidate real output" }, finish_reason: null }],
        },
        {
          id: "chatcmpl-shadow",
          object: "chat.completion.chunk",
          created,
          model: "test-model",
          choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
          usage: { prompt_tokens: 8, completion_tokens: 3, total_tokens: 11 },
        },
      ]
      return new Response(`${chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("")}data: [DONE]\n\n`, {
        headers: { "content-type": "text/event-stream" },
      })
    },
  })
  try {
    const service = await startShadowCanaryExecutor({
      host: "127.0.0.1",
      port: 0,
      bearerToken: "candidate-model-only-token",
      candidateCheckout,
      candidateTimeoutMs: 30_000,
      candidateConfigContent: JSON.stringify({
        provider: {
          test: {
            name: "Test",
            id: "test",
            env: [],
            npm: "@ai-sdk/openai-compatible",
            models: {
              "test-model": {
                id: "test-model",
                name: "Test Model",
                attachment: false,
                reasoning: false,
                temperature: false,
                tool_call: true,
                release_date: "2025-01-01",
                limit: { context: 100_000, output: 10_000 },
                cost: { input: 0, output: 0 },
                options: {},
              },
            },
            options: { apiKey: "test-key", baseURL: `http://127.0.0.1:${modelServer.port}/v1` },
          },
        },
      }),
      minimumSamples: 2,
      windowSize: 4,
    })
    services.push(service)
    const config = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${service.url}/v1/shadow/requests`,
      OPENCODE_SHADOW_CANARY_OUTCOME_URL: `${service.url}/v1/shadow/outcomes/primary`,
      OPENCODE_SHADOW_CANARY_TOKEN: "candidate-model-only-token",
      OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
    })
    const prepared = ShadowCanary.prepare(config, {
      invocationID: "inv_real_candidate",
      sessionID: "ses_real_candidate",
      agent: "build",
      model: { providerID: "test", modelID: "test-model" },
      request: {
        model: { provider: "test", route: { id: "test-route" } },
        messages: [{ role: "user", content: [{ type: "text", text: "REAL_V1_18_8_MODEL_ONLY_PROMPT" }] }],
      } as never,
    })
    expect(prepared.status).toBe("ready")
    if (prepared.status !== "ready") return
    expect((await ShadowCanary.dispatch(config, prepared)).status).toBe("accepted")
    expect((await ShadowCanary.reportOutcome(config, outcome(prepared.requestDigest, "inv_real_candidate"))).status).toBe(
      "accepted",
    )
    await waitFor(() => service.snapshot().diffs.length === 1)
    expect(service.snapshot()).toMatchObject({ candidateOutcomes: 1, primaryOutcomes: 1 })
    expect(service.snapshot().diffs[0]).toMatchObject({ statusMatch: true, candidateFailed: false })
    expect(modelRequests).toHaveLength(1)
    expect(JSON.stringify(modelRequests[0])).toContain("REAL_V1_18_8_MODEL_ONLY_PROMPT")
    expect(modelRequests[0]?.tools).toBeUndefined()
  } finally {
    modelServer.stop(true)
  }
})

function outcome(requestDigest: string, invocationID: string): ShadowCanaryOutcome {
  return {
    version: "opencode-shadow-canary-outcome.v1",
    source: "primary",
    requestDigest,
    invocationID,
    status: "completed",
    durationMs: 10,
    outputDigest: "primary-output",
    toolPlanDigest: "primary-tools",
    eventCount: 1,
    toolCalls: 0,
    usage: { input: 8, output: 3, reasoning: 0, total: 11 },
    cost: { total: 0, currency: "USD" },
  }
}

async function waitFor(check: () => boolean) {
  const deadline = Date.now() + 30_000
  while (!check()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for real candidate outcome")
    await Bun.sleep(25)
  }
}
