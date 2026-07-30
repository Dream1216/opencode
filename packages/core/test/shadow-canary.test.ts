import { afterAll, beforeEach, describe, expect, test } from "bun:test"
import type { LLMClientShape, LLMRequest } from "@opencode-ai/llm"
import { Effect } from "effect"
import type { EventV2 } from "../src/event"
import { ModelInvocationGateway } from "../src/session/runner/model-invocation-gateway"
import { ShadowCanary } from "../src/session/runner/shadow-canary"

const received: Array<{ authorization: string | null; body: string }> = []
const server = Bun.serve({
  port: 0,
  async fetch(request) {
    if (new URL(request.url).pathname === "/slow") {
      await Bun.sleep(250)
      return Response.json({ receiptID: "late" }, { status: 202 })
    }
    const body = await request.text()
    received.push({ authorization: request.headers.get("authorization"), body })
    return Response.json({ receiptID: "receipt-real-shadow" }, { status: 202 })
  },
})
const endpoint = `http://127.0.0.1:${server.port}`

beforeEach(() => {
  received.length = 0
})

afterAll(() => {
  server.stop(true)
})

describe("ShadowCanary", () => {
  test("sends sampled real request content without provider credentials", async () => {
    const config = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${endpoint}/accept`,
      OPENCODE_SHADOW_CANARY_TOKEN: "CANARY_TRANSPORT_TOKEN",
      OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
    })
    const prepared = ShadowCanary.prepare(config, input("inv_real"))
    expect(prepared.status).toBe("ready")
    if (prepared.status !== "ready") return

    const result = await ShadowCanary.dispatch(config, prepared)
    expect(result).toEqual({
      status: "accepted",
      durationMs: expect.any(Number),
      statusCode: 202,
      receiptID: "receipt-real-shadow",
    })
    expect(received).toHaveLength(1)
    expect(received[0]?.authorization).toBe("Bearer CANARY_TRANSPORT_TOKEN")
    expect(received[0]?.body).toContain("REAL_SHADOW_REQUEST_MARKER")
    expect(received[0]?.body).not.toContain("PROVIDER_SECRET_VALUE")
    expect(received[0]?.body).not.toContain("CANARY_TRANSPORT_TOKEN")
  })

  test("does not dispatch a request sampled out at zero percent", () => {
    const config = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${endpoint}/accept`,
      OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "0",
    })
    expect(ShadowCanary.prepare(config, input("inv_zero"))).toEqual({
      status: "skipped",
      reason: "sampled-out",
    })
    expect(received).toHaveLength(0)
  })

  test("contains timeout failure inside the shadow path", async () => {
    const config = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${endpoint}/slow`,
      OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
      OPENCODE_SHADOW_CANARY_TIMEOUT_MS: "25",
    })
    const prepared = ShadowCanary.prepare(config, input("inv_timeout"))
    expect(prepared.status).toBe("ready")
    if (prepared.status !== "ready") return
    const result = await ShadowCanary.dispatch(config, prepared)
    expect(result.status).toBe("failed")
    if (result.status === "failed") expect(result.failureType).toBe("timeout")
  })

  test("rejects insecure remote endpoints and missing transport auth", () => {
    const config = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: "http://canary.example/shadow",
      OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
    })
    expect(config.invalidReason).toContain("HTTPS")
    expect(config.invalidReason).toContain("TOKEN")
    expect(ShadowCanary.prepare(config, input("inv_invalid"))).toEqual({
      status: "skipped",
      reason: "invalid-config",
    })
  })

  test("rejects an unsafe or cross-origin outcome URL independently", () => {
    const insecure = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${endpoint}/accept`,
      OPENCODE_SHADOW_CANARY_OUTCOME_URL: "http://outcome.example/collect",
      OPENCODE_SHADOW_CANARY_TOKEN: "CANARY_TRANSPORT_TOKEN",
    })
    expect(insecure.invalidReason).toContain("outcome URL must use HTTPS")
    expect(insecure.invalidReason).toContain("outcome URL must use the request URL origin")

    const crossOrigin = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: "https://candidate.example/accept",
      OPENCODE_SHADOW_CANARY_OUTCOME_URL: "https://outcome.example/collect",
      OPENCODE_SHADOW_CANARY_TOKEN: "CANARY_TRANSPORT_TOKEN",
    })
    expect(crossOrigin.invalidReason).toContain("outcome URL must use the request URL origin")

    const credentialed = ShadowCanary.config({
      OPENCODE_SHADOW_CANARY_ENABLED: "true",
      OPENCODE_SHADOW_CANARY_URL: `${endpoint}/accept`,
      OPENCODE_SHADOW_CANARY_OUTCOME_URL: "http://user:password@127.0.0.1/collect",
      OPENCODE_SHADOW_CANARY_TOKEN: "CANARY_TRANSPORT_TOKEN",
    })
    expect(credentialed.invalidReason).toContain("outcome URL must not contain credentials")
  })

  test("gateway starts primary work without waiting for shadow timeout", async () => {
    const published: string[] = []
    const events = {
      publish: (definition: { type: string }) => Effect.sync(() => published.push(definition.type)),
    } as unknown as EventV2.Interface
    const llm = {
      stream: () => {
        throw new Error("primary stream is intentionally not opened by gateway.start")
      },
    } as unknown as LLMClientShape
    const gateway = ModelInvocationGateway.make({
      events,
      llm,
      shadowCanary: ShadowCanary.config({
        OPENCODE_SHADOW_CANARY_ENABLED: "true",
        OPENCODE_SHADOW_CANARY_URL: `${endpoint}/slow`,
        OPENCODE_SHADOW_CANARY_SAMPLE_RATE: "1",
        OPENCODE_SHADOW_CANARY_TIMEOUT_MS: "25",
      }),
    })
    const started = Date.now()
    await Effect.runPromise(
      gateway.start({
        sessionID: "ses_shadow_canary" as never,
        agent: "build",
        model: { providerID: "test", modelID: "test" } as never,
        request: request(),
      }),
    )
    expect(Date.now() - started).toBeLessThan(200)
    expect(published).toContain("session.next.shadow.canary.dispatched")
    await Bun.sleep(80)
    expect(published).toContain("session.next.shadow.canary.failed")
  })
})

function input(invocationID: string) {
  return {
    invocationID,
    sessionID: "ses_real_shadow",
    agent: "build",
    model: { providerID: "test", modelID: "test" },
    request: request(),
  }
}

function request() {
  return {
    model: {
      provider: "test-provider",
      route: {
        id: "test-route",
        auth: { apiKey: "PROVIDER_SECRET_VALUE" },
      },
    },
    messages: [
      {
        role: "user",
        content: [{ type: "text", text: "REAL_SHADOW_REQUEST_MARKER" }],
      },
    ],
    headers: {
      authorization: "Bearer PROVIDER_SECRET_VALUE",
    },
    tools: {
      read: {
        description: "read",
        inputSchema: { type: "object" },
        execute: () => "must not serialize",
      },
    },
  } as unknown as LLMRequest
}
