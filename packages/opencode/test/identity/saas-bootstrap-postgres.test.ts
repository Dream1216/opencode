import { afterAll, describe, expect, test } from "bun:test"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

const databaseURL = process.env.OPENCODE_P723_BOOTSTRAP_DATABASE_URL
const databaseName = databaseURL ? new URL(databaseURL).pathname.split("/").pop() : undefined
const run = databaseURL && databaseName?.startsWith("opencode_p723_") ? describe : describe.skip

run("OpenCode SaaS identity bootstrap", () => {
  process.env.OPENCODE_SAAS_MODE = "true"
  process.env.OPENCODE_DATABASE_URL = databaseURL
  process.env.OPENCODE_AUTH_URL = "http://127.0.0.1:45634"
  process.env.OPENCODE_AUTH_SECRET = "0123456789abcdef0123456789abcdef"
  process.env.OPENCODE_SAAS_AUTO_MIGRATE = "true"

  afterAll(async () => {
    await SaasIdentity.shutdown()
  })

  test("deduplicates concurrent empty-database bootstrap before the first registration", async () => {
    const results = await Promise.all(Array.from({ length: 8 }, () => SaasIdentity.bootstrap()))
    expect(results.every((result) => result.status === "ready")).toBe(true)
    expect(results[0]?.applied.length).toBeGreaterThan(0)
    expect(results.slice(1).every((result) => result.applied === results[0]?.applied)).toBe(true)

    const suffix = crypto.randomUUID()
    const response = await SaasIdentity.handle(
      new Request("http://127.0.0.1:45634/api/auth/sign-up/email", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          origin: "http://127.0.0.1:45634",
        },
        body: JSON.stringify({
          name: "P7.2.3 Bootstrap",
          email: `p723-${suffix}@example.invalid`,
          password: `P723-${suffix}-Aa1!`,
        }),
      }),
    )
    expect(response.status).toBe(200)
  })
})
