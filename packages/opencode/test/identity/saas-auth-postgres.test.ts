import { afterAll, describe, expect, test } from "bun:test"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

const databaseURL = process.env.OPENCODE_TEST_DATABASE_URL
const run = databaseURL === undefined ? describe.skip : describe

run("Better Auth PostgreSQL adapter", () => {
  const origin = "http://127.0.0.1:4096"
  const email = `p6-${crypto.randomUUID()}@example.test`
  let cookie = ""

  process.env.OPENCODE_SAAS_MODE = "true"
  process.env.OPENCODE_DATABASE_URL = databaseURL
  process.env.OPENCODE_AUTH_URL = origin
  process.env.OPENCODE_AUTH_SECRET = "0123456789abcdef0123456789abcdef"
  process.env.OPENCODE_SAAS_AUTO_MIGRATE = "true"

  afterAll(() => SaasIdentity.shutdown())

  test("registers a user and establishes a database session", async () => {
    const response = await SaasIdentity.handle(
      new Request(`${origin}/api/auth/sign-up/email`, {
        method: "POST",
        headers: { "content-type": "application/json", origin },
        body: JSON.stringify({ name: "P6 User", email, password: "correct-horse-battery-staple" }),
      }),
    )
    expect(response.status).toBe(200)
    cookie = response.headers
      .getSetCookie()
      .map((value) => value.split(";")[0])
      .join("; ")
    expect(cookie).toContain("better-auth.session_token")
  })

  test("resolves the registered actor from the session cookie", async () => {
    const session = await SaasIdentity.authenticate(new Headers({ cookie }))
    expect(session?.actor.email).toBe(email)
    expect(session?.actor.platformRoles).toEqual(["user"])
    expect(session?.sessionID).toBeTruthy()
  })
})
