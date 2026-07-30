import { describe, expect, test } from "bun:test"
import { readAuthState } from "./saas-auth"

describe("SaaS authentication gate", () => {
  test("keeps the upstream local experience when SaaS mode is disabled", async () => {
    const state = await readAuthState(() => Promise.resolve(Response.json({ error: "disabled" }, { status: 404 })))
    expect(state).toEqual({ mode: "disabled" })
  })

  test("requires login when Better Auth has no session", async () => {
    const state = await readAuthState(() => Promise.resolve(Response.json(null)))
    expect(state).toEqual({ mode: "anonymous" })
  })

  test("admits an authenticated user", async () => {
    const session = {
      user: { id: "usr_1", name: "Ada", email: "ada@example.test" },
      session: { id: "ses_1", expiresAt: new Date(Date.now() + 60_000).toISOString() },
    }
    const state = await readAuthState(() => Promise.resolve(Response.json(session)))
    expect(state).toEqual({ mode: "authenticated", session })
  })
})
