import { describe, expect, test } from "bun:test"
import { readAuthState, signOutAuthSession } from "./saas-auth"

describe("SaaS authentication HTTP contract", () => {
  test("sign out sends the Better Auth JSON request contract", async () => {
    let captured: { readonly input: RequestInfo | URL; readonly init?: RequestInit } | undefined

    await signOutAuthSession(async (input, init) => {
      captured = { input, init }
      return Response.json({ success: true })
    })

    expect(captured?.input).toBe("/api/auth/sign-out")
    expect(captured?.init).toEqual({
      method: "POST",
      credentials: "include",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: "{}",
    })
  })

  test("sign out surfaces the identity service failure", async () => {
    const operation = signOutAuthSession(async () =>
      Response.json(
        { message: "Content-Type is required. Allowed types: application/json", code: "UNSUPPORTED_MEDIA_TYPE" },
        { status: 415 },
      ),
    )

    await expect(operation).rejects.toThrow(
      "Sign out failed: Content-Type is required. Allowed types: application/json",
    )
  })

  test("session rate limiting returns a retryable authentication message", async () => {
    const operation = readAuthState(async () =>
      Response.json(
        { message: "Too many requests" },
        {
          status: 429,
          headers: { "retry-after": "30" },
        },
      ),
    )

    await expect(operation).rejects.toThrow(
      "Authentication is temporarily rate limited. Try again in 30 seconds.",
    )
  })
})
