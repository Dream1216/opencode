import { describe, expect, test } from "bun:test"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

describe("OpenCode SaaS identity configuration", () => {
  test("is opt-in and preserves local mode", () => {
    expect(SaasIdentity.config({}).enabled).toBe(false)
  })

  test("requires PostgreSQL, an external URL, and a strong secret", () => {
    expect(() => SaasIdentity.requireConfig({ OPENCODE_SAAS_MODE: "true" })).toThrow("OPENCODE_DATABASE_URL")
    expect(() =>
      SaasIdentity.requireConfig({
        OPENCODE_SAAS_MODE: "true",
        OPENCODE_DATABASE_URL: "postgres://localhost/opencode",
        OPENCODE_AUTH_URL: "https://code.example.test",
        OPENCODE_AUTH_SECRET: "short",
      }),
    ).toThrow("at least 32 characters")
  })

  test("normalizes trusted origins around the configured auth origin", () => {
    const value = SaasIdentity.requireConfig({
      OPENCODE_SAAS_MODE: "true",
      OPENCODE_DATABASE_URL: "postgres://localhost/opencode",
      OPENCODE_AUTH_URL: "https://code.example.test/auth",
      OPENCODE_AUTH_SECRET: "0123456789abcdef0123456789abcdef",
      OPENCODE_AUTH_TRUSTED_ORIGINS: "https://admin.example.test, https://code.example.test",
    })
    expect(value.trustedOrigins).toEqual(["https://admin.example.test", "https://code.example.test"])
  })
})
