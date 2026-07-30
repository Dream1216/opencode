import { describe, expect, test } from "bun:test"
import { Organization } from "../src/identity/organization"

describe("organization domain", () => {
  test("normalizes valid slugs", () => {
    expect(Organization.organizationSlug("  ACME-Platform ")).toBe("acme-platform")
  })

  test("rejects slugs that cannot be safely routed", () => {
    expect(() => Organization.organizationSlug("a")).toThrow("3..63")
    expect(() => Organization.organizationSlug("acme/platform")).toThrow("3..63")
  })
})
