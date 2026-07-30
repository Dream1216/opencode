import { describe, expect, test } from "bun:test"
import { ExecutionResourceBinding } from "../src/identity/execution-resource-binding"

describe("execution resource binding domain", () => {
  test("reads the active organization from the canonical request header", () => {
    expect(
      ExecutionResourceBinding.organizationID(
        new Headers({ "X-OpenCode-Organization-ID": "  organization_1  " }),
      ),
    ).toBe("organization_1")
  })

  test("rejects a request without an active organization", () => {
    expect(() => ExecutionResourceBinding.organizationID(new Headers())).toThrow(
      "x-opencode-organization-id is required",
    )
  })
})
