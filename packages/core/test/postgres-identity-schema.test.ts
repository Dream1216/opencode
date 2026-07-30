import { describe, expect, test } from "bun:test"
import { allStatements, migrations, validateDraft } from "../src/database/postgres/schema"

describe("P6 identity control plane schema", () => {
  test("is part of the advisory-lock PostgreSQL migration sequence", () => {
    const migration = migrations.find((item) => item.id === "p6_0_001_identity_control_plane")
    expect(migration).toBeDefined()
    expect(migration?.statements.join("\n")).toContain("opencode_identity_user")
    expect(migration?.statements.join("\n")).toContain("opencode_organization_member")
  })

  test("keeps Better Auth and tenant ownership primitives in the draft contract", () => {
    const sql = allStatements().join("\n")
    expect(sql).toContain("opencode_identity_session")
    expect(sql).toContain("tenant_organization_slug_idx")
    expect(validateDraft()).toEqual([])
  })
})
