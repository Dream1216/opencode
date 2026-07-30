import { describe, expect, test } from "bun:test"
import { PermissionCenter } from "../src/identity/permission-center"

describe("permission center domain", () => {
  test("keeps owner immutable and roles least-privileged by default", () => {
    expect(PermissionCenter.defaultPermissions("owner")).toHaveLength(PermissionCenter.Catalog.length)
    expect(PermissionCenter.defaultPermissions("viewer")).toContain("session.view")
    expect(PermissionCenter.defaultPermissions("viewer")).not.toContain("session.create")
    expect(PermissionCenter.defaultPermissions("billing_admin")).toContain("billing.manage")
    expect(PermissionCenter.defaultPermissions("billing_admin")).not.toContain("agent_run.execute")
  })

  test("normalizes policy order and rejects unknown permissions", () => {
    expect(
      PermissionCenter.normalizePermissions(["session.view", "project.view", "session.view"]),
    ).toEqual(["project.view", "session.view"])
    expect(() => PermissionCenter.normalizePermissions(["root.everything"])).toThrow("Unknown permission")
  })
})
