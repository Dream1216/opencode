import { describe, expect, test } from "bun:test"
import {
  hasOrganizationPermission,
  organizationSlugFromName,
  readOrganizations,
  type OrganizationPermissionPolicy,
} from "./organization"

describe("organization control plane state", () => {
  test("preserves local mode when the organization API is disabled", async () => {
    const state = await readOrganizations(() => Promise.resolve(undefined))
    expect(state).toEqual({ mode: "disabled" })
  })

  test("loads organizations for SaaS mode", async () => {
    const organizations = [
      {
        id: "org_1",
        slug: "runtime-team",
        name: "Runtime Team",
        status: "active" as const,
        role: "owner" as const,
        timeCreated: 1,
        timeUpdated: 1,
      },
    ]
    const state = await readOrganizations(() => Promise.resolve(organizations))
    expect(state).toEqual({ mode: "enabled", organizations })
  })

  test("derives a route-safe slug for onboarding", () => {
    expect(organizationSlugFromName("  Agent Runtime / China  ")).toBe("agent-runtime-china")
  })

  test("uses server-returned effective permissions instead of role guesses", () => {
    const policy: OrganizationPermissionPolicy = {
      organizationID: "org_1",
      version: 1,
      catalog: [],
      roles: [],
      actor: { actorID: "actor_1", role: "viewer", permissions: ["session.view"] },
    }
    expect(hasOrganizationPermission(policy, "session.view")).toBe(true)
    expect(hasOrganizationPermission(policy, "session.create")).toBe(false)
  })
})
