import { describe, expect, test } from "bun:test"
import { permissionForRequest } from "../../src/server/routes/instance/httpapi/middleware/organization-execution"

describe("permission center execution routing", () => {
  test("maps project and session reads separately from mutations", () => {
    expect(permissionForRequest("/project", "GET")).toBe("project.view")
    expect(permissionForRequest("/project/prj_1", "PATCH")).toBe("project.manage")
    expect(permissionForRequest("/api/session", "GET")).toBe("session.view")
    expect(permissionForRequest("/api/session", "POST")).toBe("session.create")
    expect(permissionForRequest("/api/session/ses_1", "DELETE")).toBe("session.manage")
  })

  test("maps AgentRun controls to explicit execute and stop permissions", () => {
    expect(permissionForRequest("/api/session/ses_1/message", "POST")).toBe("agent_run.execute")
    expect(permissionForRequest("/api/session/ses_1/abort", "POST")).toBe("agent_run.stop")
    expect(permissionForRequest("/permission/perm_1", "POST")).toBe("agent_run.execute")
  })
})
