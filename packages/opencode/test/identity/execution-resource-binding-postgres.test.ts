import { afterAll, describe, expect, test } from "bun:test"
import { ExecutionResourceBinding } from "@opencode-ai/core/identity/execution-resource-binding"
import { Organization } from "@opencode-ai/core/identity/organization"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

const databaseURL = process.env.OPENCODE_TEST_DATABASE_URL
const run = databaseURL === undefined ? describe.skip : describe

run("execution resource tenant binding", () => {
  const origin = "http://127.0.0.1:4097"
  const suffix = crypto.randomUUID()

  process.env.OPENCODE_SAAS_MODE = "true"
  process.env.OPENCODE_DATABASE_URL = databaseURL
  process.env.OPENCODE_AUTH_URL = origin
  process.env.OPENCODE_AUTH_SECRET = "0123456789abcdef0123456789abcdef"
  process.env.OPENCODE_SAAS_AUTO_MIGRATE = "true"

  afterAll(async () => {
    await ExecutionResourceBinding.shutdown()
    await Organization.shutdown()
    await SaasIdentity.shutdown()
  })

  test("binds project, session, and AgentRun while rejecting another organization", async () => {
    const owner = await register(origin, `binding-owner-${suffix}@example.test`, "Binding Owner")
    const outsider = await register(origin, `binding-outsider-${suffix}@example.test`, "Binding Outsider")
    const first = await Organization.create(owner.actor, {
      name: "Binding Organization",
      slug: `binding-${suffix.slice(0, 12)}`,
    })
    const second = await Organization.create(outsider.actor, {
      name: "Other Organization",
      slug: `other-${suffix.slice(0, 12)}`,
    })
    const firstContext = await ExecutionResourceBinding.authorize(owner.actor, first.id)
    const secondContext = await ExecutionResourceBinding.authorize(outsider.actor, second.id)
    const projectID = `project_${suffix}`
    const sessionID = `ses_${suffix.replaceAll("-", "")}`

    await ExecutionResourceBinding.bindProject(firstContext, {
      projectID,
      worktree: `/tmp/${projectID}`,
    })
    await ExecutionResourceBinding.bindSession(firstContext, { sessionID, projectID })

    expect(await ExecutionResourceBinding.allowedResourceIDs(firstContext, "project")).toContain(projectID)
    expect(await ExecutionResourceBinding.allowedResourceIDs(firstContext, "session")).toContain(sessionID)
    expect((await ExecutionResourceBinding.lookup("agent_run", `run_${sessionID}`))?.tenantID).toBe(
      firstContext.tenantID,
    )
    await expect(
      ExecutionResourceBinding.assertResource(secondContext, "session", sessionID),
    ).rejects.toMatchObject({ code: "resource_forbidden" })
    await expect(
      ExecutionResourceBinding.bindProject(secondContext, {
        projectID,
        worktree: `/tmp/${projectID}`,
      }),
    ).rejects.toMatchObject({ code: "resource_conflict" })
  })
})

async function register(origin: string, email: string, name: string) {
  const response = await SaasIdentity.handle(
    new Request(`${origin}/api/auth/sign-up/email`, {
      method: "POST",
      headers: { "content-type": "application/json", origin },
      body: JSON.stringify({ name, email, password: "correct-horse-battery-staple" }),
    }),
  )
  expect(response.status).toBe(200)
  const cookie = response.headers
    .getSetCookie()
    .map((value) => value.split(";")[0])
    .join("; ")
  const session = await SaasIdentity.authenticate(new Headers({ cookie }))
  if (!session) throw new Error("registration did not establish a session")
  return session
}
