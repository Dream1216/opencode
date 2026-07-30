import { afterAll, describe, expect, test } from "bun:test"
import { Organization } from "@opencode-ai/core/identity/organization"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

const databaseURL = process.env.OPENCODE_TEST_DATABASE_URL
const run = databaseURL === undefined ? describe.skip : describe

run("organization membership lifecycle", () => {
  const origin = "http://127.0.0.1:4096"
  const suffix = crypto.randomUUID()

  process.env.OPENCODE_SAAS_MODE = "true"
  process.env.OPENCODE_DATABASE_URL = databaseURL
  process.env.OPENCODE_AUTH_URL = origin
  process.env.OPENCODE_AUTH_SECRET = "0123456789abcdef0123456789abcdef"
  process.env.OPENCODE_SAAS_AUTO_MIGRATE = "true"

  afterAll(async () => {
    await Organization.shutdown()
    await SaasIdentity.shutdown()
  })

  test("creates, invites, authorizes, changes roles, and transfers ownership", async () => {
    const owner = await register(origin, `owner-${suffix}@example.test`, "Owner")
    const invited = await register(origin, `member-${suffix}@example.test`, "Member")
    const outsider = await register(origin, `outsider-${suffix}@example.test`, "Outsider")

    const organization = await Organization.create(owner.actor, {
      name: "P6 Organization",
      slug: `p6-${suffix.slice(0, 12)}`,
    })
    expect((await Organization.list(owner.actor))[0]?.id).toBe(organization.id)

    const issued = await Organization.invite(owner.actor, organization.id, {
      email: invited.actor.email,
      role: "member",
    })
    expect(issued.token.length).toBeGreaterThan(32)
    await Organization.acceptInvitation(invited.actor, issued.token)

    expect((await Organization.members(invited.actor, organization.id)).map((item) => item.email)).toContain(
      invited.actor.email,
    )
    await expect(
      Organization.invite(invited.actor, organization.id, {
        email: outsider.actor.email,
        role: "viewer",
      }),
    ).rejects.toMatchObject({ code: "organization_forbidden" })

    await Organization.updateMemberRole(owner.actor, organization.id, invited.actor.actorID, "admin")
    await Organization.transferOwnership(owner.actor, organization.id, invited.actor.actorID)
    const roles = new Map((await Organization.members(invited.actor, organization.id)).map((item) => [item.actorID, item.role]))
    expect(roles.get(invited.actor.actorID)).toBe("owner")
    expect(roles.get(owner.actor.actorID)).toBe("admin")

    const rejected = await Organization.invite(invited.actor, organization.id, {
      email: outsider.actor.email,
      role: "viewer",
    })
    expect((await Organization.invitations(invited.actor, organization.id))[0]?.status).toBe("pending")
    await Organization.rejectInvitation(outsider.actor, rejected.token)
    expect((await Organization.invitations(invited.actor, organization.id))[0]?.status).toBe("rejected")

    await Organization.removeMember(invited.actor, organization.id, owner.actor.actorID)
    expect((await Organization.members(invited.actor, organization.id)).map((item) => item.actorID)).not.toContain(
      owner.actor.actorID,
    )
    await expect(Organization.members(outsider.actor, organization.id)).rejects.toMatchObject({
      code: "organization_forbidden",
    })
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
  if (session === undefined) throw new Error("registration did not establish a session")
  return session
}
