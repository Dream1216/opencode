import { afterAll, describe, expect, test } from "bun:test"
import { Organization } from "@opencode-ai/core/identity/organization"
import { PermissionCenter } from "@opencode-ai/core/identity/permission-center"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"

const databaseURL = process.env.OPENCODE_TEST_DATABASE_URL
const run = databaseURL === undefined ? describe.skip : describe

run("permission center PostgreSQL policy", () => {
  const origin = "http://127.0.0.1:4097"
  const suffix = crypto.randomUUID()

  process.env.OPENCODE_SAAS_MODE = "true"
  process.env.OPENCODE_DATABASE_URL = databaseURL
  process.env.OPENCODE_AUTH_URL = origin
  process.env.OPENCODE_AUTH_SECRET = "0123456789abcdef0123456789abcdef"
  process.env.OPENCODE_SAAS_AUTO_MIGRATE = "true"

  afterAll(async () => {
    await PermissionCenter.shutdown()
    await Organization.shutdown()
    await SaasIdentity.shutdown()
  })

  test("authorizes defaults, applies versioned overrides, and rejects stale writes", async () => {
    const owner = await register(origin, `permission-owner-${suffix}@example.test`, "Permission Owner")
    const member = await register(origin, `permission-member-${suffix}@example.test`, "Permission Member")
    const organization = await Organization.create(owner.actor, {
      name: "Permission Center",
      slug: `permission-${suffix.slice(0, 12)}`,
    })
    const invitation = await Organization.invite(owner.actor, organization.id, {
      email: member.actor.email,
      role: "member",
    })
    await Organization.acceptInvitation(member.actor, invitation.token)

    await expect(
      PermissionCenter.authorize(member.actor, organization.id, "organization.members.invite"),
    ).rejects.toMatchObject({ code: "permission_denied" })

    const initial = await PermissionCenter.policy(owner.actor, organization.id)
    const next = await PermissionCenter.replaceRolePermissions(owner.actor, organization.id, "member", {
      permissions: [
        ...PermissionCenter.defaultPermissions("member"),
        "organization.members.invite",
      ],
      expectedVersion: initial.version,
    })
    expect(next.version).toBe(initial.version + 1)
    await expect(
      PermissionCenter.authorize(member.actor, organization.id, "organization.members.invite"),
    ).resolves.toMatchObject({ role: "member" })

    await expect(
      PermissionCenter.replaceRolePermissions(owner.actor, organization.id, "member", {
        permissions: PermissionCenter.defaultPermissions("member"),
        expectedVersion: initial.version,
      }),
    ).rejects.toMatchObject({ code: "permission_conflict" })

    await expect(
      PermissionCenter.replaceRolePermissions(owner.actor, organization.id, "member", {
        permissions: ["organization.owner.transfer"],
        expectedVersion: next.version,
      }),
    ).rejects.toMatchObject({ code: "permission_bad_request" })
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
