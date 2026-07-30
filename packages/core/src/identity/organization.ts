export * as Organization from "./organization"

import { createHash, randomBytes, randomUUID } from "node:crypto"
import type { Sql, TransactionSql } from "postgres"
import type { ActorContext } from "../identity"
import { makeClient } from "../database/postgres/client"
import { applyMigrations } from "../database/postgres/migration"
import { PermissionCenter } from "./permission-center"
import { requireConfig } from "./saas-auth"

export type Role = "owner" | "admin" | "member" | "viewer" | "billing_admin"
export type InvitationRole = Exclude<Role, "owner">

export type Summary = {
  readonly id: string
  readonly slug: string
  readonly name: string
  readonly status: "active" | "suspended" | "deleted"
  readonly role: Role
  readonly timeCreated: number
  readonly timeUpdated: number
}

export type Member = {
  readonly actorID: string
  readonly email: string
  readonly name: string
  readonly role: Role
  readonly status: "invited" | "active" | "suspended"
  readonly timeCreated: number
  readonly timeUpdated: number
}

export type Invitation = {
  readonly id: string
  readonly organizationID: string
  readonly email: string
  readonly role: InvitationRole
  readonly invitedBy: string
  readonly status: "pending" | "accepted" | "rejected" | "revoked" | "expired"
  readonly expiresAt: number
  readonly timeCreated: number
  readonly timeUpdated: number
}

export class Error extends globalThis.Error {
  constructor(
    readonly code:
      | "organization_not_found"
      | "organization_forbidden"
      | "organization_conflict"
      | "organization_bad_request"
      | "invitation_invalid"
      | "invitation_expired",
    message: string,
  ) {
    super(message)
    this.name = "OrganizationError"
  }
}

let database: Promise<Sql> | undefined

export async function create(actor: ActorContext, input: { readonly name: string; readonly slug: string }) {
  const name = organizationName(input.name)
  const slug = organizationSlug(input.slug)
  const sql = await load()
  const organizationID = randomUUID()
  const now = Date.now()
  try {
    await sql.begin(async (tx) => {
      await tx`
        insert into opencode_organization (
          id, slug, name, owner_actor_id, status, time_created, time_updated
        ) values (
          ${organizationID}, ${slug}, ${name}, ${actor.actorID}, 'active', ${now}, ${now}
        )
      `
      await tx`
        insert into opencode_organization_member (
          organization_id, actor_id, role, status, time_created, time_updated
        ) values (
          ${organizationID}, ${actor.actorID}, 'owner', 'active', ${now}, ${now}
        )
      `
      await audit(tx, organizationID, actor.actorID, "organization.create", "organization", organizationID, {
        slug,
      })
    })
  } catch (error) {
    if (postgresCode(error) === "23505") throw new Error("organization_conflict", "Organization slug is already used")
    throw error
  }
  return { id: organizationID, slug, name, status: "active" as const, role: "owner" as const, timeCreated: now, timeUpdated: now }
}

export async function list(actor: ActorContext): Promise<readonly Summary[]> {
  const sql = await load()
  const rows = await sql<OrganizationRow[]>`
    select o.id, o.slug, o.name, o.status, o.time_created, o.time_updated, m.role
    from opencode_organization_member m
    join opencode_organization o on o.id = m.organization_id
    where m.actor_id = ${actor.actorID}
      and m.status = 'active'
      and o.status <> 'deleted'
    order by o.name, o.id
  `
  return rows.map(summary)
}

export async function members(actor: ActorContext, organizationID: string): Promise<readonly Member[]> {
  const sql = await load()
  await requirePermission(sql, actor, organizationID, "organization.members.view")
  const rows = await sql<MemberRow[]>`
    select
      m.actor_id, u.email, u.name, m.role, m.status, m.time_created, m.time_updated
    from opencode_organization_member m
    join opencode_identity_user u on u.id = m.actor_id
    where m.organization_id = ${organizationID}
    order by
      case m.role when 'owner' then 0 when 'admin' then 1 else 2 end,
      u.name,
      u.email
  `
  return rows.map(member)
}

export async function invite(
  actor: ActorContext,
  organizationID: string,
  input: { readonly email: string; readonly role: InvitationRole; readonly expiresInMs?: number },
) {
  const email = normalizeEmail(input.email)
  const role = invitationRole(input.role)
  const sql = await load()
  const token = randomBytes(32).toString("base64url")
  const tokenDigest = digest(token)
  const invitationID = randomUUID()
  const now = Date.now()
  const expiresAt = now + Math.min(Math.max(input.expiresInMs ?? 7 * 24 * 60 * 60 * 1000, 60_000), 30 * 24 * 60 * 60 * 1000)

  await sql.begin(async (tx) => {
    await requirePermission(tx, actor, organizationID, "organization.members.invite")
    const existing = await tx<{ actor_id: string }[]>`
      select u.id as actor_id
      from opencode_identity_user u
      join opencode_organization_member m on m.actor_id = u.id
      where m.organization_id = ${organizationID}
        and lower(u.email) = ${email}
        and m.status = 'active'
      limit 1
    `
    if (existing[0] !== undefined) throw new Error("organization_conflict", "User is already an organization member")
    await tx`
      update opencode_organization_invitation
      set status = 'revoked', time_updated = ${now}
      where organization_id = ${organizationID}
        and email = ${email}
        and status = 'pending'
    `
    await tx`
      insert into opencode_organization_invitation (
        id, organization_id, email, role, token_digest, invited_by, status,
        expires_at, time_created, time_updated
      ) values (
        ${invitationID}, ${organizationID}, ${email}, ${role}, ${tokenDigest}, ${actor.actorID},
        'pending', ${expiresAt}, ${now}, ${now}
      )
    `
    await audit(tx, organizationID, actor.actorID, "organization.invitation.create", "invitation", invitationID, {
      email,
      role,
    })
  })

  return {
    invitation: {
      id: invitationID,
      organizationID,
      email,
      role,
      invitedBy: actor.actorID,
      status: "pending" as const,
      expiresAt,
      timeCreated: now,
      timeUpdated: now,
    },
    token,
  }
}

export async function invitations(actor: ActorContext, organizationID: string): Promise<readonly Invitation[]> {
  const sql = await load()
  await requirePermission(sql, actor, organizationID, "organization.members.invite")
  const rows = await sql<InvitationRow[]>`
    select id, organization_id, email, role, invited_by, status, expires_at, time_created, time_updated
    from opencode_organization_invitation
    where organization_id = ${organizationID}
    order by time_created desc
  `
  return rows.map(invitation)
}

export async function acceptInvitation(actor: ActorContext, token: string) {
  return decideInvitation(actor, token, "accepted")
}

export async function rejectInvitation(actor: ActorContext, token: string) {
  return decideInvitation(actor, token, "rejected")
}

export async function updateMemberRole(
  actor: ActorContext,
  organizationID: string,
  targetActorID: string,
  role: InvitationRole,
) {
  const nextRole = invitationRole(role)
  const sql = await load()
  await sql.begin(async (tx) => {
    const manager = await requireMembership(tx, actor.actorID, organizationID)
    await requirePermission(tx, actor, organizationID, "organization.members.manage")
    const target = await requireMembership(tx, targetActorID, organizationID)
    if (target.role === "owner") throw new Error("organization_conflict", "Transfer ownership before changing this role")
    if (manager.role === "admin" && target.role === "admin") {
      throw new Error("organization_forbidden", "Administrators cannot change another administrator")
    }
    const now = Date.now()
    await tx`
      update opencode_organization_member
      set role = ${nextRole}, time_updated = ${now}
      where organization_id = ${organizationID} and actor_id = ${targetActorID}
    `
    await audit(tx, organizationID, actor.actorID, "organization.member.role.update", "member", targetActorID, {
      previousRole: target.role,
      role: nextRole,
    })
  })
  return { actorID: targetActorID, role: nextRole }
}

export async function removeMember(actor: ActorContext, organizationID: string, targetActorID: string) {
  const sql = await load()
  await sql.begin(async (tx) => {
    const requester = await requireMembership(tx, actor.actorID, organizationID)
    const target = await requireMembership(tx, targetActorID, organizationID)
    if (target.role === "owner") throw new Error("organization_conflict", "Transfer ownership before removing the owner")
    if (actor.actorID !== targetActorID) {
      await requirePermission(tx, actor, organizationID, "organization.members.manage")
      if (requester.role === "admin" && target.role === "admin") {
        throw new Error("organization_forbidden", "Administrators cannot remove another administrator")
      }
    }
    await tx`
      delete from opencode_organization_member
      where organization_id = ${organizationID} and actor_id = ${targetActorID}
    `
    await audit(tx, organizationID, actor.actorID, "organization.member.remove", "member", targetActorID, {
      role: target.role,
    })
  })
}

export async function transferOwnership(actor: ActorContext, organizationID: string, targetActorID: string) {
  const sql = await load()
  await sql.begin(async (tx) => {
    const requester = await requireMembership(tx, actor.actorID, organizationID)
    await requirePermission(tx, actor, organizationID, "organization.owner.transfer")
    const target = await requireMembership(tx, targetActorID, organizationID)
    if (target.status !== "active") throw new Error("organization_conflict", "New owner must be an active member")
    if (targetActorID === actor.actorID) return
    const now = Date.now()
    await tx`
      update opencode_organization
      set owner_actor_id = ${targetActorID}, time_updated = ${now}
      where id = ${organizationID}
    `
    await tx`
      update opencode_organization_member
      set role = case when actor_id = ${targetActorID} then 'owner' else 'admin' end,
          time_updated = ${now}
      where organization_id = ${organizationID}
        and actor_id in (${actor.actorID}, ${targetActorID})
    `
    await audit(tx, organizationID, actor.actorID, "organization.ownership.transfer", "member", targetActorID, {
      previousOwner: actor.actorID,
    })
  })
  return { organizationID, ownerActorID: targetActorID }
}

export async function shutdown() {
  if (database === undefined) return
  const sql = await database
  database = undefined
  await sql.end({ timeout: 5 })
}

async function decideInvitation(actor: ActorContext, token: string, decision: "accepted" | "rejected") {
  if (token.length < 32 || token.length > 256) throw new Error("invitation_invalid", "Invitation token is invalid")
  const sql = await load()
  return sql.begin(async (tx) => {
    const rows = await tx<InvitationRow[]>`
      select id, organization_id, email, role, invited_by, status, expires_at, time_created, time_updated
      from opencode_organization_invitation
      where token_digest = ${digest(token)}
      for update
    `
    const value = rows[0]
    if (value === undefined || value.status !== "pending") {
      throw new Error("invitation_invalid", "Invitation is invalid or already decided")
    }
    const now = Date.now()
    if (Number(value.expires_at) <= now) {
      await tx`
        update opencode_organization_invitation
        set status = 'expired', time_updated = ${now}
        where id = ${value.id}
      `
      throw new Error("invitation_expired", "Invitation has expired")
    }
    if (normalizeEmail(actor.email) !== value.email) {
      throw new Error("organization_forbidden", "Invitation belongs to another email address")
    }
    if (decision === "accepted") {
      await tx`
        insert into opencode_organization_member (
          organization_id, actor_id, role, status, time_created, time_updated
        ) values (
          ${value.organization_id}, ${actor.actorID}, ${value.role}, 'active', ${now}, ${now}
        )
        on conflict (organization_id, actor_id)
        do update set role = excluded.role, status = 'active', time_updated = excluded.time_updated
      `
    }
    await tx`
      update opencode_organization_invitation
      set status = ${decision}, accepted_by = ${actor.actorID}, time_updated = ${now}
      where id = ${value.id}
    `
    await audit(
      tx,
      value.organization_id,
      actor.actorID,
      `organization.invitation.${decision}`,
      "invitation",
      value.id,
      { role: value.role },
    )
    return { organizationID: value.organization_id, role: value.role, status: decision }
  })
}

async function load() {
  return (database ??= initialize())
}

async function initialize() {
  const settings = requireConfig()
  const sql = makeClient({ url: settings.databaseURL, max: 4 })
  await applyMigrations(sql)
  return sql
}

async function requireMembership(sql: { readonly unsafe: Sql["unsafe"] }, actorID: string, organizationID: string) {
  const rows = await sql.unsafe<{ role: Role; status: Member["status"] }[]>(
    "select role, status from opencode_organization_member where organization_id = $1 and actor_id = $2 limit 1",
    [organizationID, actorID],
  )
  const value = rows[0]
  if (value === undefined || value.status !== "active") {
    throw new Error("organization_forbidden", "Active organization membership required")
  }
  return value
}

async function requirePermission(
  sql: { readonly unsafe: Sql["unsafe"] },
  actor: ActorContext,
  organizationID: string,
  permission: PermissionCenter.PermissionKey,
) {
  try {
    return await PermissionCenter.authorizeWith(sql, actor, organizationID, permission)
  } catch (error) {
    if (error instanceof PermissionCenter.Error && error.code === "permission_denied") {
      throw new Error("organization_forbidden", error.message)
    }
    throw error
  }
}

async function audit(
  tx: TransactionSql<Record<string, unknown>>,
  organizationID: string,
  actorID: string,
  action: string,
  targetType: string,
  targetID: string,
  metadata: Readonly<Record<string, string>>,
) {
  await tx`
    insert into opencode_organization_audit (
      id, organization_id, actor_id, action, target_type, target_id, outcome, metadata, time_created
    ) values (
      ${randomUUID()}, ${organizationID}, ${actorID}, ${action}, ${targetType}, ${targetID},
      'allowed', ${tx.json(metadata)}, ${Date.now()}
    )
  `
}

function summary(row: OrganizationRow): Summary {
  return {
    id: row.id,
    slug: row.slug,
    name: row.name,
    status: row.status,
    role: row.role,
    timeCreated: Number(row.time_created),
    timeUpdated: Number(row.time_updated),
  }
}

function member(row: MemberRow): Member {
  return {
    actorID: row.actor_id,
    email: row.email,
    name: row.name,
    role: row.role,
    status: row.status,
    timeCreated: Number(row.time_created),
    timeUpdated: Number(row.time_updated),
  }
}

function invitation(row: InvitationRow): Invitation {
  return {
    id: row.id,
    organizationID: row.organization_id,
    email: row.email,
    role: row.role,
    invitedBy: row.invited_by,
    status: row.status,
    expiresAt: Number(row.expires_at),
    timeCreated: Number(row.time_created),
    timeUpdated: Number(row.time_updated),
  }
}

export function organizationSlug(value: string) {
  const normalized = value.trim().toLowerCase()
  if (!/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(normalized)) {
    throw new Error("organization_bad_request", "Slug must be 3..63 lowercase letters, numbers, or hyphens")
  }
  return normalized
}

function organizationName(value: string) {
  const normalized = value.trim()
  if (normalized.length < 2 || normalized.length > 120) {
    throw new Error("organization_bad_request", "Organization name must contain 2..120 characters")
  }
  return normalized
}

function normalizeEmail(value: string) {
  const normalized = value.trim().toLowerCase()
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(normalized) || normalized.length > 320) {
    throw new Error("organization_bad_request", "Email address is invalid")
  }
  return normalized
}

function invitationRole(value: InvitationRole) {
  if (!["admin", "member", "viewer", "billing_admin"].includes(value)) {
    throw new Error("organization_bad_request", "Invitation role is invalid")
  }
  return value
}

function digest(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function postgresCode(error: unknown) {
  return error !== null && typeof error === "object" && "code" in error ? String(error.code) : undefined
}

type OrganizationRow = {
  readonly id: string
  readonly slug: string
  readonly name: string
  readonly status: Summary["status"]
  readonly role: Role
  readonly time_created: string | number
  readonly time_updated: string | number
}

type MemberRow = {
  readonly actor_id: string
  readonly email: string
  readonly name: string
  readonly role: Role
  readonly status: Member["status"]
  readonly time_created: string | number
  readonly time_updated: string | number
}

type InvitationRow = {
  readonly id: string
  readonly organization_id: string
  readonly email: string
  readonly role: InvitationRole
  readonly invited_by: string
  readonly status: Invitation["status"]
  readonly expires_at: string | number
  readonly time_created: string | number
  readonly time_updated: string | number
}
