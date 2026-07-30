export * as PermissionCenter from "./permission-center"

import { randomUUID } from "node:crypto"
import type { Sql, TransactionSql } from "postgres"
import type { ActorContext } from "../identity"
import { makeClient } from "../database/postgres/client"
import { applyMigrations } from "../database/postgres/migration"
import { requireConfig } from "./saas-auth"

export type Role = "owner" | "admin" | "member" | "viewer" | "billing_admin"
export type EditableRole = Exclude<Role, "owner">

export const Catalog = [
  {
    key: "organization.view",
    group: "Organization",
    label: "View organization",
    description: "Read organization identity and active context.",
    delegable: true,
  },
  {
    key: "organization.members.view",
    group: "Organization",
    label: "View members",
    description: "Read members and their assigned roles.",
    delegable: true,
  },
  {
    key: "organization.members.invite",
    group: "Organization",
    label: "Invite members",
    description: "Issue invitations and inspect pending invitations.",
    delegable: true,
  },
  {
    key: "organization.members.manage",
    group: "Organization",
    label: "Manage members",
    description: "Change roles and remove organization members.",
    delegable: true,
  },
  {
    key: "organization.permissions.view",
    group: "Governance",
    label: "View permission policy",
    description: "Inspect role grants and effective permissions.",
    delegable: true,
  },
  {
    key: "organization.permissions.manage",
    group: "Governance",
    label: "Manage permission policy",
    description: "Replace role grants through versioned policy updates.",
    delegable: true,
  },
  {
    key: "organization.owner.transfer",
    group: "Governance",
    label: "Transfer ownership",
    description: "Transfer the immutable organization owner role.",
    delegable: false,
  },
  {
    key: "project.view",
    group: "Workspace",
    label: "View projects",
    description: "List and inspect organization-bound projects.",
    delegable: true,
  },
  {
    key: "project.manage",
    group: "Workspace",
    label: "Manage projects",
    description: "Initialize or update organization-bound projects.",
    delegable: true,
  },
  {
    key: "session.view",
    group: "Execution",
    label: "View sessions",
    description: "List and read organization-bound sessions.",
    delegable: true,
  },
  {
    key: "session.create",
    group: "Execution",
    label: "Create sessions",
    description: "Create or fork sessions in bound projects.",
    delegable: true,
  },
  {
    key: "session.manage",
    group: "Execution",
    label: "Manage sessions",
    description: "Rename, archive, revert, share, or delete sessions.",
    delegable: true,
  },
  {
    key: "agent_run.execute",
    group: "Execution",
    label: "Execute agents",
    description: "Submit prompts, commands, answers, and tool decisions.",
    delegable: true,
  },
  {
    key: "agent_run.stop",
    group: "Execution",
    label: "Stop agents",
    description: "Abort active AgentRun execution.",
    delegable: true,
  },
  {
    key: "audit.view",
    group: "Governance",
    label: "View audit",
    description: "Read organization authorization and governance records.",
    delegable: true,
  },
  {
    key: "billing.view",
    group: "Billing",
    label: "View billing",
    description: "Read usage, cost, and billing summaries.",
    delegable: true,
  },
  {
    key: "billing.manage",
    group: "Billing",
    label: "Manage billing",
    description: "Manage plans, quotas, and billing settings.",
    delegable: true,
  },
] as const

export type PermissionKey = (typeof Catalog)[number]["key"]
export type CatalogItem = (typeof Catalog)[number]

const defaults: Record<Exclude<Role, "owner">, readonly PermissionKey[]> = {
  admin: [
    "organization.view",
    "organization.members.view",
    "organization.members.invite",
    "organization.members.manage",
    "organization.permissions.view",
    "project.view",
    "project.manage",
    "session.view",
    "session.create",
    "session.manage",
    "agent_run.execute",
    "agent_run.stop",
    "audit.view",
    "billing.view",
  ],
  member: [
    "organization.view",
    "organization.members.view",
    "project.view",
    "project.manage",
    "session.view",
    "session.create",
    "session.manage",
    "agent_run.execute",
    "agent_run.stop",
  ],
  viewer: [
    "organization.view",
    "organization.members.view",
    "project.view",
    "session.view",
  ],
  billing_admin: [
    "organization.view",
    "organization.members.view",
    "organization.permissions.view",
    "billing.view",
    "billing.manage",
  ],
}

export type RolePolicy = {
  readonly role: Role
  readonly permissions: readonly PermissionKey[]
  readonly mutable: boolean
}

export type Policy = {
  readonly organizationID: string
  readonly version: number
  readonly catalog: readonly CatalogItem[]
  readonly roles: readonly RolePolicy[]
  readonly actor: {
    readonly actorID: string
    readonly role: Role
    readonly permissions: readonly PermissionKey[]
  }
}

export class Error extends globalThis.Error {
  constructor(
    readonly code:
      | "permission_bad_request"
      | "permission_denied"
      | "permission_conflict",
    message: string,
  ) {
    super(message)
    this.name = "PermissionCenterError"
  }
}

let database: Promise<Sql> | undefined

export function defaultPermissions(role: Role): readonly PermissionKey[] {
  return role === "owner" ? Catalog.map((item) => item.key) : defaults[role]
}

export function normalizePermissions(values: readonly string[]): readonly PermissionKey[] {
  const unique = new Set<PermissionKey>()
  for (const value of values) {
    if (!isPermissionKey(value)) throw new Error("permission_bad_request", `Unknown permission: ${value}`)
    unique.add(value)
  }
  return Catalog.map((item) => item.key).filter((permission) => unique.has(permission))
}

export async function authorize(actor: ActorContext, organizationID: string, permission: PermissionKey) {
  return authorizeWith(await load(), actor, organizationID, permission)
}

export async function authorizeWith(
  sql: Queryable,
  actor: ActorContext,
  organizationID: string,
  permission: PermissionKey,
) {
  const member = await membership(sql, actor.actorID, organizationID)
  const permissions = await effectivePermissions(sql, organizationID, member.role)
  if (!permissions.includes(permission)) {
    throw new Error("permission_denied", `Permission required: ${permission}`)
  }
  return {
    actorID: actor.actorID,
    organizationID,
    role: member.role,
    permission,
  }
}

export async function policy(actor: ActorContext, organizationID: string): Promise<Policy> {
  const sql = await load()
  await authorizeWith(sql, actor, organizationID, "organization.permissions.view")
  await ensurePolicy(sql, actor.actorID, organizationID)
  return snapshot(sql, actor, organizationID)
}

export async function replaceRolePermissions(
  actor: ActorContext,
  organizationID: string,
  role: EditableRole,
  input: {
    readonly permissions: readonly string[]
    readonly expectedVersion: number
  },
) {
  if (!isEditableRole(role)) throw new Error("permission_bad_request", "Owner policy is immutable")
  if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 1) {
    throw new Error("permission_bad_request", "A positive expectedVersion is required")
  }
  const permissions = normalizePermissions(input.permissions)
  const immutable = Catalog.filter((item) => !item.delegable).map((item) => item.key)
  if (permissions.some((permission) => immutable.includes(permission as (typeof immutable)[number]))) {
    throw new Error("permission_bad_request", "Owner-only permissions cannot be delegated")
  }

  const sql = await load()
  await sql.begin(async (tx) => {
    await ensurePolicy(tx, actor.actorID, organizationID)
    await authorizeWith(tx, actor, organizationID, "organization.permissions.manage")
    const versions = await tx.unsafe<VersionRow[]>(
      "select version from opencode_organization_permission_policy where organization_id = $1 for update",
      [organizationID],
    )
    const version = Number(versions[0]?.version)
    if (version !== input.expectedVersion) {
      throw new Error("permission_conflict", `Permission policy changed; current version is ${version}`)
    }

    await tx.unsafe(
      "delete from opencode_organization_role_permission where organization_id = $1 and role = $2",
      [organizationID, role],
    )
    const baseline = new Set(defaultPermissions(role))
    const requested = new Set(permissions)
    const now = Date.now()
    for (const item of Catalog) {
      if (!item.delegable) continue
      const before = baseline.has(item.key)
      const after = requested.has(item.key)
      if (before === after) continue
      await tx.unsafe(
        "insert into opencode_organization_role_permission (organization_id, role, permission_key, effect, updated_by, time_updated) values ($1, $2, $3, $4, $5, $6)",
        [organizationID, role, item.key, after ? "allow" : "deny", actor.actorID, now],
      )
    }
    await tx.unsafe(
      "update opencode_organization_permission_policy set version = version + 1, updated_by = $2, time_updated = $3 where organization_id = $1",
      [organizationID, actor.actorID, now],
    )
    await audit(tx, organizationID, actor.actorID, role, permissions, input.expectedVersion + 1)
  })
  return policy(actor, organizationID)
}

export async function shutdown() {
  if (database === undefined) return
  const sql = await database
  database = undefined
  await sql.end({ timeout: 5 })
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

async function ensurePolicy(sql: Queryable, actorID: string, organizationID: string) {
  await sql.unsafe(
    "insert into opencode_organization_permission_policy (organization_id, version, updated_by, time_updated) values ($1, 1, $2, $3) on conflict (organization_id) do nothing",
    [organizationID, actorID, Date.now()],
  )
}

async function membership(sql: Queryable, actorID: string, organizationID: string) {
  const rows = await sql.unsafe<MemberRow[]>(
    "select m.role, m.status from opencode_organization_member m join opencode_organization o on o.id = m.organization_id where m.organization_id = $1 and m.actor_id = $2 and o.status = 'active' limit 1",
    [organizationID, actorID],
  )
  const value = rows[0]
  if (value === undefined || value.status !== "active") {
    throw new Error("permission_denied", "Active organization membership required")
  }
  return value
}

async function effectivePermissions(sql: Queryable, organizationID: string, role: Role) {
  if (role === "owner") return [...defaultPermissions(role)]
  const result = new Set(defaultPermissions(role))
  const rows = await sql.unsafe<OverrideRow[]>(
    "select permission_key, effect from opencode_organization_role_permission where organization_id = $1 and role = $2",
    [organizationID, role],
  )
  for (const row of rows) {
    if (!isPermissionKey(row.permission_key)) continue
    if (row.effect === "allow") result.add(row.permission_key)
    else result.delete(row.permission_key)
  }
  return Catalog.map((item) => item.key).filter((permission) => result.has(permission))
}

async function snapshot(sql: Queryable, actor: ActorContext, organizationID: string): Promise<Policy> {
  const member = await membership(sql, actor.actorID, organizationID)
  const versions = await sql.unsafe<VersionRow[]>(
    "select version from opencode_organization_permission_policy where organization_id = $1 limit 1",
    [organizationID],
  )
  const roles: Role[] = ["owner", "admin", "member", "viewer", "billing_admin"]
  const policies = await Promise.all(
    roles.map(async (role) => ({
      role,
      permissions: await effectivePermissions(sql, organizationID, role),
      mutable: role !== "owner",
    })),
  )
  return {
    organizationID,
    version: Number(versions[0]?.version ?? 1),
    catalog: Catalog,
    roles: policies,
    actor: {
      actorID: actor.actorID,
      role: member.role,
      permissions: await effectivePermissions(sql, organizationID, member.role),
    },
  }
}

async function audit(
  tx: TransactionSql<Record<string, unknown>>,
  organizationID: string,
  actorID: string,
  role: EditableRole,
  permissions: readonly PermissionKey[],
  version: number,
) {
  await tx.unsafe(
    "insert into opencode_organization_audit (id, organization_id, actor_id, action, target_type, target_id, outcome, metadata, time_created) values ($1, $2, $3, 'organization.permission_policy.update', 'role', $4, 'allowed', $5::jsonb, $6)",
    [randomUUID(), organizationID, actorID, role, JSON.stringify({ permissions, version }), Date.now()],
  )
}

function isPermissionKey(value: string): value is PermissionKey {
  return Catalog.some((item) => item.key === value)
}

function isEditableRole(value: string): value is EditableRole {
  return value === "admin" || value === "member" || value === "viewer" || value === "billing_admin"
}

type Queryable = { readonly unsafe: Sql["unsafe"] }
type MemberRow = { readonly role: Role; readonly status: "invited" | "active" | "suspended" }
type OverrideRow = { readonly permission_key: string; readonly effect: "allow" | "deny" }
type VersionRow = { readonly version: string | number }
