export * as ExecutionResourceBinding from "./execution-resource-binding"

import { randomUUID } from "node:crypto"
import type { Sql, TransactionSql } from "postgres"
import type { ActorContext } from "../identity"
import { makeClient } from "../database/postgres/client"
import { applyMigrations } from "../database/postgres/migration"
import { requireConfig } from "./saas-auth"

export const OrganizationHeader = "x-opencode-organization-id"

export type ResourceType = "project" | "session" | "agent_run"
export type OrganizationRole = "owner" | "admin" | "member" | "viewer" | "billing_admin"

export type Context = {
  readonly actorID: string
  readonly organizationID: string
  readonly tenantID: string
  readonly role: OrganizationRole
}

export type Binding = Context & {
  readonly resourceType: ResourceType
  readonly resourceID: string
  readonly projectID?: string
  readonly sessionID?: string
  readonly timeCreated: number
  readonly timeUpdated: number
}

export class Error extends globalThis.Error {
  constructor(
    readonly code:
      | "organization_header_required"
      | "organization_forbidden"
      | "resource_forbidden"
      | "resource_conflict",
    message: string,
  ) {
    super(message)
    this.name = "ExecutionResourceBindingError"
  }
}

const requestContexts = new WeakMap<object, Context>()
let database: ReturnType<typeof initialize> | undefined

export function organizationID(headers: Headers) {
  const value = headers.get(OrganizationHeader)?.trim()
  if (!value) throw new Error("organization_header_required", `${OrganizationHeader} is required`)
  return value
}

export function setRequestContext(source: object, context: Context) {
  requestContexts.set(source, context)
}

export function requestContext(source: object) {
  return requestContexts.get(source)
}

export function clearRequestContext(source: object) {
  requestContexts.delete(source)
}

export async function authorize(actor: ActorContext, organizationID: string): Promise<Context> {
  const sql = await load()
  const rows = await sql.unsafe<AuthorizationRow[]>(
    `select o.id, o.name, m.role
       from opencode_organization o
       join opencode_organization_member m on m.organization_id = o.id
      where o.id = $1
        and o.status = 'active'
        and m.actor_id = $2
        and m.status = 'active'
      limit 1`,
    [organizationID, actor.actorID],
  )
  const organization = rows[0]
  if (!organization) throw new Error("organization_forbidden", "Active organization membership required")

  const tenantID = `tenant_org_${organization.id}`
  const now = Date.now()
  await sql.begin(async (tx) => {
    await tx.unsafe(
      `insert into tenant (id, name, time_created, time_updated, organization_id, slug, status)
       values ($1, $2, $3, $3, $4, 'default', 'active')
       on conflict (id) do update
         set name = excluded.name,
             time_updated = excluded.time_updated,
             organization_id = excluded.organization_id,
             slug = 'default',
             status = 'active'`,
      [tenantID, organization.name, now, organization.id],
    )
    const tenants = await tx.unsafe<{ organization_id: string | null; status: string }[]>(
      "select organization_id, status from tenant where id = $1 limit 1",
      [tenantID],
    )
    if (tenants[0]?.organization_id !== organization.id || tenants[0]?.status !== "active") {
      throw new Error("organization_forbidden", "Organization tenant is not active")
    }
  })

  return {
    actorID: actor.actorID,
    organizationID: organization.id,
    tenantID,
    role: organization.role,
  }
}

export async function bindProject(
  context: Context,
  input: { readonly projectID: string; readonly worktree: string },
): Promise<Binding> {
  const sql = await load()
  return sql.begin(async (tx) => {
    await setTenant(tx, context.tenantID)
    const binding = await claim(tx, context, {
      resourceType: "project",
      resourceID: input.projectID,
      projectID: input.projectID,
    })
    await tx.unsafe(
      `insert into project (id, tenant_id, worktree, sandboxes)
       values ($1, $2, $3, '[]'::jsonb)
       on conflict (id) do nothing`,
      [input.projectID, context.tenantID, input.worktree],
    )
    const projects = await tx.unsafe<{ tenant_id: string }[]>(
      "select tenant_id from project where id = $1 limit 1",
      [input.projectID],
    )
    if (projects[0]?.tenant_id !== context.tenantID) {
      throw new Error("resource_conflict", "Project is already bound to another tenant")
    }
    await audit(tx, context, "execution.project.bind", "project", input.projectID)
    return binding
  })
}

export async function bindSession(
  context: Context,
  input: { readonly sessionID: string; readonly projectID: string },
): Promise<{ readonly session: Binding; readonly agentRun: Binding }> {
  const sql = await load()
  return sql.begin(async (tx) => {
    await setTenant(tx, context.tenantID)
    await requireBound(tx, context, "project", input.projectID)
    const session = await claim(tx, context, {
      resourceType: "session",
      resourceID: input.sessionID,
      projectID: input.projectID,
      sessionID: input.sessionID,
    })
    const agentRun = await claim(tx, context, {
      resourceType: "agent_run",
      resourceID: `run_${input.sessionID}`,
      projectID: input.projectID,
      sessionID: input.sessionID,
    })
    await audit(tx, context, "execution.session.bind", "session", input.sessionID)
    await audit(tx, context, "execution.agent_run.bind", "agent_run", agentRun.resourceID)
    return { session, agentRun }
  })
}

export async function assertResource(context: Context, resourceType: ResourceType, resourceID: string) {
  const sql = await load()
  const locator = await locate(sql, resourceType, resourceID)
  if (
    !locator ||
    locator.organization_id !== context.organizationID ||
    locator.tenant_id !== context.tenantID
  ) {
    throw new Error("resource_forbidden", "Resource does not belong to the active organization")
  }
  return sql.begin(async (tx) => {
    await setTenant(tx, context.tenantID)
    return requireBound(tx, context, resourceType, resourceID)
  })
}

export async function allowedResourceIDs(context: Context, resourceType: ResourceType) {
  const sql = await load()
  return sql.begin(async (tx) => {
    await setTenant(tx, context.tenantID)
    const rows = await tx.unsafe<{ resource_id: string }[]>(
      `select b.resource_id
         from opencode_execution_resource_binding b
         join opencode_organization_member m
           on m.organization_id = b.organization_id
          and m.actor_id = $1
          and m.status = 'active'
        where b.tenant_id = $2
          and b.organization_id = $3
          and b.resource_type = $4`,
      [context.actorID, context.tenantID, context.organizationID, resourceType],
    )
    return rows.map((row) => row.resource_id)
  })
}

export async function lookup(resourceType: ResourceType, resourceID: string): Promise<Binding | undefined> {
  const sql = await load()
  const locator = await locate(sql, resourceType, resourceID)
  if (!locator) return
  return sql.begin(async (tx) => {
    await setTenant(tx, locator.tenant_id)
    const rows = await tx.unsafe<BindingRow[]>(
      `select resource_type, resource_id, organization_id, tenant_id, actor_id,
              project_id, session_id, time_created, time_updated
         from opencode_execution_resource_binding
        where tenant_id = $1 and resource_type = $2 and resource_id = $3
        limit 1`,
      [locator.tenant_id, resourceType, resourceID],
    )
    return rows[0] ? binding(rows[0]) : undefined
  })
}

export async function shutdown() {
  const pending = database
  database = undefined
  if (pending) await (await pending).end({ timeout: 5 })
}

async function load() {
  return await (database ??= initialize())
}

async function initialize() {
  const settings = requireConfig()
  const sql = makeClient({ url: settings.databaseURL, max: 4 })
  await applyMigrations(sql)
  return sql
}

async function claim(
  tx: TransactionSql<Record<string, unknown>>,
  context: Context,
  input: {
    readonly resourceType: ResourceType
    readonly resourceID: string
    readonly projectID?: string
    readonly sessionID?: string
  },
) {
  const now = Date.now()
  await tx.unsafe(
    `insert into opencode_execution_resource_locator (
       resource_type, resource_id, organization_id, tenant_id, time_created
     ) values ($1, $2, $3, $4, $5)
     on conflict (resource_type, resource_id) do nothing`,
    [input.resourceType, input.resourceID, context.organizationID, context.tenantID, now],
  )
  const locator = await locate(tx, input.resourceType, input.resourceID)
  if (
    !locator ||
    locator.organization_id !== context.organizationID ||
    locator.tenant_id !== context.tenantID
  ) {
    throw new Error("resource_conflict", "Resource is already bound to another tenant")
  }

  await tx.unsafe(
    `insert into opencode_execution_resource_binding (
       resource_type, resource_id, organization_id, tenant_id, actor_id,
       project_id, session_id, time_created, time_updated
     ) values ($1, $2, $3, $4, $5, $6, $7, $8, $8)
     on conflict (resource_type, resource_id) do nothing`,
    [
      input.resourceType,
      input.resourceID,
      context.organizationID,
      context.tenantID,
      context.actorID,
      input.projectID ?? null,
      input.sessionID ?? null,
      now,
    ],
  )
  return requireBound(tx, context, input.resourceType, input.resourceID)
}

async function requireBound(
  tx: TransactionSql<Record<string, unknown>>,
  context: Context,
  resourceType: ResourceType,
  resourceID: string,
) {
  const rows = await tx.unsafe<BindingRow[]>(
    `select b.resource_type, b.resource_id, b.organization_id, b.tenant_id, b.actor_id,
            b.project_id, b.session_id, b.time_created, b.time_updated
       from opencode_execution_resource_binding b
       join opencode_organization_member m
         on m.organization_id = b.organization_id
        and m.actor_id = $1
        and m.status = 'active'
      where b.tenant_id = $2
        and b.organization_id = $3
        and b.resource_type = $4
        and b.resource_id = $5
      limit 1`,
    [context.actorID, context.tenantID, context.organizationID, resourceType, resourceID],
  )
  if (!rows[0]) throw new Error("resource_forbidden", "Resource does not belong to the active organization")
  return binding(rows[0])
}

async function locate(
  sql: { readonly unsafe: Sql["unsafe"] },
  resourceType: ResourceType,
  resourceID: string,
) {
  const rows = await sql.unsafe<LocatorRow[]>(
    `select organization_id, tenant_id
       from opencode_execution_resource_locator
      where resource_type = $1 and resource_id = $2
      limit 1`,
    [resourceType, resourceID],
  )
  return rows[0]
}

async function setTenant(tx: TransactionSql<Record<string, unknown>>, tenantID: string) {
  await tx.unsafe("select set_config('opencode.tenant_id', $1, true)", [tenantID])
}

async function audit(
  tx: TransactionSql<Record<string, unknown>>,
  context: Context,
  action: string,
  targetType: string,
  targetID: string,
) {
  await tx.unsafe(
    `insert into opencode_organization_audit (
       id, organization_id, actor_id, action, target_type, target_id, outcome, metadata, time_created
     ) values ($1, $2, $3, $4, $5, $6, 'allowed', $7::jsonb, $8)`,
    [
      randomUUID(),
      context.organizationID,
      context.actorID,
      action,
      targetType,
      targetID,
      JSON.stringify({ tenantID: context.tenantID }),
      Date.now(),
    ],
  )
}

function binding(row: BindingRow): Binding {
  return {
    actorID: row.actor_id,
    organizationID: row.organization_id,
    tenantID: row.tenant_id,
    role: "member",
    resourceType: row.resource_type,
    resourceID: row.resource_id,
    projectID: row.project_id ?? undefined,
    sessionID: row.session_id ?? undefined,
    timeCreated: Number(row.time_created),
    timeUpdated: Number(row.time_updated),
  }
}

type AuthorizationRow = {
  readonly id: string
  readonly name: string
  readonly role: OrganizationRole
}

type LocatorRow = {
  readonly organization_id: string
  readonly tenant_id: string
}

type BindingRow = {
  readonly resource_type: ResourceType
  readonly resource_id: string
  readonly organization_id: string
  readonly tenant_id: string
  readonly actor_id: string
  readonly project_id: string | null
  readonly session_id: string | null
  readonly time_created: number | string
  readonly time_updated: number | string
}
