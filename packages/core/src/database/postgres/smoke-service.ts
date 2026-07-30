import type { Sql } from "postgres"
import { applyMigrations, assertRlsReady } from "./migration"

export type SmokeTenant = {
  readonly tenantID: string
  readonly actorID: string
}

export type SmokeInput = {
  readonly tenantA?: SmokeTenant
  readonly tenantB?: SmokeTenant
  readonly prefix?: string
}

export type SmokeResult = {
  readonly status: "ok"
  readonly prefix: string
  readonly checks: readonly string[]
}

type Queryable = {
  readonly unsafe: Sql["unsafe"]
}

const defaultTenantA = { tenantID: "tenant_rls_smoke_a", actorID: "actor_rls_smoke_a" } satisfies SmokeTenant
const defaultTenantB = { tenantID: "tenant_rls_smoke_b", actorID: "actor_rls_smoke_b" } satisfies SmokeTenant

export async function runRlsSmoke(sql: Sql, input: SmokeInput = {}): Promise<SmokeResult> {
  const tenantA = input.tenantA ?? defaultTenantA
  const tenantB = input.tenantB ?? defaultTenantB
  const prefix = input.prefix ?? `pg_rls_smoke_${Date.now()}_${Math.random().toString(36).slice(2)}`
  const checks: string[] = []

  await applyMigrations(sql)
  await assertRlsReady(sql)
  checks.push("migrations-applied")
  checks.push("rls-ready")

  await assertRuntimeRoleSubjectToRls(sql)
  checks.push("runtime-role-subject-to-rls")

  await seedTenant(sql, tenantA, prefix)
  await seedTenant(sql, tenantB, prefix)
  checks.push("tenant-a-seeded")
  checks.push("tenant-b-seeded")

  await assertTenantCanReadOwnRows(sql, tenantA, prefix)
  await assertTenantCanReadOwnRows(sql, tenantB, prefix)
  checks.push("own-tenant-reads-allowed")

  await assertTenantCannotReadOtherRows(sql, tenantA, tenantB, prefix)
  await assertTenantCannotReadOtherRows(sql, tenantB, tenantA, prefix)
  checks.push("cross-tenant-reads-denied")

  await assertTenantCannotAppendOtherAggregate(sql, tenantA, tenantB, prefix)
  await assertTenantCannotPromoteOtherInput(sql, tenantA, tenantB, prefix)
  checks.push("cross-tenant-writes-denied")

  await assertMissingTenantFailsClosed(sql, tenantA, prefix)
  checks.push("missing-tenant-fails-closed")

  await cleanupTenant(sql, tenantA, prefix)
  await cleanupTenant(sql, tenantB, prefix)
  await sql`delete from tenant where id in (${tenantA.tenantID}, ${tenantB.tenantID})`
  checks.push("cleanup-completed")

  return { status: "ok", prefix, checks }
}

export async function assertRuntimeRoleSubjectToRls(sql: Sql) {
  const rows = await sql<{ rolsuper: boolean; rolbypassrls: boolean }[]>`
    select rolsuper, rolbypassrls
    from pg_roles
    where rolname = current_user
  `
  const role = rows[0]
  if (role === undefined) throw new Error("Unable to inspect current PostgreSQL role")
  if (role.rolsuper) throw new Error("Current PostgreSQL role is superuser; RLS negative tests would be invalid")
  if (role.rolbypassrls) throw new Error("Current PostgreSQL role has BYPASSRLS; RLS negative tests would be invalid")
}

async function seedTenant(sql: Sql, tenant: SmokeTenant, prefix: string) {
  await sql`insert into tenant (id, name, time_created, time_updated) values (${tenant.tenantID}, ${tenant.tenantID}, ${Date.now()}, ${Date.now()}) on conflict (id) do nothing`
  await sql.begin(async (tx) => {
    await setTenant(tx, tenant)
    await tx.unsafe(
      `insert into project (id, tenant_id, worktree, vcs, sandboxes)
       values ($1, $2, $3, $4, '[]'::jsonb)
       on conflict (id) do nothing`,
      [projectID(tenant, prefix), tenant.tenantID, `/tmp/${prefix}/${tenant.tenantID}`, "git"],
    )
    await tx.unsafe(
      `insert into session (id, tenant_id, actor_id, project_id, slug, directory, path, title, version, time_created, time_updated)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       on conflict (id) do nothing`,
      [
        sessionID(tenant, prefix),
        tenant.tenantID,
        tenant.actorID,
        projectID(tenant, prefix),
        `${prefix}-${tenant.tenantID}`,
        `/tmp/${prefix}/${tenant.tenantID}`,
        ".",
        `RLS smoke ${tenant.tenantID}`,
        "p4.11",
        Date.now(),
        Date.now(),
      ],
    )
    await tx.unsafe(
      `insert into event_sequence (tenant_id, aggregate_id, seq, owner_id)
       values ($1, $2, $3, $4)
       on conflict (tenant_id, aggregate_id) do nothing`,
      [tenant.tenantID, sessionID(tenant, prefix), 0, tenant.actorID],
    )
    await tx.unsafe(
      `insert into event (id, tenant_id, actor_id, aggregate_id, seq, type, data)
       values ($1, $2, $3, $4, 0, 'session.next.tenant.bound', '{}'::jsonb)
       on conflict (id) do nothing`,
      [eventID(tenant, prefix), tenant.tenantID, tenant.actorID, sessionID(tenant, prefix)],
    )
    await tx.unsafe(
      `insert into session_input (id, tenant_id, session_id, prompt, delivery, admitted_seq, time_created)
       values ($1, $2, $3, '{"text":"rls smoke"}'::jsonb, 'steer', 0, $4)
       on conflict (id) do nothing`,
      [inputID(tenant, prefix), tenant.tenantID, sessionID(tenant, prefix), Date.now()],
    )
    await tx.unsafe(
      `insert into audit_event (id, tenant_id, actor_id, action, resource_type, resource_id, outcome, metadata, time_created)
       values ($1, $2, $3, 'rls.smoke.seed', 'session', $4, 'success', '{}'::jsonb, $5)
       on conflict (id) do nothing`,
      [auditID(tenant, prefix), tenant.tenantID, tenant.actorID, sessionID(tenant, prefix), Date.now()],
    )
  })
}

async function assertTenantCanReadOwnRows(sql: Sql, tenant: SmokeTenant, prefix: string) {
  await sql.begin(async (tx) => {
    await setTenant(tx, tenant)
    const sessions = await tx.unsafe<{ id: string }[]>(`select id from session where id = $1`, [
      sessionID(tenant, prefix),
    ])
    if (sessions.length !== 1) throw new Error(`${tenant.tenantID} cannot read its own session`)
    const events = await tx.unsafe<{ id: string }[]>(`select id from event where aggregate_id = $1`, [
      sessionID(tenant, prefix),
    ])
    if (events.length !== 1) throw new Error(`${tenant.tenantID} cannot read its own event`)
  })
}

async function assertTenantCannotReadOtherRows(sql: Sql, reader: SmokeTenant, owner: SmokeTenant, prefix: string) {
  await sql.begin(async (tx) => {
    await setTenant(tx, reader)
    const sessions = await tx.unsafe<{ id: string }[]>(`select id from session where id = $1`, [
      sessionID(owner, prefix),
    ])
    if (sessions.length !== 0) throw new Error(`${reader.tenantID} read ${owner.tenantID} session`)
    const events = await tx.unsafe<{ id: string }[]>(`select id from event where aggregate_id = $1`, [
      sessionID(owner, prefix),
    ])
    if (events.length !== 0) throw new Error(`${reader.tenantID} read ${owner.tenantID} event`)
  })
}

async function assertTenantCannotAppendOtherAggregate(
  sql: Sql,
  writer: SmokeTenant,
  owner: SmokeTenant,
  prefix: string,
) {
  await expectRejected(
    sql.begin(async (tx) => {
      await setTenant(tx, writer)
      await tx.unsafe(
        `insert into event (id, tenant_id, actor_id, aggregate_id, seq, type, data)
         values ($1, $2, $3, $4, 99, 'session.next.audit.recorded', '{}'::jsonb)`,
        [`${eventID(owner, prefix)}_blocked_${writer.tenantID}`, owner.tenantID, writer.actorID, sessionID(owner, prefix)],
      )
    }),
    `${writer.tenantID} appended to ${owner.tenantID} aggregate`,
  )
}

async function assertTenantCannotPromoteOtherInput(sql: Sql, writer: SmokeTenant, owner: SmokeTenant, prefix: string) {
  await sql.begin(async (tx) => {
    await setTenant(tx, writer)
    const rows = await tx.unsafe<{ id: string }[]>(
      `update session_input set promoted_seq = 99 where id = $1 returning id`,
      [inputID(owner, prefix)],
    )
    if (rows.length !== 0) throw new Error(`${writer.tenantID} promoted ${owner.tenantID} input`)
  })
}

async function assertMissingTenantFailsClosed(sql: Sql, tenant: SmokeTenant, prefix: string) {
  const rows = await sql.unsafe<{ id: string }[]>(`select id from session where id = $1`, [sessionID(tenant, prefix)])
  if (rows.length !== 0) throw new Error("Unset tenant context read tenant-scoped session")
  await expectRejected(
    sql.unsafe(
      `insert into event_sequence (tenant_id, aggregate_id, seq, owner_id)
       values ($1, $2, 0, $3)`,
      [tenant.tenantID, `${sessionID(tenant, prefix)}_missing_tenant`, tenant.actorID],
    ),
    "Unset tenant context inserted tenant-scoped event sequence",
  )
}

async function cleanupTenant(sql: Sql, tenant: SmokeTenant, prefix: string) {
  await sql.begin(async (tx) => {
    await setTenant(tx, tenant)
    await tx.unsafe(`delete from audit_event where id = $1`, [auditID(tenant, prefix)])
    await tx.unsafe(`delete from session_input where id = $1`, [inputID(tenant, prefix)])
    await tx.unsafe(`delete from event where aggregate_id = $1`, [sessionID(tenant, prefix)])
    await tx.unsafe(`delete from event_sequence where aggregate_id = $1`, [sessionID(tenant, prefix)])
    await tx.unsafe(`delete from session where id = $1`, [sessionID(tenant, prefix)])
    await tx.unsafe(`delete from project where id = $1`, [projectID(tenant, prefix)])
  })
}

async function setTenant(tx: Queryable, tenant: SmokeTenant) {
  await tx.unsafe(`select set_config('opencode.tenant_id', $1, true)`, [tenant.tenantID])
  await tx.unsafe(`select set_config('opencode.actor_id', $1, true)`, [tenant.actorID])
  await tx.unsafe(`select set_config('opencode.team_id', $1, true)`, [""])
}

async function expectRejected(input: Promise<unknown>, message: string) {
  try {
    await input
  } catch {
    return
  }
  throw new Error(message)
}

const projectID = (tenant: SmokeTenant, prefix: string) => `${prefix}_project_${tenant.tenantID}`
const sessionID = (tenant: SmokeTenant, prefix: string) => `${prefix}_session_${tenant.tenantID}`
const eventID = (tenant: SmokeTenant, prefix: string) => `${prefix}_event_${tenant.tenantID}`
const inputID = (tenant: SmokeTenant, prefix: string) => `${prefix}_input_${tenant.tenantID}`
const auditID = (tenant: SmokeTenant, prefix: string) => `${prefix}_audit_${tenant.tenantID}`
