import type { Sql } from "postgres"
import { migrations, tenantScopedTables } from "./schema"

export type MigrationResult = {
  readonly applied: readonly string[]
  readonly skipped: readonly string[]
}

const migrationTable = "opencode_pg_migration"

export async function applyMigrations(sql: Sql): Promise<MigrationResult> {
  const applied: string[] = []
  const skipped: string[] = []
  await sql.begin(async (tx) => {
    await tx`select pg_advisory_xact_lock(hashtext('opencode.pg.migration'), hashtext(current_schema()))`
    await tx.unsafe(
      `create table if not exists ${migrationTable} (id text primary key, time_completed bigint not null)`,
    )
    for (const migration of migrations) {
      const existing = await tx`select id from opencode_pg_migration where id = ${migration.id}`
      if (existing.length > 0) {
        skipped.push(migration.id)
        continue
      }
      for (const statement of migration.statements) {
        await tx.unsafe(statement)
      }
      await tx`insert into opencode_pg_migration (id, time_completed) values (${migration.id}, ${Date.now()})`
      applied.push(migration.id)
    }
  })
  return { applied, skipped }
}

export async function assertRlsReady(sql: Sql) {
  const quotedTables = tenantScopedTables.map((table) => `'${table.replaceAll("'", "''")}'`).join(", ")
  const rows = await sql.unsafe<{
    table_name: string
    relrowsecurity: boolean
    relforcerowsecurity: boolean
    has_policy: boolean
  }[]>(`
    select
      c.relname as table_name,
      c.relrowsecurity,
      c.relforcerowsecurity,
      exists (
        select 1
        from pg_policies p
        where p.schemaname = n.nspname
          and p.tablename = c.relname
          and p.policyname = c.relname || '_tenant_isolation'
      ) as has_policy
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = current_schema()
      and c.relkind = 'r'
      and c.relname in (${quotedTables})
  `)
  const found = new Map(rows.map((row) => [row.table_name, row]))
  const issues: string[] = []
  for (const table of tenantScopedTables) {
    const row = found.get(table)
    if (row === undefined) {
      issues.push(`${table}: missing table`)
      continue
    }
    if (!row.relrowsecurity) issues.push(`${table}: RLS is not enabled`)
    if (!row.relforcerowsecurity) issues.push(`${table}: RLS is not forced`)
    if (!row.has_policy) issues.push(`${table}: tenant isolation policy is missing`)
  }
  if (issues.length > 0) throw new Error(`PostgreSQL RLS is not ready: ${issues.join("; ")}`)
}
