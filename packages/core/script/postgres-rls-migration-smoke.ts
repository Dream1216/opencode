import { configFromEnv, makeClient, applyMigrations, assertRlsReady } from "../src/database/postgres"
import { migrations, validateDraft } from "../src/database/postgres/schema"

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

assert(configFromEnv({}) === undefined, "postgres config must be absent without OPENCODE_DATABASE_URL")
assert(validateDraft().length === 0, "postgres schema draft must include tenant and RLS primitives")
assert(migrations.some((migration) => migration.id === "p4_4_005_rls"), "postgres migrations must include RLS stage")

if (process.env.OPENCODE_POSTGRES_RLS_SMOKE === "1") {
  const config = configFromEnv()
  assert(config !== undefined, "OPENCODE_DATABASE_URL is required when OPENCODE_POSTGRES_RLS_SMOKE=1")
  const sql = makeClient(config)
  try {
    const result = await applyMigrations(sql)
    await assertRlsReady(sql)
    console.log(JSON.stringify({ status: "ok", mode: "real-postgres", result }, undefined, 2))
  } finally {
    await sql.end({ timeout: 5 })
  }
} else {
  console.log("postgres-rls-migration smoke passed without touching a real database")
}
