import { applyMigrations, assertRlsReady, configFromEnv, makeClient } from "../src/database/postgres"

const config = configFromEnv()
if (config === undefined) {
  console.error("OPENCODE_DATABASE_URL is required to run PostgreSQL RLS migrations.")
  process.exit(1)
}

const sql = makeClient(config)
try {
  const result = await applyMigrations(sql)
  await assertRlsReady(sql)
  console.log(
    JSON.stringify(
      {
        status: "ok",
        applied: result.applied,
        skipped: result.skipped,
      },
      undefined,
      2,
    ),
  )
} finally {
  await sql.end({ timeout: 5 })
}
