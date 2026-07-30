import { configFromEnv, makeClient, runRlsSmoke } from "../src/database/postgres"

if (process.env.OPENCODE_POSTGRES_RLS_SMOKE !== "1") {
  console.log("postgres-rls-smoke-service skipped; set OPENCODE_POSTGRES_RLS_SMOKE=1 and OPENCODE_DATABASE_URL to run against real PostgreSQL")
  process.exit(0)
}

const config = configFromEnv()
if (config === undefined) {
  console.error("OPENCODE_DATABASE_URL is required when OPENCODE_POSTGRES_RLS_SMOKE=1.")
  process.exit(1)
}

const sql = makeClient(config)
try {
  const result = await runRlsSmoke(sql)
  console.log(JSON.stringify(result, undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
