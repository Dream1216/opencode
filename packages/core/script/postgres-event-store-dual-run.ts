import { configFromEnv, makeClient } from "../src/database/postgres"
import { runEventStoreDualRun } from "../src/database/postgres/event-store-dual-run"

if (process.env.OPENCODE_POSTGRES_EVENT_STORE_DUAL_RUN !== "1") {
  console.log(
    "postgres-event-store-dual-run skipped; set OPENCODE_POSTGRES_EVENT_STORE_DUAL_RUN=1 and OPENCODE_DATABASE_URL to run against real PostgreSQL",
  )
  process.exit(0)
}

const config = configFromEnv()
if (config === undefined) {
  console.error("OPENCODE_DATABASE_URL is required when OPENCODE_POSTGRES_EVENT_STORE_DUAL_RUN=1.")
  process.exit(1)
}

const sql = makeClient(config)
try {
  const result = await runEventStoreDualRun(sql)
  console.log(JSON.stringify(result, undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
