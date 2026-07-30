import { configFromEnv, makeClient } from "../src/database/postgres"
import { runEventV2FacadeSmoke } from "../src/database/postgres/event-v2-facade-smoke"

if (process.env.OPENCODE_POSTGRES_EVENT_V2_FACADE_SMOKE !== "1") {
  console.log(
    "postgres-event-v2-facade-smoke skipped; set OPENCODE_POSTGRES_EVENT_V2_FACADE_SMOKE=1 and OPENCODE_DATABASE_URL to run against real PostgreSQL",
  )
  process.exit(0)
}

const config = configFromEnv()
if (config === undefined) {
  console.error("OPENCODE_DATABASE_URL is required when OPENCODE_POSTGRES_EVENT_V2_FACADE_SMOKE=1.")
  process.exit(1)
}

const sql = makeClient(config)
try {
  const result = await runEventV2FacadeSmoke(sql)
  console.log(JSON.stringify(result, undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
