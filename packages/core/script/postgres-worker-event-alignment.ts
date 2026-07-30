import { configFromEnv, makeClient } from "../src/database/postgres"
import { runWorkerEventAlignment } from "../src/database/postgres/worker-event-alignment"

if (process.env.OPENCODE_POSTGRES_WORKER_EVENT_ALIGNMENT !== "1") {
  console.log(
    "postgres-worker-event-alignment skipped; set OPENCODE_POSTGRES_WORKER_EVENT_ALIGNMENT=1 and OPENCODE_DATABASE_URL to run against real PostgreSQL",
  )
  process.exit(0)
}

const config = configFromEnv()
if (config === undefined) {
  console.error("OPENCODE_DATABASE_URL is required when OPENCODE_POSTGRES_WORKER_EVENT_ALIGNMENT=1.")
  process.exit(1)
}

const sql = makeClient(config)
try {
  const result = await runWorkerEventAlignment(sql)
  console.log(JSON.stringify(result, undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
