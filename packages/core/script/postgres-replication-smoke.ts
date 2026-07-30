import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { runReplicationSmoke } from "../src/database/postgres/replication-smoke"

if (process.env.OPENCODE_POSTGRES_REPLICATION_SMOKE !== "1") {
  console.log(JSON.stringify({ status: "skipped", reason: "set OPENCODE_POSTGRES_REPLICATION_SMOKE=1" }, undefined, 2))
  process.exit(0)
}
const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 2) })
try {
  console.log(JSON.stringify(await runReplicationSmoke(sql), undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
