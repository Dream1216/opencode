import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { runWorkerQueueIdentitySmoke } from "../src/database/postgres/worker-queue-identity-smoke"

if (process.env.OPENCODE_POSTGRES_WORKER_QUEUE_IDENTITY_SMOKE !== "1") {
  console.log(
    JSON.stringify(
      { status: "skipped", reason: "set OPENCODE_POSTGRES_WORKER_QUEUE_IDENTITY_SMOKE=1" },
      undefined,
      2,
    ),
  )
  process.exit(0)
}
const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 4) })
try {
  console.log(JSON.stringify(await runWorkerQueueIdentitySmoke(sql, { url: config.url }), undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
