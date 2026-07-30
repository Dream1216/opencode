import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { runWorkerJobChaos } from "../src/database/postgres/worker-job-chaos"

if (process.env.OPENCODE_POSTGRES_WORKER_JOB_CHAOS !== "1") {
  console.log(JSON.stringify({ status: "skipped", reason: "set OPENCODE_POSTGRES_WORKER_JOB_CHAOS=1" }, undefined, 2))
  process.exit(0)
}
const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 4) })
try {
  console.log(JSON.stringify(await runWorkerJobChaos(sql, { url: config.url }), undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}
