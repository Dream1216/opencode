import { makeClient } from "../src/database/postgres/client"
import { runWorkerQueueLiveIntegration } from "../src/database/postgres/worker-queue-live-integration"

if (process.env.OPENCODE_POSTGRES_WORKER_QUEUE_LIVE_INTEGRATION !== "1") {
  throw new Error("Set OPENCODE_POSTGRES_WORKER_QUEUE_LIVE_INTEGRATION=1 to run the P4.41 live integration gate")
}

const url = required(process.env.OPENCODE_DATABASE_URL, "OPENCODE_DATABASE_URL")
const sql = makeClient({ url, max: 4 })
try {
  console.log(JSON.stringify(await runWorkerQueueLiveIntegration(sql, { url }), undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
