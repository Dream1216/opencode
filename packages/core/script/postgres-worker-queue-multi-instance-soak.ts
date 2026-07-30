import { makeClient } from "../src/database/postgres/client"
import { runWorkerQueueMultiInstanceSoak } from "../src/database/postgres/worker-queue-multi-instance-soak"

if (process.env.OPENCODE_POSTGRES_WORKER_QUEUE_MULTI_INSTANCE_SOAK !== "1") {
  throw new Error("Set OPENCODE_POSTGRES_WORKER_QUEUE_MULTI_INSTANCE_SOAK=1 to run the P4.41 multi-instance soak")
}

const url = required(process.env.OPENCODE_DATABASE_URL, "OPENCODE_DATABASE_URL")
const sql = makeClient({ url, max: 8 })
try {
  console.log(JSON.stringify(await runWorkerQueueMultiInstanceSoak(sql, { url }), undefined, 2))
} finally {
  await sql.end({ timeout: 5 })
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
