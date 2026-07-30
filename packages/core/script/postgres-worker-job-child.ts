import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { claimNextWorkerJob } from "../src/database/postgres/worker-job"

const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const tenant = {
  tenantID: required(process.env.OPENCODE_TENANT_ID, "OPENCODE_TENANT_ID"),
  actorID: required(process.env.OPENCODE_ACTOR_ID, "OPENCODE_ACTOR_ID"),
}
const runID = required(process.env.OPENCODE_WORKER_JOB_RUN_ID, "OPENCODE_WORKER_JOB_RUN_ID")
const ownerID = required(process.env.OPENCODE_WORKER_JOB_OWNER_ID, "OPENCODE_WORKER_JOB_OWNER_ID")
const mode = process.env.OPENCODE_WORKER_JOB_CHILD_MODE ?? "once"
const claimMs = Number(process.env.OPENCODE_WORKER_JOB_CLAIM_MS ?? 5_000)
const sql = makeClient({ ...config, max: 1 })
try {
  const claim = await claimNextWorkerJob(sql, { tenant, runID, ownerID, claimMs })
  console.log(JSON.stringify(claim === undefined ? { claimed: false } : { claimed: true, ...claim }))
  if (mode === "hold") {
    setInterval(() => {}, 1_000)
    await new Promise(() => {})
  }
} finally {
  if (mode !== "hold") await sql.end({ timeout: 5 })
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
