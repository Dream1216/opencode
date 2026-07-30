import { configFromEnv, makeClient } from "../src/database/postgres/client"
import {
  listWorkerQueueRecoverable,
  operatorRequeueWorkerJob,
  readWorkerQueueReadiness,
} from "../src/database/postgres/worker-queue-operations"

const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const tenantID = required(process.env.OPENCODE_TENANT_ID, "OPENCODE_TENANT_ID")
const actorID = required(process.env.OPENCODE_ACTOR_ID, "OPENCODE_ACTOR_ID")
const tenant = { tenantID, actorID }
const operation = process.env.OPENCODE_WORKER_QUEUE_OPERATION ?? "readiness"
const sql = makeClient({ ...config, max: Math.max(config.max ?? 1, 2) })
try {
  switch (operation) {
    case "readiness":
      console.log(
        JSON.stringify(
          await readWorkerQueueReadiness(sql, {
            tenant,
            maxPendingAgeMs: number(process.env.OPENCODE_WORKER_QUEUE_MAX_PENDING_AGE_MS, 60_000),
          }),
          undefined,
          2,
        ),
      )
      break
    case "list":
      console.log(
        JSON.stringify(
          await listWorkerQueueRecoverable(sql, {
            tenant,
            limit: number(process.env.OPENCODE_WORKER_QUEUE_LIST_LIMIT, 100),
          }),
          undefined,
          2,
        ),
      )
      break
    case "requeue":
      console.log(
        JSON.stringify(
          await operatorRequeueWorkerJob(sql, {
            tenant,
            runID: required(process.env.OPENCODE_WORKER_JOB_RUN_ID, "OPENCODE_WORKER_JOB_RUN_ID"),
            expectedGeneration: number(
              process.env.OPENCODE_WORKER_JOB_EXPECTED_GENERATION,
              Number.NaN,
            ),
            expectedClaimToken: number(
              process.env.OPENCODE_WORKER_JOB_EXPECTED_CLAIM_TOKEN,
              Number.NaN,
            ),
            requestID: process.env.OPENCODE_WORKER_QUEUE_REQUEST_ID,
          }),
          undefined,
          2,
        ),
      )
      break
    default:
      throw new Error(`Unsupported OPENCODE_WORKER_QUEUE_OPERATION: ${operation}`)
  }
} finally {
  await sql.end({ timeout: 5 })
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}

function number(value: string | undefined, fallback: number) {
  const parsed = Number(value ?? fallback)
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`Invalid numeric worker queue setting: ${value}`)
  return parsed
}
