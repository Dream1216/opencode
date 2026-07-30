import { configFromEnv, makeClient } from "../src/database/postgres/client"
import { acquireWorkerLease, commitWorkerEffect } from "../src/database/postgres/worker-lease"

const config = configFromEnv()
if (config === undefined) throw new Error("OPENCODE_DATABASE_URL is required")
const tenant = {
  tenantID: required(process.env.OPENCODE_TENANT_ID, "OPENCODE_TENANT_ID"),
  actorID: required(process.env.OPENCODE_ACTOR_ID, "OPENCODE_ACTOR_ID"),
}
const runID = required(process.env.OPENCODE_WORKER_LEASE_RUN_ID, "OPENCODE_WORKER_LEASE_RUN_ID")
const ownerID = required(process.env.OPENCODE_WORKER_LEASE_OWNER_ID, "OPENCODE_WORKER_LEASE_OWNER_ID")
const mode = process.env.OPENCODE_WORKER_LEASE_CHILD_MODE ?? "once"
const leaseMs = Number(process.env.OPENCODE_WORKER_LEASE_MS ?? 5_000)
const sql = makeClient({ ...config, max: 1 })
try {
  const acquired = await acquireWorkerLease(sql, { tenant, runID, ownerID, leaseMs })
  let effectCommitted: boolean | undefined
  const effectKey = process.env.OPENCODE_WORKER_EFFECT_KEY
  if (acquired.acquired && effectKey !== undefined) {
    const effect = await commitWorkerEffect(sql, {
      tenant,
      runID,
      ownerID,
      fencingToken: acquired.lease.fencingToken,
      effectKey,
      payload: { source: "chaos-winner" },
    })
    effectCommitted = effect.committed
  }
  console.log(
    JSON.stringify({
      acquired: acquired.acquired,
      ownerID,
      fencingToken: acquired.lease.fencingToken,
      leaseExpiresAt: acquired.lease.leaseExpiresAt,
      ...(effectCommitted === undefined ? {} : { effectCommitted }),
    }),
  )
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
