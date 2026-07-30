import { runReplicationWorkerOnce } from "../src/database/postgres/replication-worker"

const continuous = process.env.OPENCODE_POSTGRES_REPLICATION_MODE === "continuous"
const intervalMs = Number(process.env.OPENCODE_POSTGRES_REPLICATION_INTERVAL_MS ?? 1_000)
let stopping = false
process.on("SIGINT", () => {
  stopping = true
})
process.on("SIGTERM", () => {
  stopping = true
})

do {
  console.log(JSON.stringify(await runReplicationWorkerOnce(), undefined, 2))
  if (continuous && !stopping) await Bun.sleep(intervalMs)
} while (continuous && !stopping)
