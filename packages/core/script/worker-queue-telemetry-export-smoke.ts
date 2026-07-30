import { runWorkerQueueTelemetryExportSmoke } from "../src/observability/worker-queue-telemetry-export-smoke"

if (process.env.OPENCODE_WORKER_QUEUE_TELEMETRY_EXPORT_SMOKE !== "1") {
  console.log(
    JSON.stringify(
      { status: "skipped", reason: "set OPENCODE_WORKER_QUEUE_TELEMETRY_EXPORT_SMOKE=1" },
      undefined,
      2,
    ),
  )
  process.exit(0)
}
console.log(JSON.stringify(await runWorkerQueueTelemetryExportSmoke(), undefined, 2))
