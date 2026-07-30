import { runWorkerQueueCloudContractSmoke } from "../src/database/postgres/worker-queue-cloud-contract-smoke"

console.log(JSON.stringify(await runWorkerQueueCloudContractSmoke(), undefined, 2))
