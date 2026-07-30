import { runSaasReleaseGateSmoke } from "../src/database/postgres/saas-release-gate-smoke"

console.log(JSON.stringify(runSaasReleaseGateSmoke(), undefined, 2))
