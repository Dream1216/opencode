import { runReleaseProofSmoke } from "../src/database/postgres/release-proof-smoke"

console.log(JSON.stringify(await runReleaseProofSmoke(), undefined, 2))
