import { runAlphaStartupSmoke } from "../src/database/postgres/alpha-startup-smoke"

console.log(JSON.stringify(await runAlphaStartupSmoke(), undefined, 2))
