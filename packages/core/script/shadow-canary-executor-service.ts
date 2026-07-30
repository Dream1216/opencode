import {
  shadowCanaryExecutorConfigFromEnv,
  startShadowCanaryExecutor,
} from "../src/session/runner/shadow-canary-executor"

const service = await startShadowCanaryExecutor(shadowCanaryExecutorConfigFromEnv())
console.log(JSON.stringify({ status: "listening", url: service.url, snapshot: service.snapshot() }))
await new Promise<void>((resolve) => {
  process.once("SIGINT", resolve)
  process.once("SIGTERM", resolve)
})
await service.close()
