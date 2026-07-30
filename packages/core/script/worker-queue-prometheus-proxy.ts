import {
  prometheusProxyConfigFromEnv,
  startWorkerQueuePrometheusProxy,
} from "../src/observability/worker-queue-prometheus-proxy"

const config = prometheusProxyConfigFromEnv()
if (config === undefined) throw new Error("Set OPENCODE_WORKER_QUEUE_PROMETHEUS_PROXY_ENABLED=1")
const proxy = await startWorkerQueuePrometheusProxy(config)
console.log(JSON.stringify({ status: "listening", url: proxy.url }))
await new Promise<void>((resolve) => {
  process.once("SIGINT", () => resolve())
  process.once("SIGTERM", () => resolve())
})
await proxy.close()
