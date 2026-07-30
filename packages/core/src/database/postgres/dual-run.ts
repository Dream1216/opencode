export type Workload = {
  readonly id: string
  readonly description: string
  readonly requiredForRelease: boolean
}

export const workloads: readonly Workload[] = [
  {
    id: "prompt-admission-promotion-parity",
    description: "SQLite and PostgreSQL produce the same prompt.admitted -> prompted durable ordering.",
    requiredForRelease: true,
  },
  {
    id: "durable-worker-replay-parity",
    description: "Worker scheduled/resumed/lease/heartbeat/completed events replay gap-free on both backends.",
    requiredForRelease: true,
  },
  {
    id: "worker-event-alignment-parity",
    description: "Worker stop/resume/replay/claim lifecycle events keep the same ordered timeline on SQLite EventV2 and PostgreSQL facade.",
    requiredForRelease: true,
  },
  {
    id: "model-tool-governance-parity",
    description: "Model invocation and tool governance events keep the same normalized event contract.",
    requiredForRelease: true,
  },
  {
    id: "tenant-a-cannot-read-tenant-b-session",
    description: "Tenant A cannot list, read, or replay Tenant B sessions.",
    requiredForRelease: true,
  },
  {
    id: "tenant-a-cannot-append-tenant-b-aggregate",
    description: "Tenant A cannot append events to Tenant B aggregate IDs.",
    requiredForRelease: true,
  },
  {
    id: "missing-tenant-setting-fails-closed",
    description: "Runtime app role cannot read or write tenant scoped tables when opencode.tenant_id is unset.",
    requiredForRelease: true,
  },
  {
    id: "session-projection-shadow-parity",
    description: "SQLite EventV2 and PostgreSQL facade events produce the same dry-run session projection and write an isolated PostgreSQL shadow projection.",
    requiredForRelease: true,
  },
]

export function missingRequired(results: ReadonlyMap<string, boolean>) {
  return workloads.filter((workload) => workload.requiredForRelease && results.get(workload.id) !== true)
}
