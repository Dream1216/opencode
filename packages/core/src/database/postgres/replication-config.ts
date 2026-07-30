import { DatabaseBackend } from "../backend"

export type ReplicationContext = {
  readonly tenantID: string
  readonly actorID: string
}

export function replicationContextFromEnv(env: NodeJS.ProcessEnv = process.env): ReplicationContext | undefined {
  const config = DatabaseBackend.fromEnv(() => ":memory:", env)
  if (config.type !== "postgres-alpha" || !config.dualWriteEnabled) return undefined
  if (config.tenantID === undefined || config.tenantID.trim() === "") {
    throw new Error("PostgreSQL replication outbox requires OPENCODE_TENANT_ID")
  }
  if (config.actorID === undefined || config.actorID.trim() === "") {
    throw new Error("PostgreSQL replication outbox requires OPENCODE_ACTOR_ID")
  }
  return { tenantID: config.tenantID, actorID: config.actorID }
}
