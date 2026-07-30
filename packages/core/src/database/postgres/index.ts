export * as PostgresDatabase from "./index"
export * from "./alpha-startup"
export * from "./client"
export * from "./event-store"
export * from "./migration"
export * from "./release-proof"
export * from "./smoke-service"

import { DatabaseBackend } from "../backend"

export function unavailableMessage(config: DatabaseBackend.PostgresConfig) {
  const missing = startupConfigIssues(config)
  return [
    "PostgreSQL backend is not enabled in this build.",
    "Use OPENCODE_DATABASE_BACKEND=postgres-alpha only for the SQLite-primary PostgreSQL alpha sidecar.",
    "SQLite remains the default backend; unset OPENCODE_DATABASE_BACKEND or set it to sqlite to continue.",
    missing.length === 0 ? undefined : `Configuration issues: ${missing.join(", ")}.`,
  ]
    .filter((line): line is string => line !== undefined)
    .join(" ")
}

export function startupConfigIssues(config: DatabaseBackend.PostgresConfig) {
  const issues: string[] = []
  if (config.url === undefined || config.url.trim() === "") issues.push("OPENCODE_DATABASE_URL is required")
  if (config.requireRls !== true) issues.push("OPENCODE_POSTGRES_REQUIRE_RLS must not disable RLS for SaaS")
  if (config.tenantID === undefined || config.tenantID.trim() === "") issues.push("OPENCODE_TENANT_ID is required")
  if (config.actorID === undefined || config.actorID.trim() === "") issues.push("OPENCODE_ACTOR_ID is recommended")
  return issues
}
