export type ExportPlan = {
  readonly source: "sqlite"
  readonly target: "postgres"
  readonly mode: "read-only-export"
  readonly requiresTenantMapping: true
  readonly idempotencyKeys: readonly string[]
}

export type ImportBatch = {
  readonly tenantID: string
  readonly actorID: string
  readonly sessions: number
  readonly events: number
}

export function exportPlan(): ExportPlan {
  return {
    source: "sqlite",
    target: "postgres",
    mode: "read-only-export",
    requiresTenantMapping: true,
    idempotencyKeys: ["tenantID", "session.id", "event.id", "event.aggregate_id:event.seq"],
  }
}

export function validateImportBatch(batch: ImportBatch) {
  const issues: string[] = []
  if (batch.tenantID.trim() === "") issues.push("tenantID is required")
  if (batch.actorID.trim() === "") issues.push("actorID is required")
  if (!Number.isSafeInteger(batch.sessions) || batch.sessions < 0) issues.push("sessions must be a non-negative safe integer")
  if (!Number.isSafeInteger(batch.events) || batch.events < 0) issues.push("events must be a non-negative safe integer")
  return issues
}
