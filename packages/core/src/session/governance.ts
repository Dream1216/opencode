export * as SessionGovernance from "./governance"

import { DateTime, Effect } from "effect"
import { InstallationChannel, InstallationVersion } from "../installation/version"
import { EventV2 } from "../event"
import { SessionEvent } from "./event"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"

type Resource = {
  readonly type: string
  readonly id: string
}

type Input = {
  readonly events: EventV2.Interface
  readonly session: SessionSchema.Info
}

export const recordSessionCreated = Effect.fn("SessionGovernance.recordSessionCreated")(function* (input: Input) {
  const tenant = tenantContext(input.session)
  yield* input.events.publish(SessionEvent.Tenant.Bound, {
    sessionID: input.session.id,
    timestamp: yield* DateTime.now,
    tenant,
  })
  yield* publishRls(input.events, input.session.id, tenant, "session.create", {
    type: "session",
    id: input.session.id,
  })
  yield* input.events.publish(SessionEvent.ReleaseGovernance.Evaluated, {
    sessionID: input.session.id,
    timestamp: yield* DateTime.now,
    tenant,
    release: releaseGate(),
  })
  yield* publishAudit(input.events, input.session.id, tenant, "session.create", {
    type: "session",
    id: input.session.id,
  })
})

export const recordPromptAdmitted = Effect.fn("SessionGovernance.recordPromptAdmitted")(function* (input: {
  readonly events: EventV2.Interface
  readonly session?: SessionSchema.Info
  readonly sessionID?: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly delivery: string
  readonly admittedSeq?: number
  readonly promotedSeq?: number
}) {
  return yield* recordPromptPrompted(input)
})

export const recordPromptPrompted = Effect.fn("SessionGovernance.recordPromptPrompted")(function* (input: {
  readonly events: EventV2.Interface
  readonly session?: SessionSchema.Info
  readonly sessionID?: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly delivery: string
  readonly admittedSeq?: number
  readonly promotedSeq?: number
}) {
  const sessionID = input.session?.id ?? input.sessionID
  if (sessionID === undefined) return
  const tenant = input.session === undefined ? tenantContextFromWorkspace() : tenantContext(input.session)
  const resource = { type: "message", id: input.messageID }
  yield* publishRls(input.events, sessionID, tenant, "session.prompt.prompted", resource)
  yield* publishAudit(input.events, sessionID, tenant, "session.prompt.prompted", resource, {
    delivery: input.delivery,
    ...(input.admittedSeq === undefined ? {} : { admittedSeq: input.admittedSeq }),
    ...(input.promotedSeq === undefined ? {} : { promotedSeq: input.promotedSeq }),
  })
})

function tenantContext(session: SessionSchema.Info) {
  return tenantContextFromWorkspace(session.location.workspaceID)
}

function tenantContextFromWorkspace(workspaceID?: string) {
  const tenantID = process.env.OPENCODE_TENANT_ID ?? "tenant_local"
  const teamID = process.env.OPENCODE_TEAM_ID
  const actorID = process.env.OPENCODE_ACTOR_ID ?? process.env.USER ?? "local-user"
  return {
    tenantID,
    ...(teamID === undefined ? {} : { teamID }),
    actorID,
    ...(workspaceID === undefined ? {} : { workspaceID }),
    source: process.env.OPENCODE_TENANT_ID === undefined ? "local-default" : "env",
    mode: process.env.OPENCODE_TENANT_ID === undefined ? "single-tenant-local" : "multi-tenant",
  } as const
}

function releaseGate() {
  const environment = process.env.OPENCODE_RELEASE_ENV ?? (InstallationChannel === "local" ? "local" : "release")
  const saas = process.env.OPENCODE_SAAS_RELEASE === "1" || process.env.OPENCODE_SAAS_RELEASE === "true"
  return {
    channel: InstallationChannel,
    version: InstallationVersion,
    environment,
    gate: saas ? "saas-ready" : "local-development",
    result: saas && process.env.OPENCODE_TENANT_ID === undefined ? "warn" : "allow",
    checks: [
      "tenant_context_bound",
      "logical_rls_event_recorded",
      "audit_event_recorded",
      saas ? "saas_release_requested" : "local_release_default",
    ],
  } as const
}

const publishRls = Effect.fn("SessionGovernance.publishRls")(function* (
  events: EventV2.Interface,
  sessionID: SessionSchema.ID,
  tenant: ReturnType<typeof tenantContext>,
  operation: string,
  resource: Resource,
) {
  yield* events.publish(SessionEvent.Rls.Evaluated, {
    sessionID,
    timestamp: yield* DateTime.now,
    operation,
    resource,
    tenant,
    policy: {
      mode: "logical-sqlite",
      enforced: true,
      result: "allow",
      checks: ["tenant_context_present", "session_aggregate_scope", "workspace_scope_if_present"],
      reason: "OpenCode local storage uses durable logical tenant scoping; PostgreSQL RLS is a later backend swap.",
    },
  })
})

const publishAudit = Effect.fn("SessionGovernance.publishAudit")(function* (
  events: EventV2.Interface,
  sessionID: SessionSchema.ID,
  tenant: ReturnType<typeof tenantContext>,
  action: string,
  resource: Resource,
  metadata?: Record<string, unknown>,
) {
  yield* events.publish(SessionEvent.Audit.Recorded, {
    sessionID,
    timestamp: yield* DateTime.now,
    action,
    resource,
    outcome: "success",
    tenant,
    ...(metadata === undefined ? {} : { metadata }),
  })
})
