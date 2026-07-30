import { Context, Effect, Layer } from "effect"
import { createHash, randomUUID, timingSafeEqual } from "node:crypto"
import type { Sql, TransactionSql } from "postgres"
import { configFromEnv, makeClient, setTenantContext, type TenantContext } from "./client"
import { assertRlsReady } from "./migration"
import {
  listWorkerQueueRecoverable,
  readWorkerQueueReadiness,
  type WorkerQueueReadiness,
} from "./worker-queue-operations"
import {
  requeueWorkerJobInTransaction,
  WorkerJobRequeueRejectedError,
  type WorkerJob,
} from "./worker-job"
import {
  observeWorkerQueue,
  recordWorkerQueueOperatorAction,
  renderWorkerQueuePrometheus,
} from "./worker-queue-telemetry"
import { makeGlobalNode } from "../../effect/app-node"
import {
  identityAdapterFromEnv,
  type WorkerQueueIdentityAdapter,
} from "./worker-queue-identity"
import {
  signRequest,
  type WorkerQueueIdentityProvider,
  type WorkerQueueIdentityRequest,
} from "./worker-queue-request-signing"
import {
  createSecretManager,
  type SecretManager,
} from "../../security/secret-manager"

export type TeamRole = "viewer" | "operator" | "admin" | "owner"
export type Principal = {
  readonly tenantID: string
  readonly teamID: string
  readonly actorID: string
  readonly tenantRole: string
  readonly teamRole: TeamRole
  readonly identityProvider: WorkerQueueIdentityProvider
  readonly keyID?: string
}

export type SignedRequest = WorkerQueueIdentityRequest
export { signRequest }

export type RequeueAction = {
  readonly id: string
  readonly tenantID: string
  readonly teamID: string
  readonly runID: string
  readonly expectedGeneration: number
  readonly expectedClaimToken: number
  readonly requestedBy: string
  readonly status: "pending" | "executed" | "rejected" | "expired"
  readonly requiredApprovals: number
  readonly approvalCount: number
  readonly timeCreated: number
  readonly timeExpires: number
  readonly timeDecided?: number
}

export class DisabledError extends Error {
  constructor() {
    super("Worker queue management API is disabled")
    this.name = "WorkerQueueAdminDisabledError"
  }
}

export class AuthenticationError extends Error {
  constructor(message = "Worker queue management request authentication failed") {
    super(message)
    this.name = "WorkerQueueAdminAuthenticationError"
  }
}

export class AuthorizationError extends Error {
  constructor(message = "Worker queue management request is forbidden") {
    super(message)
    this.name = "WorkerQueueAdminAuthorizationError"
  }
}

export class ConflictError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WorkerQueueAdminConflictError"
  }
}

export class RateLimitError extends Error {
  readonly retryAfterSeconds: number
  constructor(retryAfterSeconds: number) {
    super("Worker queue management rate limit exceeded")
    this.name = "WorkerQueueAdminRateLimitError"
    this.retryAfterSeconds = Math.max(1, retryAfterSeconds)
  }
}

export type Interface = {
  readonly enabled: boolean
  readonly authenticate: (request: SignedRequest) => Effect.Effect<Principal>
  readonly readiness: (principal: Principal) => Effect.Effect<WorkerQueueReadiness>
  readonly prometheus: (principal: Principal) => Effect.Effect<string>
  readonly recoverable: (principal: Principal, limit?: number) => Effect.Effect<readonly WorkerJob[]>
  readonly requestRequeue: (
    principal: Principal,
    input: {
      readonly runID: string
      readonly expectedGeneration: number
      readonly expectedClaimToken: number
      readonly requestID?: string
    },
  ) => Effect.Effect<RequeueAction>
  readonly approve: (principal: Principal, actionID: string) => Effect.Effect<RequeueAction>
  readonly revokeApproval: (
    principal: Principal,
    actionID: string,
    actorID: string,
    reason: string,
  ) => Effect.Effect<RequeueAction>
  readonly expireApprovals: (principal: Principal) => Effect.Effect<{ readonly expired: number }>
  readonly breakGlassRequeue: (
    principal: Principal,
    input: {
      readonly runID: string
      readonly expectedGeneration: number
      readonly expectedClaimToken: number
      readonly incidentID: string
      readonly reason: string
      readonly token: string
      readonly requestID?: string
    },
  ) => Effect.Effect<WorkerJob>
  readonly action: (principal: Principal, actionID: string) => Effect.Effect<RequeueAction>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/WorkerQueueAdmin") {}

export const layer = layerFromEnv()

export function layerFromEnv(env: NodeJS.ProcessEnv = process.env) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const settings = yield* Effect.tryPromise({
        try: () => settingsFromEnv(env),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      }).pipe(Effect.orDie)
      if (settings === undefined) return disabledService()
      const sql = makeClient({ url: settings.url, max: 4 })
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise(() => sql.end({ timeout: 5 })).pipe(Effect.catchCause(() => Effect.void)),
      )
      yield* Effect.tryPromise(() => assertRlsReady(sql)).pipe(Effect.orDie)
      const effect = <A>(operation: () => Promise<A>) =>
        Effect.tryPromise({
          try: operation,
          catch: (error) => (error instanceof Error ? error : new Error(String(error))),
        }).pipe(Effect.orDie)
      const telemetry = { tenantID: settings.tenant.tenantID, teamID: settings.teamID }
      return Service.of({
        enabled: true,
        authenticate: (request) => effect(() => authenticateWithAudit(sql, settings, request)),
        readiness: (principal) =>
          Effect.sync(() => authorize(principal, "observe")).pipe(
            Effect.andThen(
              effect(() =>
                readWorkerQueueReadiness(sql, {
                  tenant: settings.tenant,
                  maxPendingAgeMs: settings.maxPendingAgeMs,
                }),
              ),
            ),
            Effect.tap((result) => Effect.sync(() => observeWorkerQueue(result, telemetry))),
          ),
        prometheus: (principal) =>
          Effect.sync(() => authorize(principal, "observe")).pipe(
            Effect.andThen(
              effect(() =>
                readWorkerQueueReadiness(sql, {
                  tenant: settings.tenant,
                  maxPendingAgeMs: settings.maxPendingAgeMs,
                }),
              ),
            ),
            Effect.tap((result) => Effect.sync(() => observeWorkerQueue(result, telemetry))),
            Effect.map((result) => renderWorkerQueuePrometheus(result, telemetry)),
          ),
        recoverable: (principal, limit) =>
          Effect.sync(() => authorize(principal, "recover")).pipe(
            Effect.andThen(effect(() => listWorkerQueueRecoverable(sql, { tenant: settings.tenant, limit }))),
          ),
        requestRequeue: (principal, input) =>
          Effect.sync(() => authorize(principal, "request")).pipe(
            Effect.andThen(effect(() => requestRequeue(sql, settings, principal, input))),
            Effect.tap(() =>
              Effect.sync(() => recordWorkerQueueOperatorAction({ ...telemetry, action: "request", outcome: "allowed" })),
            ),
          ),
        approve: (principal, actionID) =>
          Effect.sync(() => authorize(principal, "approve")).pipe(
            Effect.andThen(effect(() => approve(sql, settings, principal, actionID))),
            Effect.tap((action) =>
              Effect.sync(() => {
                recordWorkerQueueOperatorAction({ ...telemetry, action: "approve", outcome: "allowed" })
                if (action.status === "executed") {
                  recordWorkerQueueOperatorAction({ ...telemetry, action: "execute", outcome: "allowed" })
                }
                if (action.status === "rejected") {
                  recordWorkerQueueOperatorAction({ ...telemetry, action: "execute", outcome: "rejected" })
                }
              }),
            ),
          ),
        revokeApproval: (principal, actionID, actorID, reason) =>
          Effect.sync(() => authorize(principal, "approve")).pipe(
            Effect.andThen(effect(() => revokeApproval(sql, settings, principal, actionID, actorID, reason))),
          ),
        expireApprovals: (principal) =>
          Effect.sync(() => authorize(principal, "approve")).pipe(
            Effect.andThen(effect(() => expirePendingActions(sql, settings, principal))),
          ),
        breakGlassRequeue: (principal, input) =>
          Effect.sync(() => authorize(principal, "break-glass")).pipe(
            Effect.andThen(effect(() => breakGlassRequeue(sql, settings, principal, input))),
          ),
        action: (principal, actionID) =>
          Effect.sync(() => authorize(principal, "recover")).pipe(
            Effect.andThen(effect(() => readAction(sql, settings, principal, actionID))),
          ),
      })
    }),
  )
}

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

function disabledService() {
  const disabled = () => Effect.die(new DisabledError())
  return Service.of({
    enabled: false,
    authenticate: disabled,
    readiness: disabled,
    prometheus: disabled,
    recoverable: disabled,
    requestRequeue: disabled,
    approve: disabled,
    revokeApproval: disabled,
    expireApprovals: disabled,
    breakGlassRequeue: disabled,
    action: disabled,
  })
}

type Settings = {
  readonly url: string
  readonly tenant: TenantContext & { readonly actorID: string; readonly teamID: string }
  readonly teamID: string
  readonly identity: WorkerQueueIdentityAdapter
  readonly requestSkewMs: number
  readonly nonceTtlMs: number
  readonly actionTtlMs: number
  readonly requiredApprovals: number
  readonly maxPendingAgeMs: number
  readonly rateLimits: Readonly<Record<RateLimitScope, RateLimit>>
  readonly secretManager: SecretManager
  readonly breakGlass?: {
    readonly secretRef: string
  }
}

type RateLimitScope = "observe" | "recover" | "mutate" | "approval" | "break-glass"
type RateLimit = { readonly limit: number; readonly windowMs: number }

async function settingsFromEnv(env: NodeJS.ProcessEnv): Promise<Settings | undefined> {
  if (env.OPENCODE_POSTGRES_WORKER_QUEUE_ADMIN_ENABLED !== "1") return undefined
  if (env.OPENCODE_DATABASE_BACKEND !== "postgres-alpha") {
    throw new Error("Worker queue management API requires OPENCODE_DATABASE_BACKEND=postgres-alpha")
  }
  const config = configFromEnv(env)
  if (config === undefined) throw new Error("Worker queue management API requires OPENCODE_DATABASE_URL")
  const tenantID = required(env.OPENCODE_TENANT_ID, "OPENCODE_TENANT_ID")
  const teamID = required(env.OPENCODE_TEAM_ID, "OPENCODE_TEAM_ID")
  const actorID = required(env.OPENCODE_ACTOR_ID, "OPENCODE_ACTOR_ID")
  const secretManager = createSecretManager({
    env,
    cacheTtlMs: integer(env.OPENCODE_SECRET_MANAGER_CACHE_TTL_MS, 60_000, 0, 3_600_000),
  })
  const breakGlassEnabled = env.OPENCODE_WORKER_QUEUE_BREAK_GLASS_ENABLED === "1"
  const breakGlassSecretRef = optional(env.OPENCODE_WORKER_QUEUE_BREAK_GLASS_SECRET_REF)
  if (breakGlassEnabled && breakGlassSecretRef === undefined) {
    throw new Error("Break-glass requires OPENCODE_WORKER_QUEUE_BREAK_GLASS_SECRET_REF")
  }
  return {
    url: config.url,
    tenant: { tenantID, teamID, actorID },
    teamID,
    identity: await identityAdapterFromEnv(env, secretManager),
    requestSkewMs: positive(env.OPENCODE_WORKER_QUEUE_ADMIN_REQUEST_SKEW_MS, 300_000),
    nonceTtlMs: positive(env.OPENCODE_WORKER_QUEUE_ADMIN_NONCE_TTL_MS, 600_000),
    actionTtlMs: positive(env.OPENCODE_WORKER_QUEUE_ACTION_TTL_MS, 3_600_000),
    requiredApprovals: Math.max(2, positive(env.OPENCODE_WORKER_QUEUE_REQUIRED_APPROVALS, 2)),
    maxPendingAgeMs: positive(env.OPENCODE_WORKER_QUEUE_MAX_PENDING_AGE_MS, 60_000),
    rateLimits: rateLimitsFromEnv(env),
    secretManager,
    ...(breakGlassEnabled ? { breakGlass: { secretRef: breakGlassSecretRef! } } : {}),
  }
}

async function authenticateWithAudit(sql: Sql, settings: Settings, request: SignedRequest): Promise<Principal> {
  try {
    const principal = await authenticate(sql, settings, request)
    await writeIdentityAudit(sql, settings, request, {
      actorID: principal.actorID,
      provider: principal.identityProvider,
      keyID: principal.keyID,
      outcome: "allow",
      reasonCode: "authenticated",
    })
    return principal
  } catch (error) {
    await writeIdentityAudit(sql, settings, request, {
      actorID: request.actorID === "" ? undefined : request.actorID,
      provider: inferredProvider(request),
      keyID: request.keyID,
      outcome: error instanceof RateLimitError ? "throttle" : "deny",
      reasonCode: error instanceof Error ? error.name : "unknown",
    })
    throw error
  }
}

async function authenticate(sql: Sql, settings: Settings, request: SignedRequest): Promise<Principal> {
  if (
    !Number.isSafeInteger(request.timestamp) ||
    Math.abs(Date.now() - request.timestamp) > settings.requestSkewMs ||
    !/^[a-zA-Z0-9_-]{16,128}$/.test(request.nonce)
  ) {
    throw new AuthenticationError()
  }
  const identity = await settings.identity.authenticate(request).catch(() => {
    throw new AuthenticationError()
  })
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    const now = await databaseNow(tx)
    await tx`delete from worker_queue_api_nonce where expires_at <= ${now}`
    try {
      await tx`
        insert into worker_queue_api_nonce (
          tenant_id, actor_id, nonce, signature_digest, expires_at, time_created
        )
        values (
          ${settings.tenant.tenantID}, ${identity.actorID}, ${request.nonce},
          ${credentialDigest(request, identity.provider, identity.keyID)},
          ${now + settings.nonceTtlMs}, ${now}
        )
      `
    } catch (error) {
      if (postgresCode(error) === "23505") throw new AuthenticationError("Worker queue management nonce was replayed")
      throw error
    }
    const memberships = await tx<{ tenant_role: string; team_role: TeamRole }[]>`
      select tm.role as tenant_role, gm.role as team_role
      from tenant_member tm
      join team_member gm
        on gm.tenant_id = tm.tenant_id
       and gm.actor_id = tm.actor_id
      where tm.actor_id = ${identity.actorID}
        and gm.team_id = ${settings.teamID}
    `
    const membership = memberships[0]
    if (membership === undefined) throw new AuthorizationError()
    const principal = {
      tenantID: settings.tenant.tenantID,
      teamID: settings.teamID,
      actorID: identity.actorID,
      tenantRole: membership.tenant_role,
      teamRole: membership.team_role,
      identityProvider: identity.provider,
      ...(identity.keyID === undefined ? {} : { keyID: identity.keyID }),
    }
    await enforceRateLimit(tx, settings, principal.actorID, rateLimitScope(request), now)
    return principal
  })
}

function authorize(
  principal: Principal,
  permission: "observe" | "recover" | "request" | "approve" | "break-glass",
) {
  const allowed: Record<typeof permission, readonly TeamRole[]> = {
    observe: ["viewer", "operator", "admin", "owner"],
    recover: ["operator", "admin", "owner"],
    request: ["operator", "admin", "owner"],
    approve: ["admin", "owner"],
    "break-glass": ["owner"],
  }
  if (!allowed[permission].includes(principal.teamRole)) throw new AuthorizationError()
}

async function requestRequeue(
  sql: Sql,
  settings: Settings,
  principal: Principal,
  input: {
    readonly runID: string
    readonly expectedGeneration: number
    readonly expectedClaimToken: number
    readonly requestID?: string
  },
) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    const now = await databaseNow(tx)
    await expireActions(tx, now, settings, principal)
    const jobs = await tx<{ status: string; requested_generation: string | number; claim_token: string | number }[]>`
      select status, requested_generation, claim_token
      from worker_job
      where run_id = ${input.runID}
      for update
    `
    const job = jobs[0]
    if (
      job === undefined ||
      (job.status !== "failed" && job.status !== "cancelled") ||
      Number(job.requested_generation) !== input.expectedGeneration ||
      Number(job.claim_token) !== input.expectedClaimToken
    ) {
      throw new ConflictError("Worker job is not recoverable with the supplied generation and claim token")
    }
    const id = `wqa_${randomUUID()}`
    try {
      await tx`
        insert into worker_queue_action (
          tenant_id, id, team_id, run_id, action, expected_generation,
          expected_claim_token, requested_by, status, required_approvals,
          time_created, time_expires
        )
        values (
          ${settings.tenant.tenantID}, ${id}, ${settings.teamID}, ${input.runID}, 'requeue',
          ${input.expectedGeneration}, ${input.expectedClaimToken}, ${principal.actorID}, 'pending',
          ${settings.requiredApprovals}, ${now}, ${now + settings.actionTtlMs}
        )
      `
    } catch (error) {
      if (postgresCode(error) === "23505") throw new ConflictError("A pending requeue action already exists")
      throw error
    }
    if (principal.teamRole === "admin" || principal.teamRole === "owner") {
      await tx`
        insert into worker_queue_action_approval (
          tenant_id, action_id, actor_id, decision, time_created
        )
        values (${settings.tenant.tenantID}, ${id}, ${principal.actorID}, 'approve', ${now})
      `
      await recordApprovalEvent(tx, settings, {
        actionID: id,
        subjectActorID: principal.actorID,
        actorID: principal.actorID,
        event: "approve",
        identityProvider: principal.identityProvider,
        now,
      })
    }
    await writeAudit(tx, {
      tenantID: settings.tenant.tenantID,
      actorID: principal.actorID,
      action: "worker_queue.requeue.requested",
      resourceID: id,
      requestID: input.requestID,
      metadata: {
        teamID: settings.teamID,
        runID: input.runID,
        expectedGeneration: input.expectedGeneration,
        expectedClaimToken: input.expectedClaimToken,
        requiredApprovals: settings.requiredApprovals,
      },
      now,
    })
    return await readActionTx(tx, id)
  })
}

async function approve(sql: Sql, settings: Settings, principal: Principal, actionID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    const now = await databaseNow(tx)
    await expireActions(tx, now, settings, principal)
    const rows = await tx<ActionRow[]>`
      select *
      from worker_queue_action
      where id = ${actionID}
      for update
    `
    const current = rows[0]
    if (current === undefined) throw new ConflictError("Worker queue action was not found")
    if (current.status !== "pending") throw new ConflictError(`Worker queue action is ${current.status}`)
    if (Number(current.time_expires) <= now) {
      return await readActionTx(tx, actionID)
    }
    const inserted = await tx`
      insert into worker_queue_action_approval (
        tenant_id, action_id, actor_id, decision, time_created
      )
      values (${settings.tenant.tenantID}, ${actionID}, ${principal.actorID}, 'approve', ${now})
      on conflict (tenant_id, action_id, actor_id) do nothing
      returning actor_id
    `
    if (inserted.length > 0) {
      await recordApprovalEvent(tx, settings, {
        actionID,
        subjectActorID: principal.actorID,
        actorID: principal.actorID,
        event: "approve",
        identityProvider: principal.identityProvider,
        now,
      })
      await writeAudit(tx, {
        tenantID: settings.tenant.tenantID,
        actorID: principal.actorID,
        action: "worker_queue.requeue.approved",
        resourceID: actionID,
        metadata: { teamID: settings.teamID },
        now,
      })
    }
    const approvalCount = await countApprovals(tx, actionID)
    if (approvalCount >= Number(current.required_approvals)) {
      try {
        await requeueWorkerJobInTransaction(tx, {
          tenant: {
            tenantID: settings.tenant.tenantID,
            teamID: settings.teamID,
            actorID: principal.actorID,
          },
          runID: current.run_id,
          expectedGeneration: Number(current.expected_generation),
          expectedClaimToken: Number(current.expected_claim_token),
          requestID: actionID,
        })
        await tx`
          update worker_queue_action
          set status = 'executed', time_decided = ${now}
          where id = ${actionID}
        `
      } catch (error) {
        if (!(error instanceof WorkerJobRequeueRejectedError)) throw error
        await tx`
          update worker_queue_action
          set status = 'rejected', time_decided = ${now}
          where id = ${actionID}
        `
      }
    }
    return await readActionTx(tx, actionID)
  })
}

async function revokeApproval(
  sql: Sql,
  settings: Settings,
  principal: Principal,
  actionID: string,
  actorID: string,
  reason: string,
) {
  if (reason.trim().length < 8 || reason.length > 500) throw new ConflictError("Approval revocation reason is invalid")
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    const now = await databaseNow(tx)
    await expireActions(tx, now, settings, principal)
    const actions = await tx<ActionRow[]>`
      select * from worker_queue_action where id = ${actionID} for update
    `
    const current = actions[0]
    if (current === undefined) throw new ConflictError("Worker queue action was not found")
    if (current.status !== "pending") throw new ConflictError(`Worker queue action is ${current.status}`)
    const removed = await tx`
      delete from worker_queue_action_approval
      where action_id = ${actionID} and actor_id = ${actorID}
      returning actor_id
    `
    if (removed.length === 0) throw new ConflictError("Approval was not active")
    await recordApprovalEvent(tx, settings, {
      actionID,
      subjectActorID: actorID,
      actorID: principal.actorID,
      event: "revoke",
      reason: reason.trim(),
      identityProvider: principal.identityProvider,
      now,
    })
    await writeAudit(tx, {
      tenantID: settings.tenant.tenantID,
      actorID: principal.actorID,
      action: "worker_queue.requeue.approval_revoked",
      resourceID: actionID,
      metadata: {
        teamID: settings.teamID,
        subjectActorID: actorID,
        reason: reason.trim(),
        identityProvider: principal.identityProvider,
      },
      now,
    })
    return await readActionTx(tx, actionID)
  })
}

async function expirePendingActions(sql: Sql, settings: Settings, principal: Principal) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    const now = await databaseNow(tx)
    return { expired: await expireActions(tx, now, settings, principal) }
  })
}

async function readAction(sql: Sql, settings: Settings, principal: Principal, actionID: string) {
  return await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    await expireActions(tx, await databaseNow(tx), settings, principal)
    return await readActionTx(tx, actionID)
  })
}

type ActionRow = {
  tenant_id: string
  id: string
  team_id: string
  run_id: string
  expected_generation: string | number
  expected_claim_token: string | number
  requested_by: string
  status: RequeueAction["status"]
  required_approvals: number
  time_created: string | number
  time_expires: string | number
  time_decided: string | number | null
}

async function readActionTx(tx: TransactionSql, actionID: string): Promise<RequeueAction> {
  const rows = await tx<ActionRow[]>`
    select *
    from worker_queue_action
    where id = ${actionID}
  `
  const row = rows[0]
  if (row === undefined) throw new ConflictError("Worker queue action was not found")
  return {
    id: row.id,
    tenantID: row.tenant_id,
    teamID: row.team_id,
    runID: row.run_id,
    expectedGeneration: Number(row.expected_generation),
    expectedClaimToken: Number(row.expected_claim_token),
    requestedBy: row.requested_by,
    status: row.status,
    requiredApprovals: row.required_approvals,
    approvalCount: await countApprovals(tx, actionID),
    timeCreated: Number(row.time_created),
    timeExpires: Number(row.time_expires),
    ...(row.time_decided === null ? {} : { timeDecided: Number(row.time_decided) }),
  }
}

async function countApprovals(tx: TransactionSql, actionID: string) {
  const rows = await tx<{ count: string | number }[]>`
    select count(*) as count
    from worker_queue_action_approval
    where action_id = ${actionID}
      and decision = 'approve'
  `
  return Number(rows[0]?.count ?? 0)
}

async function expireActions(
  tx: TransactionSql,
  now: number,
  settings: Settings,
  principal: Principal,
) {
  const expired = await tx<{ id: string }[]>`
    update worker_queue_action
    set status = 'expired', time_decided = ${now}
    where status = 'pending'
      and time_expires <= ${now}
    returning id
  `
  for (const action of expired) {
    await recordApprovalEvent(tx, settings, {
      actionID: action.id,
      actorID: principal.actorID,
      event: "expire",
      identityProvider: principal.identityProvider,
      now,
    })
    await writeAudit(tx, {
      tenantID: settings.tenant.tenantID,
      actorID: principal.actorID,
      action: "worker_queue.requeue.approval_expired",
      resourceID: action.id,
      metadata: { teamID: settings.teamID, identityProvider: principal.identityProvider },
      now,
    })
  }
  return expired.length
}

async function recordApprovalEvent(
  tx: TransactionSql,
  settings: Settings,
  input: {
    readonly actionID: string
    readonly subjectActorID?: string
    readonly actorID: string
    readonly event: "approve" | "revoke" | "expire"
    readonly reason?: string
    readonly identityProvider: WorkerQueueIdentityProvider
    readonly now: number
  },
) {
  await tx`
    insert into worker_queue_action_approval_event (
      tenant_id, id, action_id, subject_actor_id, actor_id, event,
      reason, identity_provider, time_created
    )
    values (
      ${settings.tenant.tenantID}, ${`wqae_${randomUUID()}`}, ${input.actionID},
      ${input.subjectActorID ?? null}, ${input.actorID}, ${input.event},
      ${input.reason ?? null}, ${input.identityProvider}, ${input.now}
    )
  `
}

async function breakGlassRequeue(
  sql: Sql,
  settings: Settings,
  principal: Principal,
  input: {
    readonly runID: string
    readonly expectedGeneration: number
    readonly expectedClaimToken: number
    readonly incidentID: string
    readonly reason: string
    readonly token: string
    readonly requestID?: string
  },
) {
  const incidentID = input.incidentID.trim()
  const reason = input.reason.trim()
  if (
    settings.breakGlass === undefined ||
    principal.identityProvider === "hmac" ||
    !/^[a-zA-Z0-9_-]{8,128}$/.test(incidentID) ||
    reason.length < 16 ||
    reason.length > 1000
  ) {
    await writeBreakGlassAudit(sql, settings, principal, input, "deny", "policy_rejected")
    throw new AuthorizationError("Break-glass policy rejected the request")
  }
  const valid = await verifyBreakGlassToken(settings, input.token)
  if (!valid) {
    await writeBreakGlassAudit(sql, settings, principal, input, "deny", "second_factor_rejected")
    throw new AuthenticationError("Break-glass second factor rejected")
  }
  try {
    return await sql.begin(async (tx) => {
      await setTenantContext(tx, settings.tenant)
      const now = await databaseNow(tx)
      const job = await requeueWorkerJobInTransaction(tx, {
        tenant: {
          tenantID: settings.tenant.tenantID,
          teamID: settings.teamID,
          actorID: principal.actorID,
        },
        runID: input.runID,
        expectedGeneration: input.expectedGeneration,
        expectedClaimToken: input.expectedClaimToken,
        requestID: input.requestID ?? incidentID,
      })
      await tx`
        insert into worker_queue_break_glass (
          tenant_id, id, incident_id, team_id, run_id, actor_id,
          identity_provider, reason, expected_generation, expected_claim_token,
          credential_digest, status, time_created
        )
        values (
          ${settings.tenant.tenantID}, ${`wqbg_${randomUUID()}`}, ${incidentID},
          ${settings.teamID}, ${input.runID}, ${principal.actorID},
          ${principal.identityProvider}, ${reason}, ${input.expectedGeneration},
          ${input.expectedClaimToken}, ${digest(input.token)}, 'executed', ${now}
        )
      `
      await writeAudit(tx, {
        tenantID: settings.tenant.tenantID,
        actorID: principal.actorID,
        action: "worker_queue.break_glass.requeue",
        resourceID: input.runID,
        requestID: input.requestID,
        metadata: {
          teamID: settings.teamID,
          incidentID,
          reason,
          identityProvider: principal.identityProvider,
        },
        now,
      })
      return job
    })
  } catch (error) {
    await writeBreakGlassAudit(sql, settings, principal, input, "deny", "execution_rejected")
    if (postgresCode(error) === "23505") throw new ConflictError("Break-glass incident ID was already used")
    if (error instanceof WorkerJobRequeueRejectedError) throw new ConflictError(error.message)
    throw error
  }
}

async function verifyBreakGlassToken(settings: Settings, token: string) {
  if (settings.breakGlass === undefined || token === "") return false
  const expected = await settings.secretManager.resolve(settings.breakGlass.secretRef)
  if (safeEqual(token, expected)) return true
  return safeEqual(
    token,
    await settings.secretManager.resolve(settings.breakGlass.secretRef, { refresh: true }),
  )
}

async function writeBreakGlassAudit(
  sql: Sql,
  settings: Settings,
  principal: Principal,
  input: { readonly runID: string; readonly incidentID: string; readonly reason: string },
  outcome: "allow" | "deny",
  reasonCode: string,
) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    await tx`
      insert into audit_event (
        id, tenant_id, actor_id, action, resource_type, resource_id,
        outcome, metadata, time_created
      )
      values (
        ${`audit_break_glass_${randomUUID()}`}, ${settings.tenant.tenantID},
        ${principal.actorID}, 'worker_queue.break_glass.attempt', 'worker_job',
        ${input.runID}, ${outcome}, ${tx.json({
          incidentID: input.incidentID,
          reason: input.reason,
          reasonCode,
          identityProvider: principal.identityProvider,
        } as any)}, ${await databaseNow(tx)}
      )
    `
  })
}

async function enforceRateLimit(
  tx: TransactionSql,
  settings: Settings,
  actorID: string,
  scope: RateLimitScope,
  now: number,
) {
  const limit = settings.rateLimits[scope]
  const windowStartedAt = Math.floor(now / limit.windowMs) * limit.windowMs
  const rows = await tx<{ request_count: number }[]>`
    insert into worker_queue_api_rate_limit (
      tenant_id, actor_id, scope, window_started_at, request_count, time_updated
    )
    values (${settings.tenant.tenantID}, ${actorID}, ${scope}, ${windowStartedAt}, 1, ${now})
    on conflict (tenant_id, actor_id, scope) do update
    set
      window_started_at = excluded.window_started_at,
      request_count = case
        when worker_queue_api_rate_limit.window_started_at = excluded.window_started_at
          then worker_queue_api_rate_limit.request_count + 1
        else 1
      end,
      time_updated = excluded.time_updated
    returning request_count
  `
  if (Number(rows[0]?.request_count ?? 0) > limit.limit) {
    throw new RateLimitError(Math.ceil((windowStartedAt + limit.windowMs - now) / 1000))
  }
}

async function writeIdentityAudit(
  sql: Sql,
  settings: Settings,
  request: SignedRequest,
  input: {
    readonly actorID?: string
    readonly provider: WorkerQueueIdentityProvider
    readonly keyID?: string
    readonly outcome: "allow" | "deny" | "throttle"
    readonly reasonCode: string
  },
) {
  await sql.begin(async (tx) => {
    await setTenantContext(tx, settings.tenant)
    await tx`
      insert into worker_queue_identity_audit (
        id, tenant_id, actor_id, provider, key_id, method, target,
        outcome, reason_code, nonce_digest, credential_digest, time_created
      )
      values (
        ${`wqia_${randomUUID()}`}, ${settings.tenant.tenantID}, ${input.actorID ?? null},
        ${input.provider}, ${input.keyID ?? null}, ${request.method.toUpperCase()},
        ${request.target}, ${input.outcome}, ${input.reasonCode}, ${digest(request.nonce)},
        ${credentialDigest(request, input.provider, input.keyID)}, ${await databaseNow(tx)}
      )
    `
  })
}

function inferredProvider(request: SignedRequest): WorkerQueueIdentityProvider {
  if (request.identityProvider !== undefined) return request.identityProvider
  if (request.identityToken !== undefined || request.sessionCookie !== undefined) {
    return request.identityToken?.split(".").length === 3 ? "oidc" : "better-auth"
  }
  return "hmac"
}

function rateLimitScope(request: SignedRequest): RateLimitScope {
  if (request.target.includes("/break-glass/")) return "break-glass"
  if (request.target.includes("/approve") || request.target.includes("/revoke") || request.target.endsWith("/expire")) {
    return "approval"
  }
  if (request.method.toUpperCase() === "POST") return "mutate"
  if (request.target.endsWith("/readiness") || request.target.endsWith("/metrics")) return "observe"
  return "recover"
}

function rateLimitsFromEnv(env: NodeJS.ProcessEnv): Readonly<Record<RateLimitScope, RateLimit>> {
  const defaults: Record<RateLimitScope, RateLimit> = {
    observe: { limit: 120, windowMs: 60_000 },
    recover: { limit: 60, windowMs: 60_000 },
    mutate: { limit: 20, windowMs: 60_000 },
    approval: { limit: 30, windowMs: 60_000 },
    "break-glass": { limit: 2, windowMs: 60_000 },
  }
  const raw = optional(env.OPENCODE_WORKER_QUEUE_RATE_LIMITS)
  if (raw === undefined) return defaults
  const parsed = JSON.parse(raw) as Record<string, { limit?: unknown; windowMs?: unknown }>
  for (const scope of Object.keys(defaults) as RateLimitScope[]) {
    const value = parsed[scope]
    if (value === undefined) continue
    if (
      !Number.isSafeInteger(value.limit) ||
      Number(value.limit) < 1 ||
      !Number.isSafeInteger(value.windowMs) ||
      Number(value.windowMs) < 1_000
    ) {
      throw new Error(`Invalid worker queue rate limit for ${scope}`)
    }
    defaults[scope] = { limit: Number(value.limit), windowMs: Number(value.windowMs) }
  }
  return defaults
}

function digest(value: string) {
  return createHash("sha256").update(value).digest("hex")
}

function safeEqual(left: string, right: string) {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.length === b.length && timingSafeEqual(a, b)
}

async function writeAudit(
  tx: TransactionSql,
  input: {
    readonly tenantID: string
    readonly actorID: string
    readonly action: string
    readonly resourceID: string
    readonly requestID?: string
    readonly metadata: Record<string, unknown>
    readonly now: number
  },
) {
  await tx`
    insert into audit_event (
      id, tenant_id, actor_id, action, resource_type, resource_id,
      outcome, request_id, metadata, time_created
    )
    values (
      ${`audit_worker_queue_${randomUUID()}`}, ${input.tenantID}, ${input.actorID},
      ${input.action}, 'worker_queue_action', ${input.resourceID}, 'allow',
      ${input.requestID ?? null}, ${tx.json(input.metadata as any)}, ${input.now}
    )
  `
}

async function databaseNow(tx: TransactionSql) {
  const rows = await tx<{ now_ms: string | number }[]>`
    select floor(extract(epoch from clock_timestamp()) * 1000)::bigint as now_ms
  `
  return Number(rows[0]!.now_ms)
}

function credentialDigest(
  request: SignedRequest,
  provider: WorkerQueueIdentityProvider,
  keyID: string | undefined,
) {
  return createHash("sha256")
    .update(
      [
        provider,
        keyID ?? "",
        request.signature,
        request.identityToken ?? "",
        request.sessionCookie ?? "",
      ].join("\u0000"),
    )
    .digest("hex")
}

function positive(value: string | undefined, fallback: number) {
  const parsed = Number(value ?? fallback)
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback
}

function integer(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < min || result > max) {
    throw new Error(`Expected an integer between ${min} and ${max}`)
  }
  return result
}

function optional(value: string | undefined) {
  const result = value?.trim()
  return result === undefined || result === "" ? undefined : result
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value.trim()
}

function postgresCode(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error ? String(error.code) : undefined
}

export * as WorkerQueueAdmin from "./worker-queue-admin"
