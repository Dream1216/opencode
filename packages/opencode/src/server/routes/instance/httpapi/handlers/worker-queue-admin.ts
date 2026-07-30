import { Cause, Effect, Exit } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import * as WorkerQueueAdmin from "@opencode-ai/core/database/postgres/worker-queue-admin"
import { renderQuestionShadowPrometheus } from "@opencode-ai/core/observability/question-shadow-telemetry"
import { WorkerQueueAdminApi } from "../groups/worker-queue-admin"

type RawContext = {
  readonly request: HttpServerRequest.HttpServerRequest
  readonly params?: { readonly actionID?: string; readonly actorID?: string }
}

export const workerQueueAdminHandlers = HttpApiBuilder.group(WorkerQueueAdminApi, "workerQueueAdmin", (handlers) =>
  Effect.gen(function* () {
    const admin = yield* WorkerQueueAdmin.Service
    const execute = <A>(
      ctx: RawContext,
      body: string,
      operation: (principal: WorkerQueueAdmin.Principal) => Effect.Effect<A>,
      response: (value: A) => HttpServerResponse.HttpServerResponse = (value) =>
        HttpServerResponse.jsonUnsafe(value),
    ) =>
      Effect.gen(function* () {
        if (!admin.enabled) return notFound()
        const request = ctx.request
        const url = new URL(request.url, "http://localhost")
        const authenticated = yield* admin
          .authenticate({
            method: request.method,
            target: `${url.pathname}${url.search}`,
            actorID: request.headers["x-opencode-actor-id"] ?? "",
            timestamp: Number(request.headers["x-opencode-request-timestamp"] ?? Number.NaN),
            nonce: request.headers["x-opencode-request-nonce"] ?? "",
            signature: request.headers["x-opencode-request-signature"] ?? "",
            keyID: request.headers["x-opencode-request-key-id"],
            identityProvider: identityProvider(request.headers["x-opencode-identity-provider"]),
            identityToken:
              request.headers["x-opencode-identity-token"] ??
              bearerToken(request.headers.authorization),
            sessionCookie: request.headers.cookie,
            body,
          })
          .pipe(Effect.exit)
        if (Exit.isFailure(authenticated)) return errorResponse(authenticated.cause)
        const result = yield* operation(authenticated.value).pipe(Effect.exit)
        return Exit.isFailure(result) ? errorResponse(result.cause) : response(result.value)
      })
    const readiness = Effect.fn("WorkerQueueAdminHttpApi.readiness")(function* (ctx: RawContext) {
      return yield* execute(ctx, "", (principal) => admin.readiness(principal))
    })
    const metrics = Effect.fn("WorkerQueueAdminHttpApi.metrics")(function* (ctx: RawContext) {
      return yield* execute(ctx, "", (principal) => admin.prometheus(principal), (value) =>
        HttpServerResponse.text(`${value.trimEnd()}\n${renderQuestionShadowPrometheus()}`, {
          contentType: "text/plain; version=0.0.4; charset=utf-8",
        }),
      )
    })
    const recoverable = Effect.fn("WorkerQueueAdminHttpApi.recoverable")(function* (ctx: RawContext) {
      const url = new URL(ctx.request.url, "http://localhost")
      const limit = Number(url.searchParams.get("limit") ?? 100)
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) return badRequest("limit must be 1..500")
      return yield* execute(ctx, "", (principal) => admin.recoverable(principal, limit))
    })
    const requeue = Effect.fn("WorkerQueueAdminHttpApi.requeue")(function* (ctx: RawContext) {
      const body = yield* Effect.orDie(ctx.request.text)
      const payload = parseRequeue(body)
      if (payload === undefined) return badRequest("Invalid requeue payload")
      return yield* execute(ctx, body, (principal) =>
        admin.requestRequeue(principal, {
          ...payload,
          requestID: ctx.request.headers["x-request-id"],
        }),
      )
    })
    const action = Effect.fn("WorkerQueueAdminHttpApi.action")(function* (ctx: RawContext) {
      const actionID = ctx.params?.actionID
      if (actionID === undefined || actionID === "") return badRequest("actionID is required")
      return yield* execute(ctx, "", (principal) => admin.action(principal, actionID))
    })
    const approve = Effect.fn("WorkerQueueAdminHttpApi.approve")(function* (ctx: RawContext) {
      const actionID = ctx.params?.actionID
      if (actionID === undefined || actionID === "") return badRequest("actionID is required")
      const body = yield* Effect.orDie(ctx.request.text)
      return yield* execute(ctx, body, (principal) => admin.approve(principal, actionID))
    })
    const revoke = Effect.fn("WorkerQueueAdminHttpApi.revokeApproval")(function* (ctx: RawContext) {
      const actionID = ctx.params?.actionID
      const actorID = ctx.params?.actorID
      if (!actionID || !actorID) return badRequest("actionID and actorID are required")
      const body = yield* Effect.orDie(ctx.request.text)
      const reason = parseReason(body)
      if (reason === undefined) return badRequest("A revocation reason of 8..500 characters is required")
      return yield* execute(ctx, body, (principal) => admin.revokeApproval(principal, actionID, actorID, reason))
    })
    const expire = Effect.fn("WorkerQueueAdminHttpApi.expireApprovals")(function* (ctx: RawContext) {
      return yield* execute(ctx, "", (principal) => admin.expireApprovals(principal))
    })
    const breakGlass = Effect.fn("WorkerQueueAdminHttpApi.breakGlassRequeue")(function* (ctx: RawContext) {
      const body = yield* Effect.orDie(ctx.request.text)
      const payload = parseBreakGlass(body)
      if (payload === undefined) return badRequest("Invalid break-glass payload")
      return yield* execute(ctx, body, (principal) =>
        admin.breakGlassRequeue(principal, {
          ...payload,
          token: ctx.request.headers["x-opencode-break-glass-token"] ?? "",
          requestID: ctx.request.headers["x-request-id"],
        }),
      )
    })
    return handlers
      .handleRaw("workerQueueReadiness", readiness)
      .handleRaw("workerQueueMetrics", metrics)
      .handleRaw("workerQueueRecoverable", recoverable)
      .handleRaw("workerQueueRequeue", requeue)
      .handleRaw("workerQueueAction", action)
      .handleRaw("workerQueueApprove", approve)
      .handleRaw("workerQueueRevokeApproval", revoke)
      .handleRaw("workerQueueExpireApprovals", expire)
      .handleRaw("workerQueueBreakGlassRequeue", breakGlass)
  }),
)

function parseRequeue(body: string) {
  try {
    const input = JSON.parse(body) as Record<string, unknown>
    if (
      typeof input.runID !== "string" ||
      input.runID === "" ||
      !Number.isSafeInteger(input.expectedGeneration) ||
      Number(input.expectedGeneration) < 0 ||
      !Number.isSafeInteger(input.expectedClaimToken) ||
      Number(input.expectedClaimToken) < 0
    ) {
      return undefined
    }
    return {
      runID: input.runID,
      expectedGeneration: Number(input.expectedGeneration),
      expectedClaimToken: Number(input.expectedClaimToken),
    }
  } catch {
    return undefined
  }
}

function parseReason(body: string) {
  try {
    const reason = (JSON.parse(body) as Record<string, unknown>).reason
    return typeof reason === "string" && reason.trim().length >= 8 && reason.length <= 500
      ? reason.trim()
      : undefined
  } catch {
    return undefined
  }
}

function parseBreakGlass(body: string) {
  try {
    const input = JSON.parse(body) as Record<string, unknown>
    if (
      typeof input.runID !== "string" ||
      !Number.isSafeInteger(input.expectedGeneration) ||
      !Number.isSafeInteger(input.expectedClaimToken) ||
      typeof input.incidentID !== "string" ||
      typeof input.reason !== "string"
    ) {
      return undefined
    }
    return {
      runID: input.runID,
      expectedGeneration: Number(input.expectedGeneration),
      expectedClaimToken: Number(input.expectedClaimToken),
      incidentID: input.incidentID,
      reason: input.reason,
    }
  } catch {
    return undefined
  }
}

function errorResponse(cause: Cause.Cause<never>) {
  const error = Cause.squash(cause)
  if (error instanceof WorkerQueueAdmin.DisabledError) return notFound()
  if (error instanceof WorkerQueueAdmin.AuthenticationError) {
    return HttpServerResponse.jsonUnsafe({ error: "worker_queue_unauthorized" }, { status: 401 })
  }
  if (error instanceof WorkerQueueAdmin.AuthorizationError) {
    return HttpServerResponse.jsonUnsafe({ error: "worker_queue_forbidden" }, { status: 403 })
  }
  if (error instanceof WorkerQueueAdmin.ConflictError) {
    return HttpServerResponse.jsonUnsafe({ error: "worker_queue_conflict", message: error.message }, { status: 409 })
  }
  if (error instanceof WorkerQueueAdmin.RateLimitError) {
    return HttpServerResponse.jsonUnsafe(
      { error: "worker_queue_rate_limited" },
      { status: 429, headers: { "retry-after": String(error.retryAfterSeconds) } },
    )
  }
  return HttpServerResponse.jsonUnsafe({ error: "worker_queue_internal_error" }, { status: 500 })
}

function badRequest(message: string) {
  return HttpServerResponse.jsonUnsafe({ error: "worker_queue_bad_request", message }, { status: 400 })
}

function notFound() {
  return HttpServerResponse.jsonUnsafe({ error: "not_found" }, { status: 404 })
}

function identityProvider(value: string | undefined) {
  return value === "hmac" || value === "oidc" || value === "better-auth" ? value : undefined
}

function bearerToken(value: string | undefined) {
  return value?.startsWith("Bearer ") ? value.slice("Bearer ".length) : undefined
}
