import { ExecutionResourceBinding } from "@opencode-ai/core/identity/execution-resource-binding"
import { PermissionCenter } from "@opencode-ai/core/identity/permission-center"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"
import { Cause, Effect } from "effect"
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"

export function sessionIDFromPath(pathname: string) {
  const segments = pathname.split("/").filter(Boolean)
  const index = segments.indexOf("session")
  if (index === -1) return
  const value = segments[index + 1]
  return value?.startsWith("ses_") ? value : undefined
}

export function permissionForRequest(
  pathname: string,
  method: string,
): PermissionCenter.PermissionKey | undefined {
  const verb = method.toUpperCase()
  if (pathname === "/project" || pathname.startsWith("/project/")) {
    return verb === "GET" || verb === "HEAD" ? "project.view" : "project.manage"
  }
  if (
    pathname === "/session" ||
    pathname.startsWith("/session/") ||
    pathname === "/api/session" ||
    pathname.startsWith("/api/session/")
  ) {
    if (verb === "GET" || verb === "HEAD") return "session.view"
    if (verb === "DELETE" || verb === "PATCH" || verb === "PUT") return "session.manage"
    if (verb === "POST") {
      if (pathname.endsWith("/abort")) return "agent_run.stop"
      if (/\/(message|prompt|command|shell|chat)$/.test(pathname)) return "agent_run.execute"
      if (pathname.endsWith("/fork") || pathname === "/session" || pathname === "/api/session") {
        return "session.create"
      }
      return "session.manage"
    }
  }
  if (pathname === "/event" || pathname.startsWith("/event/")) return "session.view"
  if (
    pathname === "/permission" ||
    pathname.startsWith("/permission/") ||
    pathname === "/question" ||
    pathname.startsWith("/question/")
  ) {
    return "agent_run.execute"
  }
  return undefined
}

function response(error: unknown) {
  const value =
    error instanceof ExecutionResourceBinding.Error
      ? { error: error.code, message: error.message }
      : error instanceof PermissionCenter.Error
        ? { error: error.code, message: error.message }
      : { error: "organization_forbidden", message: "Active organization membership required" }
  const status = value.error === "organization_header_required" ? 400 : 403
  return HttpServerResponse.jsonUnsafe(value, { status })
}

export const organizationExecutionRouterMiddleware = HttpRouter.middleware()(
  Effect.succeed((effect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest
      if (!SaasIdentity.enabled()) return yield* effect
      const pathname = new URL(request.url, "http://localhost").pathname
      if (pathname === "/api/health" || pathname === "/global/health") return yield* effect

      const resolved = yield* Effect.promise(async () => {
        try {
          const session = await SaasIdentity.authenticate(new Headers(request.headers as HeadersInit))
          if (!session) return { status: 401 as const }
          const organizationID = ExecutionResourceBinding.organizationID(
            new Headers(request.headers as HeadersInit),
          )
          const context = await ExecutionResourceBinding.authorize(session.actor, organizationID)
          const permission = permissionForRequest(pathname, request.method)
          if (permission) await PermissionCenter.authorize(session.actor, organizationID, permission)
          const sessionID = sessionIDFromPath(pathname)
          if (sessionID) await ExecutionResourceBinding.assertResource(context, "session", sessionID)
          return { status: 200 as const, context }
        } catch (error) {
          return { status: 403 as const, error }
        }
      })

      if (resolved.status === 403) return response(resolved.error)
      if (resolved.status === 401) {
        return HttpServerResponse.jsonUnsafe({ error: "authentication_required" }, { status: 401 })
      }

      const context = resolved.context
      ExecutionResourceBinding.setRequestContext(request.source, context)
      return yield* effect.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            ExecutionResourceBinding.clearRequestContext(request.source)
          }),
        ),
        Effect.catchCause((cause) => {
          const error = Cause.squash(cause)
          if (error instanceof ExecutionResourceBinding.Error) return Effect.succeed(response(error))
          return Effect.failCause(cause)
        }),
      )
    }),
  ),
)
