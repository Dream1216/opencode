import { Organization } from "@opencode-ai/core/identity/organization"
import { PermissionCenter } from "@opencode-ai/core/identity/permission-center"
import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"
import type { ActorContext } from "@opencode-ai/core/identity"
import { Effect } from "effect"
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"

export const organizationRoutes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    yield* router.add("GET", "/api/organizations", collection)
    yield* router.add("POST", "/api/organizations", collection)
    yield* router.add("GET", "/api/organizations/:organizationID/members", resource)
    yield* router.add("PATCH", "/api/organizations/:organizationID/members/:actorID", resource)
    yield* router.add("DELETE", "/api/organizations/:organizationID/members/:actorID", resource)
    yield* router.add("GET", "/api/organizations/:organizationID/invitations", resource)
    yield* router.add("POST", "/api/organizations/:organizationID/invitations", resource)
    yield* router.add("POST", "/api/organizations/:organizationID/transfer-ownership", resource)
    yield* router.add("GET", "/api/organizations/:organizationID/permissions", resource)
    yield* router.add("PUT", "/api/organizations/:organizationID/permissions/roles/:role", resource)
    yield* router.add("POST", "/api/organization-invitations/accept", (request) =>
      invitationDecision(request, "accept"),
    )
    yield* router.add("POST", "/api/organization-invitations/reject", (request) =>
      invitationDecision(request, "reject"),
    )
  }),
)

function collection(request: HttpServerRequest.HttpServerRequest) {
  if (request.method === "GET") return execute(request, (actor) => Organization.list(actor))
  if (request.method === "POST") {
    return executeBody(request, (actor, body) => {
      const input = parse(body)
      if (typeof input.name !== "string" || typeof input.slug !== "string") {
        throw new Organization.Error("organization_bad_request", "name and slug are required")
      }
      return Organization.create(actor, { name: input.name, slug: input.slug })
    })
  }
  return Effect.succeed(methodNotAllowed())
}

function resource(request: HttpServerRequest.HttpServerRequest) {
  const path = new URL(request.url, "http://localhost").pathname
  const parts = path.slice("/api/organizations/".length).split("/").filter(Boolean)
  const organizationID = parts[0]
  if (organizationID === undefined) return Effect.succeed(notFound())

  if (parts.length === 2 && parts[1] === "members" && request.method === "GET") {
    return execute(request, (actor) => Organization.members(actor, organizationID))
  }
  if (parts.length === 3 && parts[1] === "members" && request.method === "PATCH") {
    return executeBody(request, (actor, body) => {
      const input = parse(body)
      if (!isInvitationRole(input.role)) {
        throw new Organization.Error("organization_bad_request", "A non-owner member role is required")
      }
      return Organization.updateMemberRole(actor, organizationID, parts[2]!, input.role)
    })
  }
  if (parts.length === 3 && parts[1] === "members" && request.method === "DELETE") {
    return execute(request, async (actor) => {
      await Organization.removeMember(actor, organizationID, parts[2]!)
      return { removed: true }
    })
  }
  if (parts.length === 2 && parts[1] === "invitations" && request.method === "GET") {
    return execute(request, (actor) => Organization.invitations(actor, organizationID))
  }
  if (parts.length === 2 && parts[1] === "invitations" && request.method === "POST") {
    return executeBody(request, (actor, body) => {
      const input = parse(body)
      if (typeof input.email !== "string" || !isInvitationRole(input.role)) {
        throw new Organization.Error("organization_bad_request", "email and a non-owner role are required")
      }
      return Organization.invite(actor, organizationID, {
        email: input.email,
        role: input.role,
        ...(typeof input.expiresInMs === "number" ? { expiresInMs: input.expiresInMs } : {}),
      })
    })
  }
  if (parts.length === 2 && parts[1] === "transfer-ownership" && request.method === "POST") {
    return executeBody(request, (actor, body) => {
      const input = parse(body)
      if (typeof input.actorID !== "string" || input.actorID === "") {
        throw new Organization.Error("organization_bad_request", "actorID is required")
      }
      return Organization.transferOwnership(actor, organizationID, input.actorID)
    })
  }
  if (parts.length === 2 && parts[1] === "permissions" && request.method === "GET") {
    return execute(request, (actor) => PermissionCenter.policy(actor, organizationID))
  }
  if (parts.length === 4 && parts[1] === "permissions" && parts[2] === "roles" && request.method === "PUT") {
    return executeBody(request, (actor, body) => {
      const input = parse(body)
      const permissionInput = input.permissions
      if (
        !isPermissionRole(parts[3]) ||
        !Array.isArray(permissionInput) ||
        !permissionInput.every((value): value is string => typeof value === "string") ||
        typeof input.expectedVersion !== "number"
      ) {
        throw new PermissionCenter.Error(
          "permission_bad_request",
          "role, permissions, and expectedVersion are required",
        )
      }
      return PermissionCenter.replaceRolePermissions(actor, organizationID, parts[3], {
        permissions: permissionInput,
        expectedVersion: input.expectedVersion,
      })
    })
  }
  return Effect.succeed(notFound())
}

function invitationDecision(request: HttpServerRequest.HttpServerRequest, decision: "accept" | "reject") {
  return executeBody(request, (actor, body) => {
    const input = parse(body)
    if (typeof input.token !== "string") {
      throw new Organization.Error("organization_bad_request", "Invitation token is required")
    }
    return decision === "accept"
      ? Organization.acceptInvitation(actor, input.token)
      : Organization.rejectInvitation(actor, input.token)
  })
}

function execute(
  request: HttpServerRequest.HttpServerRequest,
  operation: (actor: ActorContext) => Promise<unknown>,
) {
  if (!SaasIdentity.enabled()) return Effect.succeed(notFound())
  return Effect.tryPromise(async () => {
    const session = await SaasIdentity.authenticate(new Headers(request.headers as HeadersInit))
    if (session === undefined) return unauthorized()
    try {
      return HttpServerResponse.jsonUnsafe(await operation(session.actor))
    } catch (error) {
      return errorResponse(error)
    }
  }).pipe(
    Effect.catch(() =>
      Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "organization_internal_error" }, { status: 500 })),
    ),
  )
}

function executeBody(
  request: HttpServerRequest.HttpServerRequest,
  operation: (actor: ActorContext, body: string) => Promise<unknown>,
) {
  return Effect.gen(function* () {
    const body = yield* request.text
    return yield* execute(request, (actor) => operation(actor, body))
  }).pipe(
    Effect.catch(() =>
      Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "organization_bad_request" }, { status: 400 })),
    ),
  )
}

function parse(body: string): Record<string, unknown> {
  const value = JSON.parse(body) as unknown
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Organization.Error("organization_bad_request", "JSON object required")
  }
  return value as Record<string, unknown>
}

function isInvitationRole(value: unknown): value is Organization.InvitationRole {
  return value === "admin" || value === "member" || value === "viewer" || value === "billing_admin"
}

function isPermissionRole(value: unknown): value is PermissionCenter.EditableRole {
  return value === "admin" || value === "member" || value === "viewer" || value === "billing_admin"
}

function errorResponse(error: unknown) {
  if (error instanceof PermissionCenter.Error) {
    const status =
      error.code === "permission_bad_request" ? 400 : error.code === "permission_conflict" ? 409 : 403
    return HttpServerResponse.jsonUnsafe({ error: error.code, message: error.message }, { status })
  }
  if (!(error instanceof Organization.Error)) {
    return HttpServerResponse.jsonUnsafe({ error: "organization_internal_error" }, { status: 500 })
  }
  const status =
    error.code === "organization_bad_request" || error.code === "invitation_invalid"
      ? 400
      : error.code === "organization_forbidden"
        ? 403
        : error.code === "organization_not_found"
          ? 404
          : error.code === "invitation_expired"
            ? 410
            : 409
  return HttpServerResponse.jsonUnsafe({ error: error.code, message: error.message }, { status })
}

function unauthorized() {
  return HttpServerResponse.jsonUnsafe({ error: "authentication_required" }, { status: 401 })
}

function notFound() {
  return HttpServerResponse.jsonUnsafe({ error: "not_found" }, { status: 404 })
}

function methodNotAllowed() {
  return HttpServerResponse.jsonUnsafe({ error: "method_not_allowed" }, { status: 405 })
}
