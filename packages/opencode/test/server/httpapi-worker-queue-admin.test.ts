import { NodeHttpServer } from "@effect/platform-node"
import { describe, expect } from "bun:test"
import { Context, Effect, Layer, Option } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import * as WorkerQueueAdmin from "@opencode-ai/core/database/postgres/worker-queue-admin"
import { ServerAuth } from "../../src/server/auth"
import { WorkerQueueAdminApi } from "../../src/server/routes/instance/httpapi/groups/worker-queue-admin"
import { workerQueueAdminHandlers } from "../../src/server/routes/instance/httpapi/handlers/worker-queue-admin"
import { authorizationLayer } from "../../src/server/routes/instance/httpapi/middleware/authorization"
import { schemaErrorLayer } from "../../src/server/routes/instance/httpapi/middleware/schema-error"
import { testEffect } from "../lib/effect"

const principal: WorkerQueueAdmin.Principal = {
  tenantID: "tenant-http",
  teamID: "team-http",
  actorID: "actor-http",
  tenantRole: "member",
  teamRole: "viewer",
  identityProvider: "hmac",
}

const service = WorkerQueueAdmin.Service.of({
  enabled: true,
  authenticate: (request) =>
    request.signature === "limited"
      ? Effect.die(new WorkerQueueAdmin.RateLimitError(17))
      : request.signature === "signed" || request.identityToken === "oidc-token"
      ? Effect.succeed(principal)
      : Effect.die(new WorkerQueueAdmin.AuthenticationError()),
  readiness: () => Effect.succeed({ ready: true, degraded: false } as never),
  prometheus: () => Effect.succeed("opencode_worker_queue_ready{tenant_id=\"tenant-http\",team_id=\"team-http\"} 1\n"),
  recoverable: () => Effect.succeed([]),
  requestRequeue: () => Effect.die(new WorkerQueueAdmin.ConflictError("not exercised")),
  approve: () => Effect.die(new WorkerQueueAdmin.ConflictError("not exercised")),
  revokeApproval: () => Effect.die(new WorkerQueueAdmin.ConflictError("not exercised")),
  expireApprovals: () => Effect.succeed({ expired: 0 }),
  breakGlassRequeue: (_principal, input) =>
    input.token === "break-glass-token"
      ? Effect.succeed({ runID: input.runID, status: "pending" } as never)
      : Effect.die(new WorkerQueueAdmin.AuthenticationError()),
  action: () => Effect.die(new WorkerQueueAdmin.ConflictError("not exercised")),
})

const apiLayer = HttpRouter.serve(
  HttpApiBuilder.layer(WorkerQueueAdminApi).pipe(
    Layer.provide(workerQueueAdminHandlers),
    Layer.provide([authorizationLayer, schemaErrorLayer]),
    // Raw HttpApi routes expose an opaque handler context at the request boundary.
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    HttpRouter.provideRequest(Layer.succeedContext(Context.empty() as Context.Context<unknown>)),
  ),
  { disableListenLog: true, disableLogger: true },
).pipe(
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provide(Layer.succeed(WorkerQueueAdmin.Service, service)),
  Layer.provide(ServerAuth.Config.configLayer({ password: Option.none(), username: "opencode" })),
)
const it = testEffect(apiLayer)

const signedHeaders = {
  "x-opencode-actor-id": principal.actorID,
  "x-opencode-request-timestamp": String(Date.now()),
  "x-opencode-request-nonce": "http-route-nonce-0001",
  "x-opencode-request-signature": "signed",
}

describe("worker queue management HttpApi", () => {
  it.live("rejects a request without an actor signature", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/experimental/worker-queue/readiness").pipe(HttpClient.execute)
      expect(response.status).toBe(401)
    }),
  )

  it.live("serves signed readiness", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/experimental/worker-queue/readiness").pipe(
        HttpClientRequest.setHeaders(signedHeaders),
        HttpClient.execute,
      )
      expect(response.status).toBe(200)
      expect(yield* response.json).toEqual({ ready: true, degraded: false })
    }),
  )

  it.live("serves Prometheus text without actor labels", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/experimental/worker-queue/metrics").pipe(
        HttpClientRequest.setHeaders(signedHeaders),
        HttpClient.execute,
      )
      const body = yield* response.text
      expect(response.status).toBe(200)
      expect(response.headers["content-type"]).toContain("text/plain")
      expect(body).toContain("opencode_worker_queue_ready")
      expect(body).not.toContain(principal.actorID)
    }),
  )

  it.live("forwards an external identity token to the identity adapter", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/experimental/worker-queue/readiness").pipe(
        HttpClientRequest.setHeaders({
          "x-opencode-identity-provider": "oidc",
          "x-opencode-identity-token": "oidc-token",
          "x-opencode-request-timestamp": String(Date.now()),
          "x-opencode-request-nonce": "http-identity-nonce-0001",
        }),
        HttpClient.execute,
      )
      expect(response.status).toBe(200)
    }),
  )

  it.live("maps distributed rate limiting to 429", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/experimental/worker-queue/readiness").pipe(
        HttpClientRequest.setHeaders({ ...signedHeaders, "x-opencode-request-signature": "limited" }),
        HttpClient.execute,
      )
      expect(response.status).toBe(429)
      expect(response.headers["retry-after"]).toBe("17")
    }),
  )

  it.live("forwards break-glass payload and second factor", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.post("/experimental/worker-queue/actions/break-glass/requeue").pipe(
        HttpClientRequest.setHeaders({
          ...signedHeaders,
          "x-opencode-break-glass-token": "break-glass-token",
        }),
        HttpClientRequest.bodyJsonUnsafe({
          runID: "ses_break_glass",
          expectedGeneration: 1,
          expectedClaimToken: 2,
          incidentID: "incident-http-001",
          reason: "Emergency HTTP route validation incident",
        }),
        HttpClient.execute,
      )
      expect(response.status).toBe(200)
    }),
  )
})
