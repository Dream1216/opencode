import { SaasIdentity } from "@opencode-ai/core/identity/saas-auth"
import { Effect } from "effect"
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"

export const saasAuthRoutes = HttpRouter.use((router) =>
  router.add("*", "/api/auth/*", (request) => {
    if (!SaasIdentity.enabled()) {
      return Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "saas_mode_disabled" }, { status: 404 }))
    }
    return forward(request).pipe(
      Effect.catch(() =>
        Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "identity_service_unavailable" }, { status: 503 })),
      ),
    )
  }),
)

function forward(request: HttpServerRequest.HttpServerRequest) {
  return Effect.gen(function* () {
    const value = SaasIdentity.requireConfig()
    const headers = new Headers(request.headers as HeadersInit)
    const body =
      request.method === "GET" || request.method === "HEAD" ? undefined : yield* request.text
    const response = yield* Effect.tryPromise(() =>
      SaasIdentity.handle(
        new Request(new URL(request.url, value.baseURL), {
          method: request.method,
          headers,
          body,
        }),
      ),
    )
    return HttpServerResponse.raw(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())), {
      status: response.status,
      headers: response.headers,
    })
  })
}
