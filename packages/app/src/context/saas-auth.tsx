import { createContext, createResource, type ParentProps, Show, useContext } from "solid-js"
import { SaasAuthPage } from "@/pages/saas-auth"

export type AuthSession = {
  readonly user: {
    readonly id: string
    readonly name: string
    readonly email: string
    readonly role?: string
  }
  readonly session: {
    readonly id: string
    readonly expiresAt: string
  }
}

export type AuthState =
  | { readonly mode: "disabled" }
  | { readonly mode: "anonymous" }
  | { readonly mode: "authenticated"; readonly session: AuthSession }

type AuthContextValue = {
  readonly state: () => AuthState
}

const AuthContext = createContext<AuthContextValue>()

export function useSaasAuth() {
  const value = useContext(AuthContext)
  if (value === undefined) throw new Error("useSaasAuth must be used within SaasAuthGate")
  return value
}

type Fetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>

export async function readAuthState(fetcher: Fetcher = fetch): Promise<AuthState> {
  const response = await fetcher("/api/auth/get-session", {
    credentials: "include",
    headers: { accept: "application/json" },
  })
  if (response.status === 404) return { mode: "disabled" }
  if (!response.ok) throw new Error(authResponseError(response))
  if (!response.headers.get("content-type")?.includes("application/json")) return { mode: "disabled" }
  const value = (await response.json()) as AuthSession | null
  if (!value?.user || !value.session) return { mode: "anonymous" }
  return { mode: "authenticated", session: value }
}

export async function signOutAuthSession(fetcher: Fetcher = fetch): Promise<void> {
  const response = await fetcher("/api/auth/sign-out", {
    method: "POST",
    credentials: "include",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: "{}",
  })
  if (response.ok) return

  const detail = await response
    .json()
    .then((value: unknown) => {
      if (typeof value !== "object" || value === null) return undefined
      const body = value as { readonly message?: unknown; readonly error?: unknown; readonly code?: unknown }
      return [body.message, body.error, body.code].find((item): item is string => typeof item === "string")
    })
    .catch(() => undefined)
  throw new Error(detail === undefined ? `Sign out failed (${response.status})` : `Sign out failed: ${detail}`)
}

export function SaasAuthGate(props: ParentProps) {
  const [state, actions] = createResource(() => readAuthState())
  const ready = () => {
    if (state.error !== undefined) return false
    const value = state()
    return value?.mode === "disabled" || value?.mode === "authenticated"
  }

  return (
    <>
      <Show when={ready()}>
        <AuthContext.Provider value={{ state: () => state()! }}>{props.children}</AuthContext.Provider>
      </Show>
      <Show when={!ready()}>
        <SaasAuthPage
          loading={state.loading}
          error={state.error instanceof Error ? state.error.message : undefined}
          onAuthenticated={() => actions.refetch()}
          onRetry={() => actions.refetch()}
        />
      </Show>
    </>
  )
}

function authResponseError(response: Response) {
  if (response.status !== 429) return `Identity service returned ${response.status}`
  const seconds = Number.parseInt(response.headers.get("retry-after") ?? "", 10)
  if (Number.isFinite(seconds) && seconds > 0) {
    return `Authentication is temporarily rate limited. Try again in ${seconds} seconds.`
  }
  return "Authentication is temporarily rate limited. Try again shortly."
}
