import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import { OpenCode, type OpenCodeClient } from "@opencode-ai/client/promise"
import type { ServerConnection } from "@/context/server"
import { decode64 } from "@/utils/base64"

const ORGANIZATION_STORAGE_KEY = "opencode.saas.organization"

export function organizationIDFromStorage(storage?: Pick<Storage, "getItem">) {
  try {
    return storage?.getItem(ORGANIZATION_STORAGE_KEY)?.trim() || undefined
  } catch {
    return
  }
}

export function withOrganizationFetch(
  fetcher: typeof globalThis.fetch,
  storage: Pick<Storage, "getItem"> | undefined = typeof localStorage === "undefined" ? undefined : localStorage,
): typeof globalThis.fetch {
  return ((input, init) => {
    const organizationID = organizationIDFromStorage(storage)
    if (!organizationID) return fetcher(input, init)
    const headers = new Headers(init?.headers)
    headers.set("X-OpenCode-Organization-ID", organizationID)
    return fetcher(input, { ...init, headers })
  }) as typeof globalThis.fetch
}

export function authTokenFromCredentials(input: { username?: string; password: string }) {
  return btoa(`${input.username ?? "opencode"}:${input.password}`)
}

export function authFromToken(token: string | null) {
  const decoded = decode64(token ?? undefined)
  if (!decoded) return
  const separator = decoded.indexOf(":")
  if (separator === -1) return
  return {
    username: decoded.slice(0, separator) || "opencode",
    password: decoded.slice(separator + 1),
  }
}

export function createSdkForServer({
  server,
  ...config
}: Omit<NonNullable<Parameters<typeof createOpencodeClient>[0]>, "baseUrl"> & {
  server: ServerConnection.HttpBase
}) {
  const auth = (() => {
    if (!server.password) return
    return {
      Authorization: `Basic ${authTokenFromCredentials({ username: server.username, password: server.password })}`,
    }
  })()

  return createOpencodeClient({
    ...config,
    fetch: withOrganizationFetch(config.fetch ?? globalThis.fetch),
    headers: {
      ...(config.headers instanceof Headers ? Object.fromEntries(config.headers.entries()) : config.headers),
      ...auth,
    },
    baseUrl: server.url,
  })
}

export function createApiForServer(input: {
  server: ServerConnection.HttpBase
  fetch?: typeof globalThis.fetch
}): OpenCodeClient {
  return OpenCode.make({
    baseUrl: input.server.url,
    fetch: withOrganizationFetch(input.fetch ?? globalThis.fetch),
    headers: input.server.password
      ? {
          Authorization: `Basic ${authTokenFromCredentials({
            username: input.server.username,
            password: input.server.password,
          })}`,
        }
      : undefined,
  })
}

export type ServerApi = OpenCodeClient
