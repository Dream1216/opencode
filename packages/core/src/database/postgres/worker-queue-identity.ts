import { createPublicKey, timingSafeEqual, verify, type JsonWebKey } from "node:crypto"
import {
  createSecretManager,
  type SecretManager,
} from "../../security/secret-manager"
import {
  signRequest,
  type WorkerQueueIdentityProvider,
  type WorkerQueueIdentityRequest,
} from "./worker-queue-request-signing"

export type WorkerQueueIdentity = {
  readonly actorID: string
  readonly provider: WorkerQueueIdentityProvider
  readonly keyID?: string
}

export type WorkerQueueIdentityAdapter = {
  readonly providers: readonly WorkerQueueIdentityProvider[]
  readonly authenticate: (request: WorkerQueueIdentityRequest) => Promise<WorkerQueueIdentity>
}

export class WorkerQueueIdentityError extends Error {
  constructor(message = "Worker queue identity authentication failed") {
    super(message)
    this.name = "WorkerQueueIdentityError"
  }
}

export async function identityAdapterFromEnv(
  env: NodeJS.ProcessEnv,
  providedSecretManager?: SecretManager,
): Promise<WorkerQueueIdentityAdapter> {
  const mode = (env.OPENCODE_WORKER_QUEUE_IDENTITY_MODE ?? "hmac").trim()
  if (!["hmac", "oidc", "better-auth", "hybrid"].includes(mode)) {
    throw new Error("OPENCODE_WORKER_QUEUE_IDENTITY_MODE must be hmac, oidc, better-auth, or hybrid")
  }
  const secretManager =
    providedSecretManager ??
    createSecretManager({
      env,
      cacheTtlMs: integer(env.OPENCODE_SECRET_MANAGER_CACHE_TTL_MS, 60_000, 0, 3_600_000),
    })
  const legacyKeys = parseLegacyKeys(env.OPENCODE_WORKER_QUEUE_ADMIN_KEYS)
  const keyringRef = optional(env.OPENCODE_WORKER_QUEUE_ADMIN_KEYRING_REF)
  const oidc = makeOidcVerifier(env)
  const betterAuth = makeBetterAuthVerifier(env, secretManager)
  const providers: WorkerQueueIdentityProvider[] = []
  if (mode === "hmac" || (mode === "hybrid" && (legacyKeys.size > 0 || keyringRef !== undefined))) {
    if (legacyKeys.size === 0 && keyringRef === undefined) {
      throw new Error("HMAC identity mode requires OPENCODE_WORKER_QUEUE_ADMIN_KEYS or OPENCODE_WORKER_QUEUE_ADMIN_KEYRING_REF")
    }
    providers.push("hmac")
  }
  if (mode === "oidc" || (mode === "hybrid" && oidc !== undefined)) {
    if (oidc === undefined) throw new Error("OIDC identity mode requires issuer and audience")
    providers.push("oidc")
  }
  if (mode === "better-auth" || (mode === "hybrid" && betterAuth !== undefined)) {
    if (betterAuth === undefined) throw new Error("Better Auth identity mode requires OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL")
    providers.push("better-auth")
  }
  if (providers.length === 0) throw new Error("Worker queue identity adapter has no configured providers")

  const authenticateHmac = async (request: WorkerQueueIdentityRequest) => {
    if (!providers.includes("hmac") || request.actorID === "" || !/^[a-f0-9]{64}$/i.test(request.signature)) {
      throw new WorkerQueueIdentityError()
    }
    if (keyringRef === undefined) {
      if (request.keyID !== undefined) throw new WorkerQueueIdentityError()
      const secret = legacyKeys.get(request.actorID)
      if (secret === undefined || !equal(signRequest({ ...request, secret }), request.signature)) {
        throw new WorkerQueueIdentityError()
      }
      return { actorID: request.actorID, provider: "hmac" as const }
    }
    const verifyKeyring = async (refresh: boolean) => {
      const manifest = parseKeyring(await secretManager.resolve(keyringRef, { refresh }))
      const actor = manifest.actors[request.actorID]
      const keyID = request.keyID
      if (actor === undefined || keyID === undefined) return undefined
      const key = actor.keys[keyID]
      if (
        key === undefined ||
        key.status === "revoked" ||
        (key.status === "retiring" && (key.notAfter === undefined || key.notAfter <= Date.now())) ||
        (key.status === "active" && actor.activeKeyID !== keyID)
      ) {
        return undefined
      }
      const secret = await secretManager.resolve(key.secretRef, { refresh })
      return equal(signRequest({ ...request, secret }), request.signature)
        ? { actorID: request.actorID, provider: "hmac" as const, keyID }
        : undefined
    }
    return (await verifyKeyring(false)) ?? (await verifyKeyring(true)) ?? Promise.reject(new WorkerQueueIdentityError())
  }

  return {
    providers,
    authenticate: async (request) => {
      let identity: WorkerQueueIdentity
      const requested = request.identityProvider
      if (requested === "oidc" || (requested === undefined && request.identityToken?.split(".").length === 3)) {
        if (!providers.includes("oidc") || oidc === undefined || request.identityToken === undefined) {
          throw new WorkerQueueIdentityError()
        }
        identity = { actorID: await oidc(request.identityToken), provider: "oidc" }
      } else if (
        requested === "better-auth" ||
        (requested === undefined && (request.identityToken !== undefined || request.sessionCookie !== undefined))
      ) {
        if (!providers.includes("better-auth") || betterAuth === undefined) throw new WorkerQueueIdentityError()
        identity = {
          actorID: await betterAuth(request.identityToken, request.sessionCookie),
          provider: "better-auth",
        }
      } else {
        identity = await authenticateHmac(request)
      }
      if (request.actorID !== "" && request.actorID !== identity.actorID) throw new WorkerQueueIdentityError()
      return identity
    },
  }
}

type KeyringManifest = {
  readonly version: 1
  readonly actors: Readonly<
    Record<
      string,
      {
        readonly activeKeyID: string
        readonly keys: Readonly<
          Record<
            string,
            {
              readonly secretRef: string
              readonly status: "active" | "retiring" | "revoked"
              readonly notAfter?: number
            }
          >
        >
      }
    >
  >
}

function parseKeyring(raw: string): KeyringManifest {
  const value = JSON.parse(raw) as unknown
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new WorkerQueueIdentityError()
  const record = value as Record<string, unknown>
  if (record.version !== 1 || record.actors === null || typeof record.actors !== "object" || Array.isArray(record.actors)) {
    throw new WorkerQueueIdentityError()
  }
  for (const actor of Object.values(record.actors as Record<string, unknown>)) {
    if (actor === null || typeof actor !== "object" || Array.isArray(actor)) throw new WorkerQueueIdentityError()
    const entry = actor as Record<string, unknown>
    if (typeof entry.activeKeyID !== "string" || entry.keys === null || typeof entry.keys !== "object") {
      throw new WorkerQueueIdentityError()
    }
    const active = (entry.keys as Record<string, unknown>)[entry.activeKeyID]
    if (
      active === null ||
      typeof active !== "object" ||
      (active as Record<string, unknown>).status !== "active"
    ) {
      throw new WorkerQueueIdentityError()
    }
    for (const key of Object.values(entry.keys as Record<string, unknown>)) {
      if (key === null || typeof key !== "object" || Array.isArray(key)) throw new WorkerQueueIdentityError()
      const item = key as Record<string, unknown>
      if (
        typeof item.secretRef !== "string" ||
        !["active", "retiring", "revoked"].includes(String(item.status)) ||
        (item.notAfter !== undefined && !Number.isSafeInteger(item.notAfter))
      ) {
        throw new WorkerQueueIdentityError()
      }
    }
  }
  return value as KeyringManifest
}

function parseLegacyKeys(raw: string | undefined) {
  const keys = new Map<string, string>()
  if (optional(raw) === undefined) return keys
  const value = JSON.parse(raw!) as unknown
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("OPENCODE_WORKER_QUEUE_ADMIN_KEYS must be a JSON object")
  }
  for (const [actor, secret] of Object.entries(value as Record<string, unknown>)) {
    if (typeof secret !== "string" || secret.length < 16) {
      throw new Error(`Worker queue management key for ${actor} must contain at least 16 characters`)
    }
    keys.set(actor, secret)
  }
  return keys
}

function makeOidcVerifier(env: NodeJS.ProcessEnv) {
  const issuer = optional(env.OPENCODE_WORKER_QUEUE_OIDC_ISSUER)
  const audience = optional(env.OPENCODE_WORKER_QUEUE_OIDC_AUDIENCE)
  if (issuer === undefined && audience === undefined) return undefined
  if (issuer === undefined || audience === undefined) throw new Error("OIDC issuer and audience must be configured together")
  const normalizedIssuer = issuer.replace(/\/+$/, "")
  assertRemoteURL(normalizedIssuer)
  const explicitJwks = optional(env.OPENCODE_WORKER_QUEUE_OIDC_JWKS_URL)
  if (explicitJwks !== undefined) assertRemoteURL(explicitJwks)
  const actorClaim = optional(env.OPENCODE_WORKER_QUEUE_OIDC_ACTOR_CLAIM) ?? "sub"
  const algorithms = new Set<string>(
    (optional(env.OPENCODE_WORKER_QUEUE_OIDC_ALGORITHMS) ?? "RS256,ES256")
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value === "RS256" || value === "ES256"),
  )
  if (algorithms.size === 0) throw new Error("OIDC requires at least one supported algorithm")
  const timeoutMs = integer(env.OPENCODE_WORKER_QUEUE_IDENTITY_TIMEOUT_MS, 5_000, 250, 60_000)
  const cacheTtlMs = integer(env.OPENCODE_WORKER_QUEUE_OIDC_CACHE_TTL_MS, 300_000, 1_000, 3_600_000)
  let jwksURL = explicitJwks
  let keys: { readonly expiresAt: number; readonly values: readonly Record<string, unknown>[] } | undefined

  const loadKeys = async (refresh = false) => {
    if (!refresh && keys !== undefined && keys.expiresAt > Date.now()) return keys.values
    if (jwksURL === undefined) {
      const discovery = await fetchJson(
        `${normalizedIssuer}/.well-known/openid-configuration`,
        timeoutMs,
      )
      if (discovery.issuer !== normalizedIssuer || typeof discovery.jwks_uri !== "string") {
        throw new WorkerQueueIdentityError()
      }
      assertRemoteURL(discovery.jwks_uri)
      jwksURL = discovery.jwks_uri
    }
    const jwks = await fetchJson(jwksURL, timeoutMs)
    if (!Array.isArray(jwks.keys)) throw new WorkerQueueIdentityError()
    keys = {
      expiresAt: Date.now() + cacheTtlMs,
      values: jwks.keys.filter((key): key is Record<string, unknown> => key !== null && typeof key === "object"),
    }
    return keys.values
  }

  return async (token: string) => {
    try {
      const parts = token.split(".")
      if (parts.length !== 3) throw new WorkerQueueIdentityError()
      const header = decodeJson(parts[0]!)
      const payload = decodeJson(parts[1]!)
      const algorithm = typeof header.alg === "string" ? header.alg : ""
      const kid = typeof header.kid === "string" ? header.kid : ""
      if (!algorithms.has(algorithm) || kid === "") throw new WorkerQueueIdentityError()
      let key = (await loadKeys()).find((value) => value.kid === kid && (value.alg === undefined || value.alg === algorithm))
      if (key === undefined) {
        key = (await loadKeys(true)).find((value) => value.kid === kid && (value.alg === undefined || value.alg === algorithm))
      }
      if (key === undefined) throw new WorkerQueueIdentityError()
      const publicKey = createPublicKey({ key: key as JsonWebKey, format: "jwk" })
      const data = Buffer.from(`${parts[0]}.${parts[1]}`)
      const signature = Buffer.from(parts[2]!, "base64url")
      const valid =
        algorithm === "RS256"
          ? verify("RSA-SHA256", data, publicKey, signature)
          : verify("sha256", data, { key: publicKey, dsaEncoding: "ieee-p1363" }, signature)
      if (!valid) throw new WorkerQueueIdentityError()
      const now = Math.floor(Date.now() / 1000)
      if (
        payload.iss !== normalizedIssuer ||
        !audienceMatches(payload.aud, audience) ||
        typeof payload.exp !== "number" ||
        payload.exp <= now ||
        (typeof payload.nbf === "number" && payload.nbf > now + 60) ||
        (typeof payload.iat === "number" && payload.iat > now + 60)
      ) {
        throw new WorkerQueueIdentityError()
      }
      const actorID = readClaim(payload, actorClaim)
      if (typeof actorID !== "string" || actorID === "") throw new WorkerQueueIdentityError()
      return actorID
    } catch (error) {
      if (error instanceof WorkerQueueIdentityError) throw error
      throw new WorkerQueueIdentityError()
    }
  }
}

function makeBetterAuthVerifier(env: NodeJS.ProcessEnv, secretManager: SecretManager) {
  const base = optional(env.OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL)
  if (base === undefined) return undefined
  const endpoint = new URL(
    optional(env.OPENCODE_WORKER_QUEUE_BETTER_AUTH_SESSION_PATH) ?? "/api/auth/get-session",
    base,
  )
  assertRemoteURL(endpoint.toString())
  const timeoutMs = integer(env.OPENCODE_WORKER_QUEUE_IDENTITY_TIMEOUT_MS, 5_000, 250, 60_000)
  const serviceTokenRef = optional(env.OPENCODE_WORKER_QUEUE_BETTER_AUTH_SERVICE_TOKEN_REF)
  return async (token: string | undefined, cookie: string | undefined) => {
    if (token === undefined && cookie === undefined) throw new WorkerQueueIdentityError()
    const headers: Record<string, string> = { accept: "application/json" }
    if (token !== undefined) headers.authorization = `Bearer ${token}`
    if (cookie !== undefined) headers.cookie = cookie
    if (serviceTokenRef !== undefined) {
      headers["x-opencode-service-token"] = await secretManager.resolve(serviceTokenRef)
    }
    try {
      const response = await fetch(endpoint, { headers, signal: AbortSignal.timeout(timeoutMs) })
      if (response.status !== 200) throw new WorkerQueueIdentityError()
      const length = Number(response.headers.get("content-length") ?? 0)
      if (Number.isFinite(length) && length > 64 * 1024) throw new WorkerQueueIdentityError()
      const raw = await response.text()
      if (Buffer.byteLength(raw) > 64 * 1024) throw new WorkerQueueIdentityError()
      const session = JSON.parse(raw) as Record<string, unknown>
      const user = session.user
      if (user === null || typeof user !== "object" || typeof (user as Record<string, unknown>).id !== "string") {
        throw new WorkerQueueIdentityError()
      }
      const data = session.session
      if (data !== null && typeof data === "object" && (data as Record<string, unknown>).expiresAt !== undefined) {
        const expiresAt = new Date(String((data as Record<string, unknown>).expiresAt)).getTime()
        if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) throw new WorkerQueueIdentityError()
      }
      return String((user as Record<string, unknown>).id)
    } catch (error) {
      if (error instanceof WorkerQueueIdentityError) throw error
      throw new WorkerQueueIdentityError()
    }
  }
}

async function fetchJson(url: string, timeoutMs: number) {
  const response = await fetch(url, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(timeoutMs) })
  if (response.status !== 200) throw new WorkerQueueIdentityError()
  const raw = await response.text()
  if (Buffer.byteLength(raw) > 1024 * 1024) throw new WorkerQueueIdentityError()
  const value = JSON.parse(raw) as unknown
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new WorkerQueueIdentityError()
  return value as Record<string, unknown>
}

function decodeJson(value: string) {
  const decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as unknown
  if (decoded === null || typeof decoded !== "object" || Array.isArray(decoded)) throw new WorkerQueueIdentityError()
  return decoded as Record<string, unknown>
}

function readClaim(payload: Record<string, unknown>, path: string) {
  return path.split(".").reduce<unknown>(
    (value, part) => (value !== null && typeof value === "object" ? (value as Record<string, unknown>)[part] : undefined),
    payload,
  )
}

function audienceMatches(value: unknown, expected: string) {
  return value === expected || (Array.isArray(value) && value.includes(expected))
}

function assertRemoteURL(value: string) {
  const url = new URL(value)
  const loopback = url.hostname === "127.0.0.1" || url.hostname === "::1" || url.hostname === "localhost"
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) {
    throw new Error("Identity endpoints must use HTTPS unless they bind to loopback")
  }
}

function equal(left: string, right: string) {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.length === b.length && timingSafeEqual(a, b)
}

function optional(value: string | undefined) {
  const result = value?.trim()
  return result === undefined || result === "" ? undefined : result
}

function integer(value: string | undefined, fallback: number, min: number, max: number) {
  if (value === undefined) return fallback
  const result = Number(value)
  if (!Number.isSafeInteger(result) || result < min || result > max) {
    throw new Error(`Expected an integer between ${min} and ${max}`)
  }
  return result
}

export * as WorkerQueueIdentity from "./worker-queue-identity"
