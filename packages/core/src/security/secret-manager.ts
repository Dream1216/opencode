import fs from "node:fs/promises"

export type SecretManagerResolveOptions = {
  readonly refresh?: boolean
}

export type SecretManager = {
  readonly resolve: (reference: string, options?: SecretManagerResolveOptions) => Promise<string>
  readonly invalidate: (reference?: string) => void
}

export function createSecretManager(
  input: {
    readonly env?: NodeJS.ProcessEnv
    readonly cacheTtlMs?: number
    readonly fetchAwsSecret?: (region: string, secretID: string) => Promise<string>
  } = {},
): SecretManager {
  const env = input.env ?? process.env
  const ttl = Math.max(0, input.cacheTtlMs ?? 60_000)
  const cache = new Map<string, { readonly value: string; readonly expiresAt: number }>()
  const aws = input.fetchAwsSecret ?? fetchAwsSecret
  return {
    resolve: async (reference, options = {}) => {
      const cached = cache.get(reference)
      if (!options.refresh && cached !== undefined && cached.expiresAt > Date.now()) return cached.value
      const value = await resolveReference(reference, env, aws)
      if (value === "") throw new Error(`Secret Manager returned an empty ${scheme(reference)} secret`)
      cache.set(reference, { value, expiresAt: Date.now() + ttl })
      return value
    },
    invalidate: (reference) => {
      if (reference === undefined) cache.clear()
      else cache.delete(reference)
    },
  }
}

async function resolveReference(
  reference: string,
  env: NodeJS.ProcessEnv,
  aws: (region: string, secretID: string) => Promise<string>,
) {
  if (reference.startsWith("env://")) {
    const key = reference.slice("env://".length)
    if (!/^[A-Z_][A-Z0-9_]*$/i.test(key)) throw new Error("Invalid env Secret Manager reference")
    const value = env[key]?.trim()
    if (value === undefined || value === "") throw new Error("Referenced environment secret is not configured")
    return value
  }
  if (reference.startsWith("file://")) {
    const stat = await fs.stat(new URL(reference))
    if (!stat.isFile() || stat.size > 1024 * 1024) throw new Error("Mounted secret file is invalid or too large")
    return (await fs.readFile(new URL(reference), "utf8")).trim()
  }
  if (reference.startsWith("aws-sm://")) {
    const parsed = new URL(reference)
    const region = parsed.hostname
    const secretID = decodeURIComponent(parsed.pathname.slice(1))
    if (region === "" || secretID === "") throw new Error("Invalid AWS Secrets Manager reference")
    const raw = await aws(region, secretID)
    const field = decodeURIComponent(parsed.hash.slice(1))
    if (field === "") return raw.trim()
    const value = (JSON.parse(raw) as Record<string, unknown>)[field]
    if (typeof value !== "string") throw new Error("AWS Secrets Manager JSON field is missing or not a string")
    return value.trim()
  }
  throw new Error("Unsupported Secret Manager reference scheme")
}

async function fetchAwsSecret(region: string, secretID: string) {
  const AWS = await import("@aws-sdk/client-secrets-manager")
  const client = new AWS.SecretsManagerClient({ region })
  try {
    const result = await client.send(new AWS.GetSecretValueCommand({ SecretId: secretID }))
    if (result.SecretString !== undefined) return result.SecretString
    if (result.SecretBinary !== undefined) return Buffer.from(result.SecretBinary).toString("utf8")
    throw new Error("AWS Secrets Manager returned no secret value")
  } finally {
    client.destroy()
  }
}

function scheme(reference: string) {
  const index = reference.indexOf("://")
  return index < 0 ? "unknown" : reference.slice(0, index)
}

export * as SecretManager from "./secret-manager"
