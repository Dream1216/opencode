export * as SaasIdentity from "./saas-auth"

import { applyMigrations } from "../database/postgres/migration"
import { makeClient } from "../database/postgres/client"
import { platformRole, type IdentitySession } from "../identity"

export type Config = {
  readonly enabled: boolean
  readonly databaseURL?: string
  readonly baseURL?: string
  readonly secret?: string
  readonly trustedOrigins: readonly string[]
  readonly autoMigrate: boolean
}

type Runtime = {
  readonly handler: (request: Request) => Promise<Response>
  readonly getSession: (headers: Headers) => Promise<IdentitySession | undefined>
  readonly close: () => Promise<void>
}

export type BootstrapResult =
  | {
      readonly status: "disabled"
      readonly applied: readonly string[]
      readonly skipped: readonly string[]
    }
  | {
      readonly status: "ready"
      readonly applied: readonly string[]
      readonly skipped: readonly string[]
    }

let runtime: Promise<Runtime> | undefined
let bootstrapRuntime:
  | {
      readonly key: string
      readonly promise: Promise<BootstrapResult>
    }
  | undefined

export function config(env: NodeJS.ProcessEnv = process.env): Config {
  const baseURL = optional(env.OPENCODE_AUTH_URL ?? env.BETTER_AUTH_URL)
  const origins = new Set(
    (env.OPENCODE_AUTH_TRUSTED_ORIGINS ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value !== ""),
  )
  if (baseURL !== undefined) origins.add(new URL(baseURL).origin)
  return {
    enabled: env.OPENCODE_SAAS_MODE === "true",
    databaseURL: optional(env.OPENCODE_DATABASE_URL),
    baseURL,
    secret: optional(env.OPENCODE_AUTH_SECRET ?? env.BETTER_AUTH_SECRET),
    trustedOrigins: [...origins],
    autoMigrate: env.OPENCODE_SAAS_AUTO_MIGRATE !== "false",
  }
}

export function requireConfig(env: NodeJS.ProcessEnv = process.env): Config & {
  readonly databaseURL: string
  readonly baseURL: string
  readonly secret: string
} {
  const value = config(env)
  if (!value.enabled) throw new Error("OPENCODE_SAAS_MODE must be true")
  if (value.databaseURL === undefined) throw new Error("OPENCODE_DATABASE_URL is required in SaaS mode")
  if (value.baseURL === undefined) throw new Error("OPENCODE_AUTH_URL is required in SaaS mode")
  if (!["http:", "https:"].includes(new URL(value.baseURL).protocol)) {
    throw new Error("OPENCODE_AUTH_URL must use http or https")
  }
  if (value.secret === undefined || value.secret.length < 32) {
    throw new Error("OPENCODE_AUTH_SECRET must contain at least 32 characters")
  }
  return { ...value, databaseURL: value.databaseURL, baseURL: value.baseURL, secret: value.secret }
}

export function enabled(env: NodeJS.ProcessEnv = process.env) {
  return config(env).enabled
}

export async function bootstrap(): Promise<BootstrapResult> {
  const configured = config()
  if (!configured.enabled) return { status: "disabled", applied: [], skipped: [] }
  return bootstrapWith(requireConfig())
}

export async function handle(request: Request) {
  return (await load()).handler(request)
}

export async function authenticate(headers: Headers) {
  return (await load()).getSession(headers)
}

export async function shutdown() {
  const current = runtime
  runtime = undefined
  bootstrapRuntime = undefined
  if (current === undefined) return
  await (await current).close()
}

async function load() {
  return (runtime ??= createRuntime(requireConfig()))
}

async function createRuntime(value: ReturnType<typeof requireConfig>): Promise<Runtime> {
  await bootstrapWith(value)

  const [{ betterAuth }, { Pool }] = await Promise.all([import("better-auth"), import("pg")])
  const pool = new Pool({ connectionString: value.databaseURL })
  const auth = betterAuth({
    appName: "OpenCode",
    baseURL: value.baseURL,
    basePath: "/api/auth",
    secret: value.secret,
    trustedOrigins: [...value.trustedOrigins],
    database: pool,
    emailAndPassword: {
      enabled: true,
      minPasswordLength: 8,
      maxPasswordLength: 128,
      autoSignIn: true,
    },
    rateLimit: {
      enabled: true,
      window: 60,
      max: 10,
    },
    user: {
      modelName: "opencode_identity_user",
      additionalFields: {
        role: {
          type: "string",
          required: false,
          defaultValue: "user",
          input: false,
        },
      },
    },
    session: {
      modelName: "opencode_identity_session",
      expiresIn: 60 * 60 * 24 * 7,
      updateAge: 60 * 60 * 24,
    },
    account: {
      modelName: "opencode_identity_account",
    },
    verification: {
      modelName: "opencode_identity_verification",
    },
    advanced: {
      database: {
        generateId: () => crypto.randomUUID(),
      },
      useSecureCookies: new URL(value.baseURL).protocol === "https:",
    },
  })

  return {
    handler: (request) => auth.handler(request),
    getSession: async (headers) => {
      const result = await auth.api.getSession({ headers })
      if (!result?.user || !result.session) return undefined
      const user = result.user as typeof result.user & { role?: unknown }
      return {
        actor: {
          actorID: user.id,
          email: user.email,
          name: user.name,
          platformRoles: [platformRole(user.role)],
        },
        sessionID: result.session.id,
        expiresAt: new Date(result.session.expiresAt),
      }
    },
    close: () => pool.end(),
  }
}

async function bootstrapWith(value: ReturnType<typeof requireConfig>): Promise<BootstrapResult> {
  const key = `${value.databaseURL}\0${value.autoMigrate}`
  if (bootstrapRuntime?.key === key) return bootstrapRuntime.promise

  const entry = {
    key,
    promise: runBootstrap(value),
  }
  bootstrapRuntime = entry
  try {
    return await entry.promise
  } catch (error) {
    if (bootstrapRuntime === entry) bootstrapRuntime = undefined
    throw error
  }
}

async function runBootstrap(value: ReturnType<typeof requireConfig>): Promise<BootstrapResult> {
  const sql = makeClient({ url: value.databaseURL })
  try {
    const migration = value.autoMigrate
      ? await applyMigrations(sql)
      : { applied: [] as readonly string[], skipped: [] as readonly string[] }
    const [schema] = await sql<{
      migration: string | null
      user_table: string | null
      session_table: string | null
      account_table: string | null
      verification_table: string | null
    }[]>`
      select
        to_regclass('opencode_pg_migration')::text as migration,
        to_regclass('opencode_identity_user')::text as user_table,
        to_regclass('opencode_identity_session')::text as session_table,
        to_regclass('opencode_identity_account')::text as account_table,
        to_regclass('opencode_identity_verification')::text as verification_table
    `
    const missing = Object.entries(schema ?? {})
      .filter(([, table]) => table === null)
      .map(([table]) => table)
    if (schema === undefined || missing.length > 0) {
      throw new Error(
        `OpenCode SaaS identity bootstrap is incomplete: ${
          schema === undefined ? "schema probe returned no rows" : `missing ${missing.join(", ")}`
        }`,
      )
    }
    return {
      status: "ready",
      applied: migration.applied,
      skipped: migration.skipped,
    }
  } finally {
    await sql.end()
  }
}

function optional(value: string | undefined) {
  const normalized = value?.trim()
  return normalized === "" ? undefined : normalized
}
