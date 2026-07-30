export * as DatabaseBackend from "./backend"

export type SqliteConfig = {
  readonly type: "sqlite"
  readonly filename: string
}

export type PostgresConfig = {
  readonly type: "postgres"
  readonly url?: string
  readonly requireRls: boolean
  readonly tenantID?: string
  readonly actorID?: string
}

export type PostgresAlphaConfig = {
  readonly type: "postgres-alpha"
  readonly filename: string
  readonly url?: string
  readonly requireRls: boolean
  readonly tenantID?: string
  readonly actorID?: string
  readonly dualWriteEnabled: boolean
  readonly projectorEnabled: boolean
  readonly streamEnabled: boolean
  readonly sessionProjectionEnabled: boolean
}

export type PostgresLikeConfig = PostgresConfig | PostgresAlphaConfig
export type Config = SqliteConfig | PostgresLikeConfig

export const BACKEND_ENV = "OPENCODE_DATABASE_BACKEND"
export const POSTGRES_URL_ENV = "OPENCODE_DATABASE_URL"
export const POSTGRES_REQUIRE_RLS_ENV = "OPENCODE_POSTGRES_REQUIRE_RLS"
export const POSTGRES_ALPHA_DUAL_WRITE_ENV = "OPENCODE_POSTGRES_ALPHA_DUAL_WRITE_ENABLED"
export const POSTGRES_ALPHA_PROJECTOR_ENV = "OPENCODE_POSTGRES_ALPHA_PROJECTOR_ENABLED"
export const POSTGRES_ALPHA_STREAM_ENV = "OPENCODE_POSTGRES_ALPHA_STREAM_ENABLED"
export const POSTGRES_ALPHA_SESSION_PROJECTION_ENV = "OPENCODE_POSTGRES_ALPHA_SESSION_PROJECTION_ENABLED"

export function fromEnv(sqlitePath: () => string, env: NodeJS.ProcessEnv = process.env): Config {
  const backend = env[BACKEND_ENV] ?? "sqlite"
  if (backend === "sqlite") return { type: "sqlite", filename: sqlitePath() }
  if (backend === "postgres")
    return {
      type: "postgres",
      url: env[POSTGRES_URL_ENV],
      requireRls: rlsRequired(env),
      tenantID: env.OPENCODE_TENANT_ID,
      actorID: env.OPENCODE_ACTOR_ID,
    }
  if (backend === "postgres-alpha")
    return {
      type: "postgres-alpha",
      filename: sqlitePath(),
      url: env[POSTGRES_URL_ENV],
      requireRls: rlsRequired(env),
      tenantID: env.OPENCODE_TENANT_ID,
      actorID: env.OPENCODE_ACTOR_ID,
      dualWriteEnabled: enabled(env[POSTGRES_ALPHA_DUAL_WRITE_ENV]),
      projectorEnabled: enabled(env[POSTGRES_ALPHA_PROJECTOR_ENV]),
      streamEnabled: enabled(env[POSTGRES_ALPHA_STREAM_ENV]),
      sessionProjectionEnabled: enabled(env[POSTGRES_ALPHA_SESSION_PROJECTION_ENV]),
    }
  throw new Error(
    `${BACKEND_ENV} must be "sqlite", "postgres", or "postgres-alpha"; received ${JSON.stringify(backend)}. SQLite remains the default.`,
  )
}

export function describe(config: Config) {
  if (config.type === "sqlite") return `sqlite:${config.filename}`
  if (config.type === "postgres-alpha")
    return `sqlite:${config.filename}+postgres-alpha:${config.url === undefined ? "missing-url" : "configured"}:rls-${config.requireRls ? "required" : "optional"}:dual-write-${config.dualWriteEnabled ? "enabled" : "disabled"}`
  return `postgres:${config.url === undefined ? "missing-url" : "configured"}:rls-${config.requireRls ? "required" : "optional"}`
}

export function isPostgres(config: Config): config is PostgresLikeConfig {
  return config.type === "postgres" || config.type === "postgres-alpha"
}

function rlsRequired(env: NodeJS.ProcessEnv) {
  return env[POSTGRES_REQUIRE_RLS_ENV] !== "0" && env[POSTGRES_REQUIRE_RLS_ENV] !== "false"
}

function enabled(value: string | undefined) {
  return value === "1" || value === "true"
}
