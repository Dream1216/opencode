import postgres, { type Sql } from "postgres"

export type ClientConfig = {
  readonly url: string
  readonly max?: number
  readonly connectTimeoutSeconds?: number
  readonly idleTimeoutSeconds?: number
}

export type TenantContext = {
  readonly tenantID: string
  readonly actorID?: string
  readonly teamID?: string
}

type TenantContextClient = {
  readonly unsafe: Sql["unsafe"]
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): ClientConfig | undefined {
  const url = env.OPENCODE_DATABASE_URL
  if (url === undefined || url.trim() === "") return undefined
  return {
    url,
    max: Number(env.OPENCODE_POSTGRES_POOL_MAX ?? 1),
    connectTimeoutSeconds: Number(env.OPENCODE_POSTGRES_CONNECT_TIMEOUT_SECONDS ?? 10),
    idleTimeoutSeconds: Number(env.OPENCODE_POSTGRES_IDLE_TIMEOUT_SECONDS ?? 5),
  }
}

export function makeClient(config: ClientConfig): Sql {
  return postgres(config.url, {
    max: config.max ?? 1,
    connect_timeout: config.connectTimeoutSeconds ?? 10,
    idle_timeout: config.idleTimeoutSeconds ?? 5,
    onnotice: () => {},
  })
}

export async function setTenantContext(sql: TenantContextClient, context: TenantContext) {
  await sql.unsafe(`select set_config('opencode.tenant_id', $1, true)`, [context.tenantID])
  await sql.unsafe(`select set_config('opencode.actor_id', $1, true)`, [context.actorID ?? ""])
  await sql.unsafe(`select set_config('opencode.team_id', $1, true)`, [context.teamID ?? ""])
}
