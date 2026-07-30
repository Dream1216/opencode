import type { Sql } from "postgres"
import { makeClient } from "../../database/postgres/client"

export type ShadowCanaryBreakerPolicy = {
  readonly windowSize: number
  readonly minimumSamples: number
  readonly failureRateThreshold: number
  readonly structuralMismatchRateThreshold: number
  readonly slowRateThreshold: number
  readonly latencyRatioThreshold: number
  readonly cooldownMs: number
}

export type ShadowCanaryBreakerState = {
  readonly open: boolean
  readonly reason?: string
  readonly openedAt?: number
  readonly sampleCount: number
  readonly failureRate: number
  readonly structuralMismatchRate: number
  readonly slowRate: number
  readonly updatedAt: number
  readonly revision: number
}

export type ShadowCanaryBreakerStore = {
  read(): Promise<ShadowCanaryBreakerState>
  record(diff: unknown): Promise<ShadowCanaryBreakerState>
  destroy?(): Promise<void>
  close(): Promise<void>
}

type StoredDiff = {
  readonly requestDigest: string
  readonly value: Record<string, unknown>
  readonly comparedAt: number
}

const emptyState = (now = Date.now(), revision = 0): ShadowCanaryBreakerState => ({
  open: false,
  sampleCount: 0,
  failureRate: 0,
  structuralMismatchRate: 0,
  slowRate: 0,
  updatedAt: now,
  revision,
})

const recordValue = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {}

const finite = (value: unknown) => (typeof value === "number" && Number.isFinite(value) ? value : undefined)

const normalizeDiff = (value: unknown): StoredDiff => {
  const diff = recordValue(value)
  const requestDigest = typeof diff.requestDigest === "string" ? diff.requestDigest : undefined
  if (!requestDigest) throw new Error("shadow canary breaker diff requires requestDigest")
  return {
    requestDigest,
    value: diff,
    comparedAt: finite(diff.comparedAt) ?? Date.now(),
  }
}

const evaluate = (
  diffs: readonly Record<string, unknown>[],
  policy: ShadowCanaryBreakerPolicy,
  previous: ShadowCanaryBreakerState,
  now: number,
): ShadowCanaryBreakerState => {
  const sampleCount = diffs.length
  const failures = diffs.filter((diff) => diff.candidateFailed === true).length
  const mismatches = diffs.filter((diff) => diff.statusMatch === false || diff.toolPlanDigestMatch === false).length
  const slow = diffs.filter((diff) => (finite(diff.latencyRatio) ?? 0) > policy.latencyRatioThreshold).length
  const failureRate = sampleCount === 0 ? 0 : failures / sampleCount
  const structuralMismatchRate = sampleCount === 0 ? 0 : mismatches / sampleCount
  const slowRate = sampleCount === 0 ? 0 : slow / sampleCount
  const reasons: string[] = []
  if (sampleCount >= policy.minimumSamples) {
    if (failureRate > policy.failureRateThreshold) reasons.push(`candidate_failure_rate=${failureRate.toFixed(4)}`)
    if (structuralMismatchRate > policy.structuralMismatchRateThreshold) {
      reasons.push(`structural_mismatch_rate=${structuralMismatchRate.toFixed(4)}`)
    }
    if (slowRate > policy.slowRateThreshold) reasons.push(`slow_rate=${slowRate.toFixed(4)}`)
  }
  return {
    open: reasons.length > 0,
    reason: reasons.length > 0 ? reasons.join(",") : undefined,
    openedAt: reasons.length > 0 ? previous.openedAt ?? now : undefined,
    sampleCount,
    failureRate,
    structuralMismatchRate,
    slowRate,
    updatedAt: now,
    revision: previous.revision + 1,
  }
}

const coolingDown = (state: ShadowCanaryBreakerState, policy: ShadowCanaryBreakerPolicy, now: number) =>
  state.open && state.openedAt !== undefined && now - state.openedAt < policy.cooldownMs

export function makeMemoryShadowCanaryBreakerStore(
  policy: ShadowCanaryBreakerPolicy,
): ShadowCanaryBreakerStore {
  const diffs = new Map<string, StoredDiff>()
  let state = emptyState()
  let closed = false

  const resetExpired = (now: number) => {
    if (!state.open || coolingDown(state, policy, now)) return
    diffs.clear()
    state = emptyState(now, state.revision + 1)
  }

  return {
    async read() {
      if (closed) throw new Error("shadow canary breaker store is closed")
      resetExpired(Date.now())
      return { ...state }
    },
    async record(value) {
      if (closed) throw new Error("shadow canary breaker store is closed")
      const now = Date.now()
      resetExpired(now)
      const diff = normalizeDiff(value)
      diffs.set(diff.requestDigest, diff)
      const window = [...diffs.values()]
        .sort((left, right) => right.comparedAt - left.comparedAt)
        .slice(0, policy.windowSize)
      diffs.clear()
      for (const item of window) diffs.set(item.requestDigest, item)
      if (!coolingDown(state, policy, now)) {
        state = evaluate(
          window.map((item) => item.value),
          policy,
          state,
          now,
        )
      }
      return { ...state }
    },
    async destroy() {
      diffs.clear()
      state = emptyState()
    },
    async close() {
      closed = true
    },
  }
}

type PostgresOptions = {
  readonly url: string
  readonly scope: string
  readonly policy: ShadowCanaryBreakerPolicy
  readonly max?: number
}

type StateRow = {
  readonly state: unknown
  readonly revision: number | string
}

type DiffRow = {
  readonly diff: unknown
}

const parseState = (row: StateRow | undefined): ShadowCanaryBreakerState => {
  if (!row) return emptyState()
  const raw = typeof row.state === "string" ? JSON.parse(row.state) : row.state
  const value = recordValue(raw)
  return {
    open: value.open === true,
    reason: typeof value.reason === "string" ? value.reason : undefined,
    openedAt: finite(value.openedAt),
    sampleCount: finite(value.sampleCount) ?? 0,
    failureRate: finite(value.failureRate) ?? 0,
    structuralMismatchRate: finite(value.structuralMismatchRate) ?? 0,
    slowRate: finite(value.slowRate) ?? 0,
    updatedAt: finite(value.updatedAt) ?? Date.now(),
    revision: Number(row.revision ?? value.revision ?? 0),
  }
}

const initialize = async (sql: Sql) => {
  await sql.begin(async (tx) => {
    await tx`select pg_advisory_xact_lock(hashtext('opencode.shadow.canary.breaker.schema'), hashtext(current_schema()))`
    await tx.unsafe(`
      create table if not exists opencode_shadow_canary_breaker_state (
        scope text primary key,
        state jsonb not null,
        revision bigint not null default 0,
        time_updated bigint not null
      )
    `)
    await tx.unsafe(`
      create table if not exists opencode_shadow_canary_breaker_diff (
        scope text not null,
        request_digest text not null,
        diff jsonb not null,
        time_compared bigint not null,
        primary key (scope, request_digest)
      )
    `)
    await tx.unsafe(`
      create index if not exists opencode_shadow_canary_breaker_diff_window_idx
      on opencode_shadow_canary_breaker_diff (scope, time_compared desc)
    `)
  })
}

export async function makePostgresShadowCanaryBreakerStore(options: PostgresOptions): Promise<ShadowCanaryBreakerStore> {
  const sql = makeClient({ url: options.url, max: options.max ?? 2 })
  await initialize(sql)
  let closed = false

  const transact = async (input?: unknown) =>
    sql.begin(async (tx) => {
      const now = Date.now()
      await tx`select pg_advisory_xact_lock(hashtext('opencode.shadow.canary.breaker'), hashtext(${options.scope}))`
      const rows = await tx<StateRow[]>`
        select state, revision
        from opencode_shadow_canary_breaker_state
        where scope = ${options.scope}
        for update
      `
      let state = parseState(rows[0])
      let changed = false
      if (state.open && !coolingDown(state, options.policy, now)) {
        await tx`delete from opencode_shadow_canary_breaker_diff where scope = ${options.scope}`
        state = emptyState(now, state.revision + 1)
        changed = true
      }
      if (input !== undefined) {
        const normalized = normalizeDiff(input)
        await tx`
          insert into opencode_shadow_canary_breaker_diff (scope, request_digest, diff, time_compared)
          values (${options.scope}, ${normalized.requestDigest}, ${tx.json(normalized.value as never)}, ${normalized.comparedAt})
          on conflict (scope, request_digest) do nothing
        `
        await tx`
          delete from opencode_shadow_canary_breaker_diff
          where scope = ${options.scope}
            and request_digest in (
              select request_digest
              from opencode_shadow_canary_breaker_diff
              where scope = ${options.scope}
              order by time_compared desc, request_digest desc
              offset ${options.policy.windowSize}
            )
        `
        if (!coolingDown(state, options.policy, now)) {
          const diffRows = await tx<DiffRow[]>`
            select diff
            from opencode_shadow_canary_breaker_diff
            where scope = ${options.scope}
            order by time_compared desc, request_digest desc
            limit ${options.policy.windowSize}
          `
          state = evaluate(
            diffRows.map((row) =>
              typeof row.diff === "string" ? recordValue(JSON.parse(row.diff)) : recordValue(row.diff),
            ),
            options.policy,
            state,
            now,
          )
        }
        changed = true
      }
      if (changed) {
        await tx`
          insert into opencode_shadow_canary_breaker_state (scope, state, revision, time_updated)
          values (${options.scope}, ${tx.json(state)}, ${state.revision}, ${state.updatedAt})
          on conflict (scope) do update
          set state = excluded.state,
              revision = excluded.revision,
              time_updated = excluded.time_updated
        `
      }
      return state
    })

  return {
    async read() {
      if (closed) throw new Error("shadow canary breaker store is closed")
      return transact()
    },
    async record(diff) {
      if (closed) throw new Error("shadow canary breaker store is closed")
      return transact(diff)
    },
    async destroy() {
      if (closed) throw new Error("shadow canary breaker store is closed")
      await sql.begin(async (tx) => {
        await tx`select pg_advisory_xact_lock(hashtext('opencode.shadow.canary.breaker'), hashtext(${options.scope}))`
        await tx`delete from opencode_shadow_canary_breaker_diff where scope = ${options.scope}`
        await tx`delete from opencode_shadow_canary_breaker_state where scope = ${options.scope}`
      })
    },
    async close() {
      if (closed) return
      closed = true
      await sql.end({ timeout: 5 })
    },
  }
}

export async function shadowCanaryBreakerStoreFromEnv(
  env: NodeJS.ProcessEnv,
  policy: ShadowCanaryBreakerPolicy,
  targetVersion: string,
) {
  const backend = env.OPENCODE_SHADOW_CANARY_BREAKER_BACKEND?.trim().toLowerCase() || "memory"
  if (backend === "memory") return makeMemoryShadowCanaryBreakerStore(policy)
  if (backend !== "postgres") throw new Error(`unsupported shadow canary breaker backend: ${backend}`)
  const url =
    env.OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL?.trim() || env.OPENCODE_DATABASE_URL?.trim()
  if (!url) throw new Error("PostgreSQL breaker requires OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_URL")
  const scope =
    env.OPENCODE_SHADOW_CANARY_BREAKER_SCOPE?.trim() ||
    `${env.OPENCODE_TENANT_ID?.trim() || "global"}:${targetVersion}`
  return makePostgresShadowCanaryBreakerStore({
    url,
    scope,
    policy,
    max: Number(env.OPENCODE_SHADOW_CANARY_BREAKER_DATABASE_MAX ?? 2),
  })
}
