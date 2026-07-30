export * as QuestionCommandShadow from "./command-shadow"

import { QuestionCommandGateway } from "@opencode-ai/core/question-command-gateway"
import { makeClient } from "@opencode-ai/core/database/postgres/client"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Question as QuestionV2 } from "@opencode-ai/schema/question"
import { QuestionV1 } from "@opencode-ai/schema/question-v1"
import { Context, Effect, Layer } from "effect"
import { RuntimeFlags } from "@/effect/runtime-flags"

export type Operation = "ask" | "reply" | "reject"
export type CommandStatus = "appended" | "idempotent"
export type StoreConfig =
  | {
      readonly backend: "memory"
    }
  | {
      readonly backend: "postgres"
      readonly url: string
      readonly tenantID: string
      readonly actorID?: string
      readonly poolMax: number
    }

export interface Comparison {
  readonly operation: Operation
  readonly requestID: string
  readonly expected: CommandStatus
  readonly actual?: CommandStatus
  readonly outcome: "matched" | "diverged" | "error"
  readonly error?: string
}

export interface Snapshot {
  readonly enabled: boolean
  readonly seeded: number
  readonly compared: number
  readonly matched: number
  readonly diverged: number
  readonly errors: number
  readonly last?: Comparison
}

export interface Interface {
  readonly seed: (data: QuestionV1.AskedData) => Effect.Effect<void>
  readonly observeAsk: (input: {
    readonly data: QuestionV1.AskedData
    readonly expected: CommandStatus
  }) => Effect.Effect<void>
  readonly observeReply: (input: {
    readonly requestID: typeof QuestionV1.ID.Type
    readonly sessionID: QuestionV1.AskedData["sessionID"]
    readonly answers: ReadonlyArray<typeof QuestionV1.Answer.Type>
    readonly expected: CommandStatus
  }) => Effect.Effect<void>
  readonly observeReject: (input: {
    readonly requestID: typeof QuestionV1.ID.Type
    readonly sessionID: QuestionV1.AskedData["sessionID"]
    readonly expected: CommandStatus
  }) => Effect.Effect<void>
  readonly snapshot: () => Snapshot
}

export class Service extends Context.Service<Service, Interface>()("@opencode/QuestionCommandShadow") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const flags = yield* RuntimeFlags.Service
    if (!flags.experimentalQuestionCommandGatewayShadow) {
      const snapshot = () => ({
        enabled: false,
        seeded: 0,
        compared: 0,
        matched: 0,
        diverged: 0,
        errors: 0,
      }) satisfies Snapshot
      return Service.of({
        seed: () => Effect.void,
        observeAsk: () => Effect.void,
        observeReply: () => Effect.void,
        observeReject: () => Effect.void,
        snapshot,
      })
    }

    const store = yield* Effect.promise(() => makeStore())
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => store.close()).pipe(
        Effect.ignoreCause({ log: "Warn", message: "Question Command Gateway shadow Store close failed" }),
      ),
    )
    const gateway = store.gateway
    const counters = {
      seeded: 0,
      compared: 0,
      matched: 0,
      diverged: 0,
      errors: 0,
      last: undefined as Comparison | undefined,
    }

    const record = (comparison: Comparison) => {
      counters.compared += 1
      if (comparison.outcome === "matched") counters.matched += 1
      if (comparison.outcome === "diverged") counters.diverged += 1
      if (comparison.outcome === "error") counters.errors += 1
      counters.last = comparison
      return comparison
    }

    const compare = (
      operation: Operation,
      requestID: string,
      expected: CommandStatus,
      command: () => Promise<QuestionCommandGateway.CommandResult>,
    ) =>
      Effect.promise(async () => {
        try {
          const result = await command()
          return record({
            operation,
            requestID,
            expected,
            actual: result.status,
            outcome: result.status === expected ? "matched" : "diverged",
          })
        } catch (error) {
          return record({
            operation,
            requestID,
            expected,
            outcome: "error",
            error: error instanceof Error ? error.message : String(error),
          })
        }
      }).pipe(
        Effect.tap((comparison) =>
          comparison.outcome === "matched"
            ? Effect.void
            : Effect.logWarning("Question Command Gateway shadow comparison failed", comparison),
        ),
        Effect.asVoid,
      )

    const seed = Effect.fn("QuestionCommandShadow.seed")((data: QuestionV1.AskedData) =>
      Effect.promise(async () => {
        try {
          await gateway.ask(toV2Asked(data))
          counters.seeded += 1
        } catch (error) {
          record({
            operation: "ask",
            requestID: data.id,
            expected: "idempotent",
            outcome: "error",
            error: error instanceof Error ? error.message : String(error),
          })
        }
      }),
    )

    const observeAsk = Effect.fn("QuestionCommandShadow.observeAsk")(
      (input: { readonly data: QuestionV1.AskedData; readonly expected: CommandStatus }) =>
        compare("ask", input.data.id, input.expected, () => gateway.ask(toV2Asked(input.data))),
    )

    const observeReply = Effect.fn("QuestionCommandShadow.observeReply")(
      (input: {
        readonly requestID: typeof QuestionV1.ID.Type
        readonly sessionID: QuestionV1.AskedData["sessionID"]
        readonly answers: ReadonlyArray<typeof QuestionV1.Answer.Type>
        readonly expected: CommandStatus
      }) =>
        compare("reply", input.requestID, input.expected, () =>
          gateway.reply({
            requestID: QuestionV2.ID.ascending(input.requestID),
            sessionID: input.sessionID,
            answers: input.answers,
          }),
        ),
    )

    const observeReject = Effect.fn("QuestionCommandShadow.observeReject")(
      (input: {
        readonly requestID: typeof QuestionV1.ID.Type
        readonly sessionID: QuestionV1.AskedData["sessionID"]
        readonly expected: CommandStatus
      }) =>
        compare("reject", input.requestID, input.expected, () =>
          gateway.reject({
            requestID: QuestionV2.ID.ascending(input.requestID),
            sessionID: input.sessionID,
          }),
        ),
    )

    const snapshot = (): Snapshot => ({
      enabled: true,
      seeded: counters.seeded,
      compared: counters.compared,
      matched: counters.matched,
      diverged: counters.diverged,
      errors: counters.errors,
      ...(counters.last === undefined ? {} : { last: counters.last }),
    })

    return Service.of({ seed, observeAsk, observeReply, observeReject, snapshot })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer,
  deps: [RuntimeFlags.node],
})

export function storeConfig(env: NodeJS.ProcessEnv = process.env): StoreConfig {
  const backend = env.OPENCODE_QUESTION_SHADOW_STORE_BACKEND?.trim().toLowerCase() || "memory"
  if (backend === "memory") return { backend }
  if (backend !== "postgres") {
    throw new Error("OPENCODE_QUESTION_SHADOW_STORE_BACKEND must be memory or postgres")
  }
  const url =
    env.OPENCODE_QUESTION_SHADOW_STORE_DATABASE_URL?.trim() ||
    env.OPENCODE_DATABASE_URL?.trim()
  if (!url) {
    throw new Error(
      "PostgreSQL Question shadow Store requires OPENCODE_QUESTION_SHADOW_STORE_DATABASE_URL or OPENCODE_DATABASE_URL",
    )
  }
  const poolMax = Number(env.OPENCODE_QUESTION_SHADOW_STORE_DATABASE_MAX ?? 2)
  if (!Number.isSafeInteger(poolMax) || poolMax < 1 || poolMax > 32) {
    throw new Error("OPENCODE_QUESTION_SHADOW_STORE_DATABASE_MAX must be an integer between 1 and 32")
  }
  const actorID = env.OPENCODE_QUESTION_SHADOW_STORE_ACTOR_ID?.trim()
  return {
    backend,
    url,
    tenantID:
      env.OPENCODE_QUESTION_SHADOW_STORE_TENANT_ID?.trim() ||
      "tenant_question_shadow_global",
    ...(actorID ? { actorID } : {}),
    poolMax,
  }
}

async function makeStore() {
  const config = storeConfig()
  if (config.backend === "memory") {
    return {
      gateway: QuestionCommandGateway.make(new MemoryStore(), { maxAttempts: 8 }),
      close: async () => {},
    }
  }

  const sql = makeClient({ url: config.url, max: config.poolMax })
  try {
    const now = Date.now()
    await sql`
      insert into tenant (id, name, time_created, time_updated)
      values (${config.tenantID}, 'Question shadow global tenant', ${now}, ${now})
      on conflict (id) do update set time_updated = excluded.time_updated
    `
    return {
      gateway: QuestionCommandGateway.makePostgres({
        sql,
        tenant: {
          tenantID: config.tenantID,
          ...(config.actorID ? { actorID: config.actorID } : {}),
        },
        ownerID: `question-shadow:${process.pid}`,
        maxAttempts: 8,
      }),
      close: () => sql.end({ timeout: 5 }),
    }
  } catch (error) {
    await sql.end({ timeout: 5 })
    throw error
  }
}

class MemoryStore implements QuestionCommandGateway.Store {
  private readonly records = new Map<QuestionV2.ID, QuestionCommandGateway.EventRecord[]>()

  async read(requestID: QuestionV2.ID) {
    return [...(this.records.get(requestID) ?? [])]
  }

  async append(input: {
    readonly requestID: QuestionV2.ID
    readonly expectedSeq: number
    readonly event: QuestionCommandGateway.EventRecord["event"]
  }) {
    await Promise.resolve()
    const records = this.records.get(input.requestID) ?? []
    const actualSeq = records.at(-1)?.seq ?? -1
    if (actualSeq !== input.expectedSeq) {
      throw new QuestionCommandGateway.SequenceConflictError(input.requestID, input.expectedSeq, actualSeq)
    }
    const record = {
      seq: actualSeq + 1,
      event: input.event,
    }
    records.push(record)
    this.records.set(input.requestID, records)
    return record
  }
}

function toV2Asked(data: QuestionV1.AskedData): QuestionV2.AskedData {
  return {
    id: QuestionV2.ID.ascending(data.id),
    sessionID: data.sessionID,
    questions: data.questions,
    ...(data.tool === undefined
      ? {}
      : {
          tool: {
            messageID: data.tool.messageID,
            callID: data.tool.callID,
          },
        }),
    location: data.location,
  }
}
