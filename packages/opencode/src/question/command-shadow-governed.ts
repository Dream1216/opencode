import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import {
  makeQuestionShadowBreakerStore,
  makeQuestionShadowGovernance,
  questionShadowGovernanceConfig,
  type QuestionShadowGovernanceConfig,
} from "@opencode-ai/core/question-shadow-governance"
import { makeMemoryShadowCanaryBreakerStore, type ShadowCanaryBreakerStore } from "@opencode-ai/core/session/runner/shadow-canary-breaker-store"
import { Context, Effect, Layer } from "effect"
import {
  Service as BaseService,
  node as baseNode,
  type Interface as BaseInterface,
  type Snapshot as BaseSnapshot,
} from "./command-shadow"

export interface Interface extends Omit<BaseInterface, "snapshot"> {
  readonly snapshot: () => BaseSnapshot & {
    readonly governance: ReturnType<ReturnType<typeof makeQuestionShadowGovernance>["snapshot"]>
  }
}

export class Service extends Context.Service<Service, Interface>()("@opencode/QuestionCommandShadowGoverned") {}

export function layerWith(
  input: {
    readonly config?: QuestionShadowGovernanceConfig
    readonly store?: ShadowCanaryBreakerStore
  } = {},
) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const base = yield* BaseService
      const requested = input.config ?? questionShadowGovernanceConfig()
      const initialized = yield* Effect.promise(async () => {
        try {
          const store = input.store ?? (await makeQuestionShadowBreakerStore(requested))
          return makeQuestionShadowGovernance(requested, store)
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error)
          const fallback = makeMemoryShadowCanaryBreakerStore(requested.policy)
          return makeQuestionShadowGovernance(
            {
              ...requested,
              invalidReason: [requested.invalidReason, reason].filter(Boolean).join("; "),
            },
            fallback,
          )
        }
      })
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => initialized.close()).pipe(
          Effect.ignoreCause({ log: "Warn", message: "Question shadow governance close failed" }),
        ),
      )

      const seed = (data: Parameters<BaseInterface["seed"]>[0]) =>
        execute(String(data.id), "seed", base.seed(data), false)
      const observeAsk = (data: Parameters<BaseInterface["observeAsk"]>[0]) =>
        execute(String(data.data.id), "ask", base.observeAsk(data), true)
      const observeReply = (data: Parameters<BaseInterface["observeReply"]>[0]) =>
        execute(String(data.requestID), "reply", base.observeReply(data), true)
      const observeReject = (data: Parameters<BaseInterface["observeReject"]>[0]) =>
        execute(String(data.requestID), "reject", base.observeReject(data), true)

      function execute(
        key: string,
        command: "seed" | "ask" | "reply" | "reject",
        operation: Effect.Effect<void>,
        compare: boolean,
      ) {
        return Effect.gen(function* () {
          const before = base.snapshot()
          if (!before.enabled) return yield* operation
          const admission = yield* Effect.promise(() => initialized.admit(key, command))
          if (!admission.allowed) return
          yield* operation
          if (!compare || command === "seed") return
          const after = base.snapshot()
          const outcome =
            after.errors > before.errors
              ? "error"
              : after.diverged > before.diverged
                ? "diverged"
                : after.matched > before.matched
                  ? "matched"
                  : "error"
          yield* Effect.promise(() => initialized.record({ key, command, outcome }))
        })
      }

      return Service.of({
        seed,
        observeAsk,
        observeReply,
        observeReject,
        snapshot: () => ({ ...base.snapshot(), governance: initialized.snapshot() }),
      })
    }),
  )
}

export const layer = layerWith()
export const node = LayerNode.make({ service: Service, layer, deps: [baseNode] })

export * as QuestionCommandShadow from "./command-shadow-governed"
