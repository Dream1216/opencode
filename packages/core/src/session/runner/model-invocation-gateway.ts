import {
  LLMError,
  LLMEvent,
  type LLMRequest,
  type LLMClientShape,
  type ProviderErrorEvent,
  type Usage,
} from "@opencode-ai/llm"
import { DateTime, Effect } from "effect"
import { EventV2 } from "../../event"
import { ModelV2 } from "../../model"
import { SessionEvent } from "../event"
import { SessionSchema } from "../schema"
import {
  createShadowCanaryOutcomeAccumulator,
  ShadowCanary,
  type ShadowCanaryConfig,
  type ShadowCanaryPrepared,
} from "./shadow-canary"

type Input = {
  readonly events: EventV2.Interface
  readonly llm: LLMClientShape
  readonly shadowCanary?: ShadowCanaryConfig
}

type StartInput = {
  readonly sessionID: SessionSchema.ID
  readonly agent: string
  readonly model: ModelV2.Ref
  readonly request: LLMRequest
}

let nextInvocation = 0

export const ModelInvocationGateway = {
  make(input: Input) {
    const shadowCanary = input.shadowCanary ?? ShadowCanary.config()
    return {
      start: Effect.fn("ModelInvocationGateway.start")(function* (start: StartInput) {
        const id = invocationID()
        const started = Date.now()
        let attempt = 1
        let lastUsage: Usage | undefined
        let failed = false
        let canaryPrepared: ShadowCanaryPrepared | undefined
        let canaryOutcomeReported = false
        const canaryOutcome = createShadowCanaryOutcomeAccumulator()

        const base = Effect.fn("ModelInvocationGateway.base")(function* () {
          return {
            sessionID: start.sessionID,
            timestamp: yield* DateTime.now,
            invocationID: id,
            attempt,
            agent: start.agent,
            model: start.model,
          }
        })

        const publishUsage = Effect.fn("ModelInvocationGateway.usage")(function* (usage: Usage) {
          lastUsage = usage
          yield* input.events.publish(SessionEvent.ModelInvocation.Usage, {
            ...(yield* base()),
            usage: usageSnapshot(usage),
            cost: estimateCost(usage),
          })
        })

        const reportCanaryOutcome = (status: "completed" | "failed", error?: unknown) => {
          if (!canaryPrepared || canaryOutcomeReported) return
          canaryOutcomeReported = true
          const usage = usageSnapshot(lastUsage)
          const outcome = canaryOutcome.snapshot({
            source: "primary",
            requestDigest: canaryPrepared.requestDigest,
            invocationID: id,
            status,
            durationMs: Date.now() - started,
            usage: {
              input: usage.input,
              output: usage.output,
              reasoning: usage.reasoning,
              total: usage.total,
            },
            cost: estimateCost(lastUsage),
            ...(error === undefined ? {} : { error: errorSnapshot(error).message }),
          })
          Effect.runFork(Effect.promise(() => ShadowCanary.reportOutcome(shadowCanary, outcome)).pipe(Effect.ignoreCause))
        }

        const fail = Effect.fn("ModelInvocationGateway.fail")(function* (error: unknown) {
          if (failed) return
          failed = true
          yield* input.events.publish(SessionEvent.ModelInvocation.Failed, {
            ...(yield* base()),
            durationMs: Date.now() - started,
            error: errorSnapshot(error),
            usage: usageSnapshot(lastUsage),
            cost: estimateCost(lastUsage),
          })
          yield* Effect.sync(() => reportCanaryOutcome("failed", error))
        })

        yield* input.events.publish(SessionEvent.ModelInvocation.Attempted, {
          ...(yield* base()),
          provider: start.request.model.provider,
          route: start.request.model.route.id,
        })

        if (shadowCanary.enabled) {
          const prepared = ShadowCanary.prepare(shadowCanary, {
            invocationID: id,
            sessionID: start.sessionID,
            agent: start.agent,
            model: start.model,
            request: start.request,
          })
          const canaryBase = Effect.fn("ModelInvocationGateway.shadowCanaryBase")(function* () {
            return {
              sessionID: start.sessionID,
              timestamp: yield* DateTime.now,
              invocationID: id,
              targetVersion: shadowCanary.targetVersion,
            }
          })
          if (prepared.status === "skipped") {
            yield* input.events.publish(SessionEvent.ShadowCanary.Skipped, {
              ...(yield* canaryBase()),
              reason: prepared.reason,
              sampleRate: shadowCanary.sampleRate,
            })
          } else {
            canaryPrepared = prepared
            yield* input.events.publish(SessionEvent.ShadowCanary.Dispatched, {
              ...(yield* canaryBase()),
              requestDigest: prepared.requestDigest,
              payloadBytes: prepared.payloadBytes,
              sampleRate: shadowCanary.sampleRate,
            })
            yield* Effect.sync(() =>
              Effect.runFork(
                Effect.gen(function* () {
                  const result = yield* Effect.promise(() => ShadowCanary.dispatch(shadowCanary, prepared))
                  if (result.status === "accepted") {
                    yield* input.events.publish(SessionEvent.ShadowCanary.Accepted, {
                      ...(yield* canaryBase()),
                      requestDigest: prepared.requestDigest,
                      durationMs: result.durationMs,
                      statusCode: result.statusCode,
                      receiptID: result.receiptID,
                    })
                    return
                  }
                  yield* input.events.publish(SessionEvent.ShadowCanary.Failed, {
                    ...(yield* canaryBase()),
                    requestDigest: prepared.requestDigest,
                    durationMs: result.durationMs,
                    failureType: result.failureType,
                    error: result.error,
                    statusCode: result.statusCode,
                  })
                }).pipe(Effect.ignoreCause),
              ),
            )
          }
        }

        return {
          id,
          stream: () => input.llm.stream(start.request),
          observe: Effect.fn("ModelInvocationGateway.observe")(function* (event: LLMEvent) {
            canaryOutcome.observe(event)
            if ("usage" in event && event.usage !== undefined) yield* publishUsage(event.usage)
            if (LLMEvent.is.providerError(event)) yield* fail(event)
          }),
          retry: Effect.fn("ModelInvocationGateway.retry")(function* (error: unknown) {
            attempt += 1
            yield* input.events.publish(SessionEvent.ModelInvocation.Retried, {
              ...(yield* base()),
              error: errorSnapshot(error),
            })
          }),
          complete: Effect.fn("ModelInvocationGateway.complete")(function* () {
            if (failed) return
            yield* input.events.publish(SessionEvent.ModelInvocation.Completed, {
              ...(yield* base()),
              durationMs: Date.now() - started,
              usage: usageSnapshot(lastUsage),
              cost: estimateCost(lastUsage),
            })
            yield* Effect.sync(() => reportCanaryOutcome("completed"))
          }),
          fail,
          cost: () => estimateCost(lastUsage).total,
        }
      }),
    }
  },
}

function invocationID() {
  nextInvocation += 1
  return `minv_${Date.now().toString(36)}_${nextInvocation.toString(36)}`
}

function safe(value: number | undefined) {
  return Math.max(0, Number.isFinite(value) ? (value ?? 0) : 0)
}

function usageSnapshot(usage: Usage | undefined) {
  return {
    input: safe(usage?.nonCachedInputTokens),
    output: safe(usage?.visibleOutputTokens),
    reasoning: safe(usage?.reasoningTokens),
    cache: {
      read: safe(usage?.cacheReadInputTokens),
      write: safe(usage?.cacheWriteInputTokens),
    },
    total: safe(usage?.totalTokens),
  }
}

function estimateCost(_usage: Usage | undefined) {
  return {
    total: 0,
    currency: "USD",
  }
}

function errorSnapshot(error: unknown) {
  if (isProviderError(error)) {
    return {
      type: "provider",
      message: error.message,
      retryable: error.retryable,
      classification: error.classification,
      providerMetadata: error.providerMetadata,
    }
  }
  if (error instanceof LLMError) {
    return {
      type: "llm",
      message: error.reason.message,
    }
  }
  if (error instanceof Error) {
    return {
      type: "unknown",
      message: error.message,
    }
  }
  return {
    type: "unknown",
    message: String(error),
  }
}

function isProviderError(error: unknown): error is ProviderErrorEvent {
  return typeof error === "object" && error !== null && "type" in error && error.type === "provider-error"
}
