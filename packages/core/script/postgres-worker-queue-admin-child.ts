import { Cause, Effect, Exit } from "effect"
import {
  ConflictError,
  RateLimitError,
  Service,
  layerFromEnv,
  signRequest,
  type SignedRequest,
} from "../src/database/postgres/worker-queue-admin"

const operation = required(process.env.OPENCODE_P4_41_CHILD_OPERATION, "OPENCODE_P4_41_CHILD_OPERATION")
const actorID = required(process.env.OPENCODE_P4_41_CHILD_ACTOR_ID, "OPENCODE_P4_41_CHILD_ACTOR_ID")
const secret = required(process.env.OPENCODE_P4_41_CHILD_ACTOR_SECRET, "OPENCODE_P4_41_CHILD_ACTOR_SECRET")
const target = process.env.OPENCODE_P4_41_CHILD_TARGET ?? "/experimental/worker-queue/readiness"
const startAt = Number(process.env.OPENCODE_P4_41_CHILD_START_AT ?? Date.now())

if (!Number.isSafeInteger(startAt)) throw new Error("OPENCODE_P4_41_CHILD_START_AT must be an integer")
if (startAt > Date.now()) await Bun.sleep(startAt - Date.now())

const request = signed(actorID, secret, operation === "approve" ? "POST" : "GET", target)
const result = await Effect.runPromise(
  Effect.gen(function* () {
    const admin = yield* Service
    const authenticated = yield* admin.authenticate(request).pipe(Effect.exit)
    if (Exit.isFailure(authenticated)) {
      const error = Cause.squash(authenticated.cause)
      if (error instanceof RateLimitError) return { outcome: "throttle" as const }
      return { outcome: "error" as const, error: errorName(error) }
    }
    if (operation === "authenticate") return { outcome: "allow" as const }
    if (operation !== "approve") return { outcome: "error" as const, error: "UnsupportedOperation" }
    const actionID = required(process.env.OPENCODE_P4_41_CHILD_ACTION_ID, "OPENCODE_P4_41_CHILD_ACTION_ID")
    const approved = yield* admin.approve(authenticated.value, actionID).pipe(Effect.exit)
    if (Exit.isSuccess(approved)) {
      return {
        outcome: "approved" as const,
        status: approved.value.status,
        approvalCount: approved.value.approvalCount,
      }
    }
    const error = Cause.squash(approved.cause)
    if (error instanceof ConflictError) return { outcome: "conflict" as const }
    return { outcome: "error" as const, error: errorName(error) }
  }).pipe(Effect.provide(layerFromEnv(process.env)), Effect.scoped),
)

process.stdout.write(`${JSON.stringify(result)}\n`)

function signed(actorID: string, secret: string, method: string, target: string): SignedRequest {
  const request = {
    method,
    target,
    actorID,
    timestamp: Date.now(),
    nonce: `p4_41_${crypto.randomUUID()}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_"),
    body: "",
  }
  return { ...request, signature: signRequest({ ...request, secret }) }
}

function errorName(error: unknown) {
  return error instanceof Error ? error.name : "UnknownError"
}

function required(value: string | undefined, name: string) {
  if (value === undefined || value.trim() === "") throw new Error(`${name} is required`)
  return value
}
