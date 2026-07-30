import { createHash, createHmac } from "node:crypto"

export type WorkerQueueIdentityProvider = "hmac" | "oidc" | "better-auth"

export type WorkerQueueIdentityRequest = {
  readonly method: string
  readonly target: string
  readonly actorID: string
  readonly timestamp: number
  readonly nonce: string
  readonly signature: string
  readonly keyID?: string
  readonly identityProvider?: WorkerQueueIdentityProvider
  readonly identityToken?: string
  readonly sessionCookie?: string
  readonly body: string
}

export function signRequest(
  input: Omit<
    WorkerQueueIdentityRequest,
    "signature" | "identityProvider" | "identityToken" | "sessionCookie"
  > & { readonly secret: string },
) {
  return createHmac("sha256", input.secret).update(canonicalRequest(input)).digest("hex")
}

function canonicalRequest(
  input: Omit<
    WorkerQueueIdentityRequest,
    "signature" | "identityProvider" | "identityToken" | "sessionCookie"
  >,
) {
  const common = [
    input.method.toUpperCase(),
    input.target,
    input.actorID,
    String(input.timestamp),
    input.nonce,
    createHash("sha256").update(input.body).digest("hex"),
  ]
  return input.keyID === undefined
    ? common.join("\n")
    : ["opencode-worker-queue-v2", input.keyID, ...common].join("\n")
}

export * as WorkerQueueRequestSigning from "./worker-queue-request-signing"
