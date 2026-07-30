import fs from "node:fs/promises"
import { createSecretManager, type SecretManager } from "./security/secret-manager"

export async function resolveQuestionShadowReleaseSecret(
  input: {
    readonly direct?: string
    readonly reference?: string
    readonly label: string
  },
  manager: SecretManager = createSecretManager({ cacheTtlMs: 0 }),
) {
  const reference = input.reference?.trim()
  if (reference) return manager.resolve(reference, { refresh: true })
  const direct = input.direct?.trim()
  if (direct) return direct
  throw new Error(`${input.label} or its Secret Manager reference is required`)
}

export async function readQuestionShadowReleaseInput(
  input: {
    readonly path?: string
    readonly reference?: string
    readonly label: string
  },
  manager: SecretManager = createSecretManager({ cacheTtlMs: 0 }),
) {
  const reference = input.reference?.trim()
  if (reference) return manager.resolve(reference, { refresh: true })
  const path = input.path?.trim()
  if (path) return fs.readFile(path, "utf8")
  throw new Error(`${input.label} path or Secret Manager reference is required`)
}
