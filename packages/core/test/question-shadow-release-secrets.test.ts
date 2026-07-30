import { describe, expect, test } from "bun:test"
import { createSecretManager } from "../src/security/secret-manager"
import {
  readQuestionShadowReleaseInput,
  resolveQuestionShadowReleaseSecret,
} from "../src/question-shadow-release-secrets"

describe("QuestionShadowReleaseSecrets", () => {
  test("resolves the signing key from an env Secret Manager reference", async () => {
    const manager = createSecretManager({
      env: { P737_SIGNING_KEY: "secret-from-manager" },
      cacheTtlMs: 0,
    })
    expect(
      await resolveQuestionShadowReleaseSecret(
        {
          reference: "env://P737_SIGNING_KEY",
          label: "signing key",
        },
        manager,
      ),
    ).toBe("secret-from-manager")
  })

  test("resolves proof JSON from AWS Secrets Manager without exposing it as an argument", async () => {
    const manager = createSecretManager({
      cacheTtlMs: 0,
      fetchAwsSecret: async (region, secretID) =>
        JSON.stringify({ region, secretID, proof: '{"version":1}' }),
    })
    expect(
      await readQuestionShadowReleaseInput(
        {
          reference: "aws-sm://us-east-1/opencode/question-shadow#proof",
          label: "proof",
        },
        manager,
      ),
    ).toBe('{"version":1}')
  })
})
