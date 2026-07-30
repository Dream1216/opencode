import { createSecretManager } from "../../security/secret-manager"
import { liveIntegrationPolicyFromEnv } from "./worker-queue-live-integration"

export async function runWorkerQueueCloudContractSmoke() {
  const checks: string[] = []
  const p441 = liveIntegrationPolicyFromEnv({
    OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER: "better-auth",
    OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF: "file:///run/secrets/session-cookie",
    OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL: "http://127.0.0.1:3001",
  })
  if (
    p441.provider !== "better-auth" ||
    p441.secretManagerScheme !== "file" ||
    p441.identityEndpointClass !== "loopback"
  ) {
    throw new Error("P4.41 compatibility policy was not preserved")
  }
  checks.push("p4-41-loopback-file-policy-compatible")

  expectPolicyRejected({
    OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED: "1",
    OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER: "better-auth",
    OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF: "aws-sm://us-east-1/opencode/session",
    OPENCODE_WORKER_QUEUE_BETTER_AUTH_URL: "https://auth.example.test",
  })
  checks.push("cloud-policy-non-oidc-blocked")

  expectPolicyRejected({
    OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED: "1",
    OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER: "oidc",
    OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF: "aws-sm://us-east-1/opencode/token",
    OPENCODE_WORKER_QUEUE_OIDC_ISSUER: "http://127.0.0.1:8080",
  })
  checks.push("cloud-policy-loopback-idp-blocked")

  expectPolicyRejected({
    OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED: "1",
    OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER: "oidc",
    OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF: "file:///run/secrets/oidc-token",
    OPENCODE_WORKER_QUEUE_OIDC_ISSUER: "https://identity.example.test",
  })
  checks.push("cloud-policy-mounted-file-blocked")

  const cloud = liveIntegrationPolicyFromEnv({
    OPENCODE_P4_42_CLOUD_INTEGRATION_REQUIRED: "1",
    OPENCODE_P4_41_LIVE_IDENTITY_PROVIDER: "oidc",
    OPENCODE_P4_41_LIVE_IDENTITY_CREDENTIAL_REF: "aws-sm://us-east-1/opencode/token",
    OPENCODE_WORKER_QUEUE_OIDC_ISSUER: "https://identity.example.test",
  })
  if (
    cloud.provider !== "oidc" ||
    cloud.secretManagerScheme !== "aws-sm" ||
    cloud.identityEndpointClass !== "remote"
  ) {
    throw new Error("P4.42 cloud policy did not accept remote OIDC with AWS Secrets Manager")
  }
  checks.push("remote-oidc-aws-sm-policy-ready")

  let credential = "cloud-credential-version-one"
  let outage = false
  const manager = createSecretManager({
    cacheTtlMs: 0,
    fetchAwsSecret: async () => {
      if (outage) throw new Error("simulated remote Secret Manager outage")
      return credential
    },
  })
  const reference = "aws-sm://us-east-1/opencode/identity-token"
  const first = await manager.resolve(reference, { refresh: true })
  credential = "cloud-credential-version-two"
  const rotated = await manager.resolve(reference, { refresh: true })
  if (first === rotated || rotated !== credential) {
    throw new Error("AWS Secrets Manager forced refresh did not observe credential rotation")
  }
  checks.push("aws-sm-forced-refresh-rotation-contract")

  outage = true
  await expectRejected(() => manager.resolve(reference, { refresh: true }))
  checks.push("aws-sm-outage-fails-closed-without-stale-fallback")
  return { status: "ok" as const, checks }
}

function expectPolicyRejected(env: NodeJS.ProcessEnv) {
  try {
    liveIntegrationPolicyFromEnv(env)
  } catch {
    return
  }
  throw new Error("Expected P4.42 cloud integration policy to reject configuration")
}

async function expectRejected(run: () => Promise<unknown>) {
  try {
    await run()
  } catch {
    return
  }
  throw new Error("Expected cloud dependency operation to fail closed")
}
