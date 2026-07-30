import { describe, expect, test } from "bun:test"
import { publicSaasConfig } from "../../src/server/routes/instance/httpapi/config-public"

describe("public SaaS config", () => {
  test("removes nested credentials without mutating the runtime config", () => {
    const config = {
      model: "custom/code",
      provider: {
        custom: {
          options: {
            apiKey: "provider-secret",
            auth_token: "auth-secret",
            baseURL: "https://models.example.test/v1",
            nested: {
              clientSecret: "client-secret",
              region: "cn-hangzhou",
            },
          },
          models: {
            code: {
              headers: {
                Authorization: "Bearer model-secret",
                "X-Trace-ID": "safe-trace",
              },
            },
          },
        },
      },
      mcp: {
        internal: {
          headers: {
            "X-API-Key": "mcp-secret",
            Accept: "application/json",
          },
        },
      },
    }

    const result = publicSaasConfig(config)

    expect(result as unknown).toEqual({
      model: "custom/code",
      provider: {
        custom: {
          options: {
            baseURL: "https://models.example.test/v1",
            nested: { region: "cn-hangzhou" },
          },
          models: {
            code: {
              headers: {
                "X-Trace-ID": "safe-trace",
              },
            },
          },
        },
      },
      mcp: {
        internal: {
          headers: {
            Accept: "application/json",
          },
        },
      },
    })
    expect(config.provider.custom.options.apiKey).toBe("provider-secret")
    expect(config.provider.custom.models.code.headers.Authorization).toBe("Bearer model-secret")
  })

  test("removes credential aliases from arrays and custom provider options", () => {
    const result = publicSaasConfig({
      providers: [
        {
          options: {
            accessToken: "access-secret",
            refresh_token: "refresh-secret",
            secretAccessKey: "aws-secret",
            private_key: "private-secret",
            timeout: 30_000,
          },
        },
      ],
    })

    expect(result as unknown).toEqual({
      providers: [{ options: { timeout: 30_000 } }],
    })
  })
})
