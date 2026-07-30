import { describe, expect, test } from "bun:test"
import { QuestionCommandShadow } from "@/question/command-shadow"

describe("QuestionCommandShadow Store config", () => {
  test("keeps memory as the default compatibility backend", () => {
    expect(QuestionCommandShadow.storeConfig({})).toEqual({ backend: "memory" })
  })

  test("requires a database URL for the PostgreSQL backend", () => {
    expect(() =>
      QuestionCommandShadow.storeConfig({
        OPENCODE_QUESTION_SHADOW_STORE_BACKEND: "postgres",
      }),
    ).toThrow("requires OPENCODE_QUESTION_SHADOW_STORE_DATABASE_URL or OPENCODE_DATABASE_URL")
  })

  test("builds an isolated PostgreSQL shadow Store config", () => {
    expect(
      QuestionCommandShadow.storeConfig({
        OPENCODE_QUESTION_SHADOW_STORE_BACKEND: "postgres",
        OPENCODE_DATABASE_URL: "postgresql://localhost/opencode",
        OPENCODE_QUESTION_SHADOW_STORE_TENANT_ID: "tenant_shadow_test",
        OPENCODE_QUESTION_SHADOW_STORE_ACTOR_ID: "actor_shadow_test",
        OPENCODE_QUESTION_SHADOW_STORE_DATABASE_MAX: "4",
      }),
    ).toEqual({
      backend: "postgres",
      url: "postgresql://localhost/opencode",
      tenantID: "tenant_shadow_test",
      actorID: "actor_shadow_test",
      poolMax: 4,
    })
  })
})
