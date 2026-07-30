import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Question } from "@opencode-ai/schema/question"
import { applyMigrations } from "@opencode-ai/core/database/postgres/migration"
import { makeClient } from "@opencode-ai/core/database/postgres/client"
import { QuestionCommandGateway } from "@opencode-ai/core/question-command-gateway"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"

const databaseURL = process.env.OPENCODE_P732_QUESTION_DATABASE_URL
const databaseName = databaseURL ? new URL(databaseURL).pathname.split("/").pop() : undefined
const run = databaseURL && databaseName?.startsWith("opencode_p732_") ? describe : describe.skip
const sql = databaseURL ? makeClient({ url: databaseURL, max: 12 }) : undefined

run("QuestionCommandGateway PostgreSQL", () => {
  beforeAll(async () => {
    await applyMigrations(sql!)
  })

  afterAll(async () => {
    await sql?.end()
  })

  test("uses expected sequence to serialize concurrent multi-process-style replies", async () => {
    const suffix = crypto.randomUUID()
    const requestID = Question.ID.ascending(`que_p732_${suffix}`)
    const sessionID = SessionV2.ID.make(`ses_p732_${suffix}`)
    const gateway = QuestionCommandGateway.makePostgres({
      sql: sql!,
      tenant: {
        tenantID: `tenant_p732_${suffix}`,
        actorID: `actor_p732_${suffix}`,
      },
      maxAttempts: 16,
    })

    await gateway.ask({
      id: requestID,
      sessionID,
      questions: [
        {
          header: "Target",
          question: "Which target?",
          options: [{ label: "Web", description: "Build the web target" }],
        },
      ],
      location: {
        directory: AbsolutePath.make("/p732-postgres"),
      },
    })

    const results = await Promise.all(
      Array.from({ length: 8 }, () =>
        gateway.reply({
          requestID,
          sessionID,
          answers: [["Web"]],
        }),
      ),
    )
    const snapshot = await gateway.load(requestID)

    expect(results.filter((result) => result.status === "appended")).toHaveLength(1)
    expect(results.filter((result) => result.status === "idempotent")).toHaveLength(7)
    expect(snapshot.latestSeq).toBe(1)
    expect(snapshot.records).toHaveLength(2)
    expect(snapshot.state).toMatchObject({ status: "answered", answers: [["Web"]] })
  })
})
