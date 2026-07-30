import { describe, expect, test } from "bun:test"
import { Question } from "@opencode-ai/schema/question"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { QuestionCommandGateway } from "@opencode-ai/core/question-command-gateway"
import { SessionV2 } from "@opencode-ai/core/session"

const requestID = Question.ID.ascending("que_p732_gateway")
const sessionID = SessionV2.ID.make("ses_p732_gateway")
const asked: Question.AskedData = {
  id: requestID,
  sessionID,
  questions: [
    {
      header: "Target",
      question: "Which target?",
      options: [
        { label: "Web", description: "Build the web target" },
        { label: "API", description: "Build the API target" },
      ],
    },
  ],
  location: {
    directory: AbsolutePath.make("/p732-question-gateway"),
  },
}

class MemoryStore implements QuestionCommandGateway.Store {
  readonly records = new Map<Question.ID, QuestionCommandGateway.EventRecord[]>()

  async read(id: Question.ID) {
    return [...(this.records.get(id) ?? [])]
  }

  async append(input: {
    readonly requestID: Question.ID
    readonly expectedSeq: number
    readonly event: QuestionCommandGateway.EventRecord["event"]
  }) {
    await Promise.resolve()
    const records = this.records.get(input.requestID) ?? []
    const actualSeq = records.at(-1)?.seq ?? -1
    if (actualSeq !== input.expectedSeq) {
      throw new QuestionCommandGateway.SequenceConflictError(input.requestID, input.expectedSeq, actualSeq)
    }
    const record = {
      seq: actualSeq + 1,
      event: input.event,
    }
    records.push(record)
    this.records.set(input.requestID, records)
    return record
  }
}

describe("QuestionCommandGateway", () => {
  test("creates an aggregate and makes identical ask retries idempotent", async () => {
    const store = new MemoryStore()
    const gateway = QuestionCommandGateway.make(store)

    expect((await gateway.ask(asked)).status).toBe("appended")
    expect((await gateway.ask(asked)).status).toBe("idempotent")
    expect(store.records.get(requestID)).toHaveLength(1)
  })

  test("serializes concurrent identical answers into one append", async () => {
    const store = new MemoryStore()
    const gateway = QuestionCommandGateway.make(store, { maxAttempts: 16 })
    await gateway.ask(asked)

    const results = await Promise.all(
      Array.from({ length: 12 }, () =>
        gateway.reply({
          requestID,
          sessionID,
          answers: [["Web"]],
        }),
      ),
    )

    expect(results.filter((result) => result.status === "appended")).toHaveLength(1)
    expect(results.filter((result) => result.status === "idempotent")).toHaveLength(11)
    expect(store.records.get(requestID)).toHaveLength(2)
    expect((await gateway.load(requestID)).state).toMatchObject({
      status: "answered",
      answers: [["Web"]],
    })
  })

  test("allows one conflicting terminal command and rejects the loser", async () => {
    const store = new MemoryStore()
    const gateway = QuestionCommandGateway.make(store)
    await gateway.ask(asked)

    const results = await Promise.allSettled([
      gateway.reply({ requestID, sessionID, answers: [["Web"]] }),
      gateway.reject({ requestID, sessionID }),
    ])

    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1)
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(1)
    const rejected = results.find((result) => result.status === "rejected")
    expect(rejected?.reason).toBeInstanceOf(QuestionAggregateConflict)
    expect(store.records.get(requestID)).toHaveLength(2)
  })

  test("fails closed on non-contiguous aggregate history", async () => {
    const store = new MemoryStore()
    const gateway = QuestionCommandGateway.make(store)
    const created = await gateway.ask(asked)
    expect(created.status).toBe("appended")
    store.records.get(requestID)!.push({
      seq: 2,
      event: {
        type: Question.Event.Rejected.type,
        version: Question.Event.Rejected.durable!.version,
        data: { requestID, sessionID },
      },
    })

    await expect(gateway.load(requestID)).rejects.toBeInstanceOf(QuestionCommandGateway.HistoryError)
  })
})

const QuestionAggregateConflict = Error
