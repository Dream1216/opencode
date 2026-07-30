import { describe, expect, test } from "bun:test"
import { Question } from "@opencode-ai/schema/question"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { QuestionAggregate } from "@opencode-ai/core/question-aggregate"
import { SessionV2 } from "@opencode-ai/core/session"
import { Schema } from "effect"

const requestID = Question.ID.ascending("que_p73_aggregate")
const sessionID = SessionV2.ID.make("ses_p73_aggregate")
const asked: Question.AskedData = {
  id: requestID,
  sessionID,
  questions: [
    {
      header: "Target",
      question: "Which target?",
      options: [{ label: "Web", description: "Build the web target" }],
    },
  ],
  tool: {
    messageID: "msg_p73",
    callID: "call_p73",
  },
  location: {
    directory: AbsolutePath.make("/p73-question-aggregate"),
  },
}

function pending() {
  const decision = QuestionAggregate.decideAsk(QuestionAggregate.initial(), asked)
  if (decision.kind !== "append") throw new Error("Expected asked event")
  return {
    event: decision.event,
    state: QuestionAggregate.evolve(QuestionAggregate.initial(), decision.event),
  }
}

describe("QuestionAggregate", () => {
  test("uses the existing durable Question event contract", () => {
    const { event } = pending()
    expect(Schema.decodeUnknownSync(QuestionAggregate.StoredEvent)(event)).toEqual(event)
    expect(() =>
      Schema.decodeUnknownSync(QuestionAggregate.StoredEvent)({
        ...event,
        version: 2,
      }),
    ).toThrow()
  })

  test("folds asked and replied events into an answered aggregate", () => {
    const { event: askedEvent, state } = pending()
    const decision = QuestionAggregate.decideReply(state, {
      requestID,
      sessionID,
      answers: [["Web"]],
    })
    expect(decision.kind).toBe("append")
    if (decision.kind !== "append") return

    expect(QuestionAggregate.fold([askedEvent, decision.event])).toMatchObject({
      status: "answered",
      request: { id: requestID, sessionID },
      answers: [["Web"]],
    })
  })

  test("treats identical retries as idempotent and rejects conflicting terminal transitions", () => {
    const { state } = pending()
    const reply = QuestionAggregate.decideReply(state, {
      requestID,
      sessionID,
      answers: [["Web"]],
    })
    if (reply.kind !== "append") throw new Error("Expected replied event")
    const answered = QuestionAggregate.evolve(state, reply.event)

    expect(
      QuestionAggregate.decideReply(answered, {
        requestID,
        sessionID,
        answers: [["Web"]],
      }).kind,
    ).toBe("idempotent")
    expect(() =>
      QuestionAggregate.decideReply(answered, {
        requestID,
        sessionID,
        answers: [["API"]],
      }),
    ).toThrow(QuestionAggregate.ConflictError)
    expect(() => QuestionAggregate.decideReject(answered, { requestID, sessionID })).toThrow(
      QuestionAggregate.ConflictError,
    )
  })

  test("keeps rejection idempotent and rejects answers after dismissal", () => {
    const { state } = pending()
    const reject = QuestionAggregate.decideReject(state, { requestID, sessionID })
    if (reject.kind !== "append") throw new Error("Expected rejected event")
    const rejected = QuestionAggregate.evolve(state, reject.event)

    expect(QuestionAggregate.decideReject(rejected, { requestID, sessionID }).kind).toBe("idempotent")
    expect(() =>
      QuestionAggregate.decideReply(rejected, {
        requestID,
        sessionID,
        answers: [["Web"]],
      }),
    ).toThrow(QuestionAggregate.ConflictError)
  })

  test("rejects missing aggregates, wrong sessions, and incomplete answers", () => {
    expect(() =>
      QuestionAggregate.decideReply(QuestionAggregate.initial(), {
        requestID,
        sessionID,
        answers: [["Web"]],
      }),
    ).toThrow(QuestionAggregate.NotFoundError)

    const { state } = pending()
    expect(() =>
      QuestionAggregate.decideReply(state, {
        requestID,
        sessionID: SessionV2.ID.make("ses_wrong"),
        answers: [["Web"]],
      }),
    ).toThrow(QuestionAggregate.ConflictError)
    expect(() =>
      QuestionAggregate.decideReply(state, {
        requestID,
        sessionID,
        answers: [],
      }),
    ).toThrow(QuestionAggregate.ConflictError)
  })
})
