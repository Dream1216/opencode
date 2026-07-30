export * as QuestionAggregate from "./question-aggregate"

import { Question } from "@opencode-ai/schema/question"
import { Schema } from "effect"

const AskedVersion = Question.Event.Asked.durable!.version
const RepliedVersion = Question.Event.Replied.durable!.version
const RejectedVersion = Question.Event.Rejected.durable!.version

const AskedEvent = Schema.Struct({
  type: Schema.Literal(Question.Event.Asked.type),
  version: Schema.Literal(AskedVersion),
  data: Question.Event.Asked.data,
})

const RepliedEvent = Schema.Struct({
  type: Schema.Literal(Question.Event.Replied.type),
  version: Schema.Literal(RepliedVersion),
  data: Question.Event.Replied.data,
})

const RejectedEvent = Schema.Struct({
  type: Schema.Literal(Question.Event.Rejected.type),
  version: Schema.Literal(RejectedVersion),
  data: Question.Event.Rejected.data,
})

export const StoredEvent = Schema.Union([AskedEvent, RepliedEvent, RejectedEvent]).annotate({
  identifier: "QuestionAggregate.StoredEvent",
})
export type StoredEvent = typeof StoredEvent.Type

export interface Empty {
  readonly status: "empty"
}

export interface Pending {
  readonly status: "pending"
  readonly request: Question.Request
  readonly location: Question.AskedData["location"]
}

export interface Answered {
  readonly status: "answered"
  readonly request: Question.Request
  readonly location: Question.AskedData["location"]
  readonly answers: ReadonlyArray<Question.Answer>
}

export interface Rejected {
  readonly status: "rejected"
  readonly request: Question.Request
  readonly location: Question.AskedData["location"]
}

export type State = Empty | Pending | Answered | Rejected

export type Decision =
  | {
      readonly kind: "append"
      readonly event: StoredEvent
    }
  | {
      readonly kind: "idempotent"
      readonly state: Exclude<State, Empty>
    }

export class NotFoundError extends Error {
  readonly code = "question_not_found"

  constructor(readonly requestID: Question.ID) {
    super(`Question aggregate not found: ${requestID}`)
    this.name = "QuestionAggregate.NotFoundError"
  }
}

export class ConflictError extends Error {
  readonly code = "question_transition_conflict"

  constructor(
    readonly requestID: Question.ID,
    message: string,
  ) {
    super(message)
    this.name = "QuestionAggregate.ConflictError"
  }
}

export function initial(): State {
  return { status: "empty" }
}

export function decideAsk(state: State, data: Question.AskedData): Decision {
  const event: StoredEvent = {
    type: Question.Event.Asked.type,
    version: AskedVersion,
    data,
  }
  if (state.status === "empty") return { kind: "append", event }
  return { kind: "idempotent", state: requireMaterialized(evolve(state, event)) }
}

export function decideReply(
  state: State,
  input: {
    readonly requestID: Question.ID
    readonly sessionID: Question.Request["sessionID"]
    readonly answers: ReadonlyArray<Question.Answer>
  },
): Decision {
  if (state.status === "empty") throw new NotFoundError(input.requestID)
  const event: StoredEvent = {
    type: Question.Event.Replied.type,
    version: RepliedVersion,
    data: {
      requestID: input.requestID,
      sessionID: input.sessionID,
      answers: input.answers.map((answer) => [...answer]),
    },
  }
  if (state.status === "pending") {
    evolve(state, event)
    return { kind: "append", event }
  }
  return { kind: "idempotent", state: requireMaterialized(evolve(state, event)) }
}

export function decideReject(
  state: State,
  input: {
    readonly requestID: Question.ID
    readonly sessionID: Question.Request["sessionID"]
  },
): Decision {
  if (state.status === "empty") throw new NotFoundError(input.requestID)
  const event: StoredEvent = {
    type: Question.Event.Rejected.type,
    version: RejectedVersion,
    data: input,
  }
  if (state.status === "pending") {
    evolve(state, event)
    return { kind: "append", event }
  }
  return { kind: "idempotent", state: requireMaterialized(evolve(state, event)) }
}

export function evolve(state: State, event: StoredEvent): State {
  if (event.type === Question.Event.Asked.type) {
    const request = toRequest(event.data)
    if (state.status === "empty") {
      return {
        status: "pending",
        request,
        location: event.data.location,
      }
    }
    if (sameRequest(state.request, request) && sameLocation(state.location, event.data.location)) return state
    throw new ConflictError(event.data.id, `Question ${event.data.id} was already created with different data`)
  }

  const requestID = event.data.requestID
  if (state.status === "empty") {
    throw new ConflictError(requestID, `Question ${requestID} has a terminal event before it was asked`)
  }
  if (state.request.id !== requestID) {
    throw new ConflictError(requestID, `Question event ${requestID} does not match aggregate ${state.request.id}`)
  }
  if (state.request.sessionID !== event.data.sessionID) {
    throw new ConflictError(requestID, `Question ${requestID} session does not match the aggregate`)
  }

  if (event.type === Question.Event.Replied.type) {
    validateAnswers(state.request, event.data.answers)
    if (state.status === "pending") {
      return {
        status: "answered",
        request: state.request,
        location: state.location,
        answers: event.data.answers,
      }
    }
    if (state.status === "answered" && sameAnswers(state.answers, event.data.answers)) return state
    throw new ConflictError(requestID, `Question ${requestID} is already ${state.status}`)
  }

  if (state.status === "pending") {
    return {
      status: "rejected",
      request: state.request,
      location: state.location,
    }
  }
  if (state.status === "rejected") return state
  throw new ConflictError(requestID, `Question ${requestID} is already answered`)
}

export function fold(events: Iterable<StoredEvent>): State {
  let state: State = initial()
  for (const event of events) state = evolve(state, event)
  return state
}

function requireMaterialized(state: State): Exclude<State, Empty> {
  if (state.status === "empty") throw new Error("Question aggregate did not materialize")
  return state
}

function validateAnswers(request: Question.Request, answers: ReadonlyArray<Question.Answer>) {
  if (answers.length !== request.questions.length) {
    throw new ConflictError(
      request.id,
      `Question ${request.id} expected ${request.questions.length} answers but received ${answers.length}`,
    )
  }
}

function toRequest(data: Question.AskedData): Question.Request {
  return {
    id: data.id,
    sessionID: data.sessionID,
    questions: data.questions,
    ...(data.tool === undefined ? {} : { tool: data.tool }),
  }
}

function sameRequest(left: Question.Request, right: Question.Request) {
  return (
    left.id === right.id &&
    left.sessionID === right.sessionID &&
    left.tool?.messageID === right.tool?.messageID &&
    left.tool?.callID === right.tool?.callID &&
    left.questions.length === right.questions.length &&
    left.questions.every((question, index) => sameQuestion(question, right.questions[index]))
  )
}

function sameQuestion(left: Question.Info, right: Question.Info | undefined) {
  return (
    right !== undefined &&
    left.question === right.question &&
    left.header === right.header &&
    left.multiple === right.multiple &&
    left.custom === right.custom &&
    left.options.length === right.options.length &&
    left.options.every(
      (option, index) =>
        option.label === right.options[index]?.label && option.description === right.options[index]?.description,
    )
  )
}

function sameLocation(left: Question.AskedData["location"], right: Question.AskedData["location"]) {
  return left.directory === right.directory && left.workspaceID === right.workspaceID
}

function sameAnswers(left: ReadonlyArray<Question.Answer>, right: ReadonlyArray<Question.Answer>) {
  return (
    left.length === right.length &&
    left.every(
      (answer, index) =>
        answer.length === right[index]?.length && answer.every((value, answerIndex) => value === right[index]?.[answerIndex]),
    )
  )
}
