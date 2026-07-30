export * as DurableEventManifest from "./durable-event-manifest"

import { Event } from "./event"
import { Question } from "./question"
import { SessionEvent } from "./session-event"
import { SessionV1 } from "./session-v1"
import { QuestionV1 } from "./v1/question"

export const SessionDurable = {
  definitions: Event.durable(SessionEvent.DurableDefinitions),
  schema: SessionEvent.Durable,
} as const

export const QuestionDurable = {
  definitions: Event.durable(Question.Event.Definitions),
  schema: Question.Event.All,
} as const

export const QuestionV1Durable = {
  definitions: Event.durable(QuestionV1.Event.Definitions),
  schema: QuestionV1.Event.All,
} as const

export const Durable = Event.durable([
  ...SessionV1.Event.Definitions.filter((definition) => definition.durable !== undefined),
  ...SessionEvent.DurableDefinitions,
  ...QuestionV1.Event.Definitions,
  ...Question.Event.Definitions,
])
