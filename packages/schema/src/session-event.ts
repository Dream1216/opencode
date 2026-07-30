export * as SessionEvent from "./session-event"

import { Schema } from "effect"
import { optional } from "./schema"
import { Event } from "./event"
import { ProviderMetadata, ToolContent } from "./llm"
import { Delivery } from "./session-delivery"
import { Model } from "./model"
import { DateTimeUtcFromMillis, NonNegativeInt, RelativePath } from "./schema"
import { FileAttachment, Prompt } from "./prompt"
import { SessionID } from "./session-id"
import { Location } from "./location"
import { SessionMessage } from "./session-message"
import { Revert } from "./revert"

export { FileAttachment }

export const Source = Schema.Struct({
  start: NonNegativeInt,
  end: NonNegativeInt,
  text: Schema.String,
}).annotate({
  identifier: "session.next.event.source",
})
export interface Source extends Schema.Schema.Type<typeof Source> {}

const Base = {
  timestamp: DateTimeUtcFromMillis,
  sessionID: SessionID,
}
const PromptFields = {
  ...Base,
  messageID: SessionMessage.ID,
  prompt: Prompt,
  delivery: Delivery,
}

const options = {
  durable: {
    aggregate: "sessionID",
    version: 1,
  },
} as const
const stepSettlementOptions = {
  durable: {
    aggregate: "sessionID",
    version: 2,
  },
} as const

export const UnknownError = SessionMessage.UnknownError
export type UnknownError = SessionMessage.UnknownError

export const AgentSwitched = Event.define({
  type: "session.next.agent.switched",
  ...options,
  schema: {
    ...Base,
    messageID: SessionMessage.ID,
    agent: Schema.String,
  },
})
export type AgentSwitched = typeof AgentSwitched.Type

export const ModelSwitched = Event.define({
  type: "session.next.model.switched",
  ...options,
  schema: {
    ...Base,
    messageID: SessionMessage.ID,
    model: Model.Ref,
  },
})
export type ModelSwitched = typeof ModelSwitched.Type

export const Moved = Event.define({
  type: "session.next.moved",
  ...options,
  schema: {
    ...Base,
    location: Location.Ref,
    subdirectory: RelativePath.pipe(optional),
  },
})
export type Moved = typeof Moved.Type

export const Prompted = Event.define({
  type: "session.next.prompted",
  ...options,
  schema: PromptFields,
})
export type Prompted = typeof Prompted.Type

export const PromptAdmitted = Event.define({
  type: "session.next.prompt.admitted",
  ...options,
  schema: PromptFields,
})
export type PromptAdmitted = typeof PromptAdmitted.Type

export const ContextUpdated = Event.define({
  type: "session.next.context.updated",
  ...options,
  schema: {
    ...Base,
    messageID: SessionMessage.ID,
    text: Schema.String,
  },
})
export type ContextUpdated = typeof ContextUpdated.Type

export const Synthetic = Event.define({
  type: "session.next.synthetic",
  ...options,
  schema: {
    ...Base,
    messageID: SessionMessage.ID,
    text: Schema.String,
  },
})
export type Synthetic = typeof Synthetic.Type

export namespace Shell {
  export const Started = Event.define({
    type: "session.next.shell.started",
    ...options,
    schema: {
      ...Base,
      messageID: SessionMessage.ID,
      callID: Schema.String,
      command: Schema.String,
    },
  })
  export type Started = typeof Started.Type

  export const Ended = Event.define({
    type: "session.next.shell.ended",
    ...options,
    schema: {
      ...Base,
      callID: Schema.String,
      output: Schema.String,
    },
  })
  export type Ended = typeof Ended.Type
}

export namespace Step {
  export const Started = Event.define({
    type: "session.next.step.started",
    ...options,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      agent: Schema.String,
      model: Model.Ref,
      snapshot: Schema.String.pipe(optional),
    },
  })
  export type Started = typeof Started.Type

  export const Ended = Event.define({
    type: "session.next.step.ended",
    ...stepSettlementOptions,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      finish: Schema.String,
      cost: Schema.Finite,
      tokens: Schema.Struct({
        input: Schema.Finite,
        output: Schema.Finite,
        reasoning: Schema.Finite,
        cache: Schema.Struct({
          read: Schema.Finite,
          write: Schema.Finite,
        }),
      }),
      snapshot: Schema.String.pipe(optional),
      files: Schema.Array(RelativePath).pipe(optional),
    },
  })
  export type Ended = typeof Ended.Type

  export const Failed = Event.define({
    type: "session.next.step.failed",
    ...stepSettlementOptions,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      error: UnknownError,
    },
  })
  export type Failed = typeof Failed.Type
}

export namespace Text {
  export const Started = Event.define({
    type: "session.next.text.started",
    ...options,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      textID: Schema.String,
    },
  })
  export type Started = typeof Started.Type

  // Stream fragments are live-only; Text.Ended is the replayable full-value boundary.
  export const Delta = Event.define({
    type: "session.next.text.delta",
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      textID: Schema.String,
      delta: Schema.String,
    },
  })
  export type Delta = typeof Delta.Type

  export const Ended = Event.define({
    type: "session.next.text.ended",
    ...options,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      textID: Schema.String,
      text: Schema.String,
    },
  })
  export type Ended = typeof Ended.Type
}

export namespace Reasoning {
  export const Started = Event.define({
    type: "session.next.reasoning.started",
    ...options,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      reasoningID: Schema.String,
      providerMetadata: ProviderMetadata.pipe(optional),
    },
  })
  export type Started = typeof Started.Type

  // Stream fragments are live-only; Reasoning.Ended is the replayable full-value boundary.
  export const Delta = Event.define({
    type: "session.next.reasoning.delta",
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      reasoningID: Schema.String,
      delta: Schema.String,
    },
  })
  export type Delta = typeof Delta.Type

  export const Ended = Event.define({
    type: "session.next.reasoning.ended",
    ...options,
    schema: {
      ...Base,
      assistantMessageID: SessionMessage.ID,
      reasoningID: Schema.String,
      text: Schema.String,
      providerMetadata: ProviderMetadata.pipe(optional),
    },
  })
  export type Ended = typeof Ended.Type
}

export namespace Tool {
  const ToolBase = {
    ...Base,
    assistantMessageID: SessionMessage.ID,
    callID: Schema.String,
  }

  export namespace Input {
    export const Started = Event.define({
      type: "session.next.tool.input.started",
      ...options,
      schema: {
        ...ToolBase,
        name: Schema.String,
      },
    })
    export type Started = typeof Started.Type

    // Stream fragments are live-only; Input.Ended is the replayable raw-input boundary.
    export const Delta = Event.define({
      type: "session.next.tool.input.delta",
      schema: {
        ...ToolBase,
        delta: Schema.String,
      },
    })
    export type Delta = typeof Delta.Type

    export const Ended = Event.define({
      type: "session.next.tool.input.ended",
      ...options,
      schema: {
        ...ToolBase,
        text: Schema.String,
      },
    })
    export type Ended = typeof Ended.Type
  }

  export const Called = Event.define({
    type: "session.next.tool.called",
    ...options,
    schema: {
      ...ToolBase,
      tool: Schema.String,
      input: Schema.Record(Schema.String, Schema.Unknown),
      provider: Schema.Struct({
        executed: Schema.Boolean,
        metadata: ProviderMetadata.pipe(optional),
      }),
    },
  })
  export type Called = typeof Called.Type

  /**
   * Replayable bounded running-tool state. Tools should checkpoint semantic
   * transitions or at a bounded cadence, not persist every stdout/stderr chunk.
   */
  export const Progress = Event.define({
    type: "session.next.tool.progress",
    ...options,
    schema: {
      ...ToolBase,
      structured: Schema.Record(Schema.String, Schema.Unknown),
      content: Schema.Array(ToolContent),
    },
  })
  export type Progress = typeof Progress.Type

  export const Success = Event.define({
    type: "session.next.tool.success",
    ...options,
    schema: {
      ...ToolBase,
      structured: Schema.Record(Schema.String, Schema.Unknown),
      content: Schema.Array(ToolContent),
      outputPaths: Schema.Array(Schema.String).pipe(optional),
      result: Schema.Unknown.pipe(optional),
      provider: Schema.Struct({
        executed: Schema.Boolean,
        metadata: ProviderMetadata.pipe(optional),
      }),
    },
  })
  export type Success = typeof Success.Type

  export const Failed = Event.define({
    type: "session.next.tool.failed",
    ...options,
    schema: {
      ...ToolBase,
      error: UnknownError,
      result: Schema.Unknown.pipe(optional),
      provider: Schema.Struct({
        executed: Schema.Boolean,
        metadata: ProviderMetadata.pipe(optional),
      }),
    },
  })
  export type Failed = typeof Failed.Type
}

export const RetryError = Schema.Struct({
  message: Schema.String,
  statusCode: Schema.Finite.pipe(optional),
  isRetryable: Schema.Boolean,
  responseHeaders: Schema.Record(Schema.String, Schema.String).pipe(optional),
  responseBody: Schema.String.pipe(optional),
  metadata: Schema.Record(Schema.String, Schema.String).pipe(optional),
}).annotate({
  identifier: "session.next.retry_error",
})
export interface RetryError extends Schema.Schema.Type<typeof RetryError> {}

export const Retried = Event.define({
  type: "session.next.retried",
  ...options,
  schema: {
    ...Base,
    attempt: Schema.Finite,
    error: RetryError,
  },
})
export type Retried = typeof Retried.Type

export const ModelInvocationUsage = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  reasoning: Schema.Finite,
  cache: Schema.Struct({
    read: Schema.Finite,
    write: Schema.Finite,
  }),
  total: Schema.Finite,
}).annotate({ identifier: "session.next.model_invocation.usage" })
export type ModelInvocationUsage = typeof ModelInvocationUsage.Type

export const ModelInvocationCost = Schema.Struct({
  total: Schema.Finite,
  currency: Schema.String,
}).annotate({ identifier: "session.next.model_invocation.cost" })
export type ModelInvocationCost = typeof ModelInvocationCost.Type

export const ModelInvocationError = Schema.Struct({
  type: Schema.String,
  message: Schema.String,
  retryable: Schema.Boolean.pipe(optional),
  classification: Schema.Unknown.pipe(optional),
  providerMetadata: Schema.Unknown.pipe(optional),
}).annotate({ identifier: "session.next.model_invocation.error" })
export type ModelInvocationError = typeof ModelInvocationError.Type

export namespace ModelInvocation {
  const InvocationBase = {
    ...Base,
    invocationID: Schema.String,
    attempt: Schema.Finite,
    agent: Schema.String,
    model: Model.Ref,
  }

  export const Attempted = Event.define({
    type: "session.next.model.invocation.attempted",
    ...options,
    schema: {
      ...InvocationBase,
      provider: Schema.String,
      route: Schema.String,
    },
  })
  export type Attempted = typeof Attempted.Type

  export const Retried = Event.define({
    type: "session.next.model.invocation.retried",
    ...options,
    schema: {
      ...InvocationBase,
      error: ModelInvocationError,
    },
  })
  export type Retried = typeof Retried.Type

  export const Usage = Event.define({
    type: "session.next.model.invocation.usage",
    ...options,
    schema: {
      ...InvocationBase,
      usage: ModelInvocationUsage,
      cost: ModelInvocationCost,
    },
  })
  export type Usage = typeof Usage.Type

  export const Completed = Event.define({
    type: "session.next.model.invocation.completed",
    ...options,
    schema: {
      ...InvocationBase,
      durationMs: Schema.Finite,
      usage: ModelInvocationUsage,
      cost: ModelInvocationCost,
    },
  })
  export type Completed = typeof Completed.Type

  export const Failed = Event.define({
    type: "session.next.model.invocation.failed",
    ...options,
    schema: {
      ...InvocationBase,
      durationMs: Schema.Finite,
      error: ModelInvocationError,
      usage: ModelInvocationUsage,
      cost: ModelInvocationCost,
    },
  })
  export type Failed = typeof Failed.Type
}

export namespace ShadowCanary {
  const CanaryBase = {
    ...Base,
    invocationID: Schema.String,
    targetVersion: Schema.String,
  }

  export const Dispatched = Event.define({
    type: "session.next.shadow.canary.dispatched",
    ...options,
    schema: {
      ...CanaryBase,
      requestDigest: Schema.String,
      payloadBytes: Schema.Finite,
      sampleRate: Schema.Finite,
    },
  })
  export type Dispatched = typeof Dispatched.Type

  export const Skipped = Event.define({
    type: "session.next.shadow.canary.skipped",
    ...options,
    schema: {
      ...CanaryBase,
      reason: Schema.String,
      sampleRate: Schema.Finite,
    },
  })
  export type Skipped = typeof Skipped.Type

  export const Accepted = Event.define({
    type: "session.next.shadow.canary.accepted",
    ...options,
    schema: {
      ...CanaryBase,
      requestDigest: Schema.String,
      durationMs: Schema.Finite,
      statusCode: Schema.Finite,
      receiptID: Schema.String.pipe(optional),
    },
  })
  export type Accepted = typeof Accepted.Type

  export const Failed = Event.define({
    type: "session.next.shadow.canary.failed",
    ...options,
    schema: {
      ...CanaryBase,
      requestDigest: Schema.String,
      durationMs: Schema.Finite,
      failureType: Schema.String,
      error: Schema.String,
      statusCode: Schema.Finite.pipe(optional),
    },
  })
  export type Failed = typeof Failed.Type
}

export const WorkerError = Schema.Struct({
  type: Schema.String,
  message: Schema.String,
}).annotate({ identifier: "session.next.worker.error" })
export type WorkerError = typeof WorkerError.Type

export namespace Worker {
  export const LeaseInfo = Schema.Struct({
    leaseID: Schema.String,
    ownerID: Schema.String,
    ttlMs: Schema.Finite,
    backend: Schema.Literals(["local", "postgres"]).pipe(optional),
    fencingToken: Schema.Int.pipe(optional),
    expiresAt: Schema.Finite.pipe(optional),
  }).annotate({ identifier: "session.next.worker.lease" })
  export type LeaseInfo = typeof LeaseInfo.Type

  export const Lease = Event.define({
    type: "session.next.worker.lease",
    ...options,
    schema: {
      ...Base,
      phase: Schema.Literals(["acquired", "heartbeat", "released", "contended"]),
      lease: LeaseInfo,
      reason: Schema.String.pipe(optional),
    },
  })
  export type Lease = typeof Lease.Type

  export const Scheduled = Event.define({
    type: "session.next.worker.scheduled",
    ...options,
    schema: {
      ...Base,
      reason: Schema.Literals(["wake", "resume"]),
    },
  })
  export type Scheduled = typeof Scheduled.Type

  export const Resumed = Event.define({
    type: "session.next.worker.resumed",
    ...options,
    schema: Base,
  })
  export type Resumed = typeof Resumed.Type

  export const Started = Event.define({
    type: "session.next.worker.started",
    ...options,
    schema: {
      ...Base,
      force: Schema.Boolean,
    },
  })
  export type Started = typeof Started.Type

  export const StopRequested = Event.define({
    type: "session.next.worker.stop_requested",
    ...options,
    schema: Base,
  })
  export type StopRequested = typeof StopRequested.Type

  export const Stopped = Event.define({
    type: "session.next.worker.stopped",
    ...options,
    schema: {
      ...Base,
      reason: Schema.String,
    },
  })
  export type Stopped = typeof Stopped.Type

  export const Completed = Event.define({
    type: "session.next.worker.completed",
    ...options,
    schema: Base,
  })
  export type Completed = typeof Completed.Type

  export const Failed = Event.define({
    type: "session.next.worker.failed",
    ...options,
    schema: {
      ...Base,
      error: WorkerError,
    },
  })
  export type Failed = typeof Failed.Type
}

export const WorkflowNode = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  label: Schema.String,
  config: Schema.Record(Schema.String, Schema.Unknown).pipe(optional),
}).annotate({ identifier: "session.next.workflow.node" })
export type WorkflowNode = typeof WorkflowNode.Type

export const WorkflowEdge = Schema.Struct({
  id: Schema.String,
  source: Schema.String,
  target: Schema.String,
  condition: Schema.String.pipe(optional),
}).annotate({ identifier: "session.next.workflow.edge" })
export type WorkflowEdge = typeof WorkflowEdge.Type

export const WorkflowDefinition = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  nodes: Schema.Array(WorkflowNode),
  edges: Schema.Array(WorkflowEdge),
  settings: Schema.Record(Schema.String, Schema.Unknown),
}).annotate({ identifier: "session.next.workflow.definition" })
export type WorkflowDefinition = typeof WorkflowDefinition.Type

export const WorkflowVersion = Schema.Struct({
  id: Schema.String,
  definitionID: Schema.String,
  version: Schema.String,
  runtime: Schema.Literal("opencode-main-loop-sidecar"),
  checksum: Schema.String,
}).annotate({ identifier: "session.next.workflow.version" })
export type WorkflowVersion = typeof WorkflowVersion.Type

export const WorkflowComplexity = Schema.Struct({
  score: Schema.Finite,
  reasons: Schema.Array(Schema.String),
}).annotate({ identifier: "session.next.workflow.complexity" })
export type WorkflowComplexity = typeof WorkflowComplexity.Type

export namespace Workflow {
  export const Bound = Event.define({
    type: "session.next.workflow.bound",
    ...options,
    schema: {
      ...Base,
      messageID: SessionMessage.ID,
      complexity: WorkflowComplexity,
      definition: WorkflowDefinition,
      version: WorkflowVersion,
    },
  })
  export type Bound = typeof Bound.Type
}

export const TenantContext = Schema.Struct({
  tenantID: Schema.String,
  teamID: Schema.String.pipe(optional),
  actorID: Schema.String,
  workspaceID: Schema.String.pipe(optional),
  source: Schema.Literals(["env", "local-default"]),
  mode: Schema.Literals(["single-tenant-local", "multi-tenant"]),
}).annotate({ identifier: "session.next.tenant.context" })
export type TenantContext = typeof TenantContext.Type

export const GovernanceResource = Schema.Struct({
  type: Schema.String,
  id: Schema.String,
}).annotate({ identifier: "session.next.governance.resource" })
export type GovernanceResource = typeof GovernanceResource.Type

export namespace Tenant {
  export const Bound = Event.define({
    type: "session.next.tenant.bound",
    ...options,
    schema: {
      ...Base,
      tenant: TenantContext,
    },
  })
  export type Bound = typeof Bound.Type
}

export const RlsPolicy = Schema.Struct({
  mode: Schema.Literals(["logical-sqlite", "postgres-rls"]),
  enforced: Schema.Boolean,
  result: Schema.Literals(["allow", "deny"]),
  checks: Schema.Array(Schema.String),
  reason: Schema.String,
}).annotate({ identifier: "session.next.rls.policy" })
export type RlsPolicy = typeof RlsPolicy.Type

export namespace Rls {
  export const Evaluated = Event.define({
    type: "session.next.rls.evaluated",
    ...options,
    schema: {
      ...Base,
      operation: Schema.String,
      resource: GovernanceResource,
      tenant: TenantContext,
      policy: RlsPolicy,
    },
  })
  export type Evaluated = typeof Evaluated.Type
}

export namespace Audit {
  export const Recorded = Event.define({
    type: "session.next.audit.recorded",
    ...options,
    schema: {
      ...Base,
      action: Schema.String,
      resource: GovernanceResource,
      outcome: Schema.Literals(["success", "failure", "denied"]),
      tenant: TenantContext,
      metadata: Schema.Record(Schema.String, Schema.Unknown).pipe(optional),
    },
  })
  export type Recorded = typeof Recorded.Type
}

export const ReleaseGate = Schema.Struct({
  channel: Schema.String,
  version: Schema.String,
  environment: Schema.String,
  gate: Schema.Literals(["local-development", "saas-ready"]),
  result: Schema.Literals(["allow", "warn", "deny"]),
  checks: Schema.Array(Schema.String),
}).annotate({ identifier: "session.next.release.governance.gate" })
export type ReleaseGate = typeof ReleaseGate.Type

export namespace ReleaseGovernance {
  export const Evaluated = Event.define({
    type: "session.next.release.governance.evaluated",
    ...options,
    schema: {
      ...Base,
      tenant: TenantContext,
      release: ReleaseGate,
    },
  })
  export type Evaluated = typeof Evaluated.Type
}

export namespace Compaction {
  export const Started = Event.define({
    type: "session.next.compaction.started",
    ...options,
    schema: {
      ...Base,
      messageID: SessionMessage.ID,
      reason: Schema.Union([Schema.Literal("auto"), Schema.Literal("manual")]),
    },
  })
  export type Started = typeof Started.Type

  export const Delta = Event.define({
    type: "session.next.compaction.delta",
    schema: {
      ...Base,
      messageID: SessionMessage.ID,
      text: Schema.String,
    },
  })
  export type Delta = typeof Delta.Type

  export const Ended = Event.define({
    type: "session.next.compaction.ended",
    ...options,
    schema: {
      ...Base,
      messageID: SessionMessage.ID,
      reason: Started.data.fields.reason,
      text: Schema.String,
      recent: Schema.String,
    },
  })
  export type Ended = typeof Ended.Type
}

export namespace RevertEvent {
  export const Staged = Event.define({
    type: "session.next.revert.staged",
    ...options,
    schema: { ...Base, revert: Revert.State },
  })
  export const Cleared = Event.define({ type: "session.next.revert.cleared", ...options, schema: Base })
  export const Committed = Event.define({
    type: "session.next.revert.committed",
    ...options,
    schema: { ...Base, messageID: SessionMessage.ID },
  })
}

export const DurableDefinitions = Event.inventory(
  AgentSwitched,
  ModelSwitched,
  Moved,
  Prompted,
  PromptAdmitted,
  ContextUpdated,
  Synthetic,
  Shell.Started,
  Shell.Ended,
  Step.Started,
  Step.Ended,
  Step.Failed,
  Text.Started,
  Text.Ended,
  Tool.Input.Started,
  Tool.Input.Ended,
  Tool.Called,
  Tool.Progress,
  Tool.Success,
  Tool.Failed,
  Reasoning.Started,
  Reasoning.Ended,
  Retried,
  ModelInvocation.Attempted,
  ModelInvocation.Retried,
  ModelInvocation.Usage,
  ModelInvocation.Completed,
  ModelInvocation.Failed,
  Worker.Scheduled,
  Worker.Resumed,
  Worker.Started,
  Worker.StopRequested,
  Worker.Stopped,
  Worker.Completed,
  Worker.Failed,
  Worker.Lease,
  Workflow.Bound,
  Tenant.Bound,
  Rls.Evaluated,
  Audit.Recorded,
  ReleaseGovernance.Evaluated,
  Compaction.Started,
  Compaction.Ended,
  RevertEvent.Staged,
  RevertEvent.Cleared,
  RevertEvent.Committed,
)

export const Definitions = Event.inventory(
  AgentSwitched,
  ModelSwitched,
  Moved,
  Prompted,
  PromptAdmitted,
  ContextUpdated,
  Synthetic,
  Shell.Started,
  Shell.Ended,
  Step.Started,
  Step.Ended,
  Step.Failed,
  Text.Started,
  Text.Delta,
  Text.Ended,
  Reasoning.Started,
  Reasoning.Delta,
  Reasoning.Ended,
  Tool.Input.Started,
  Tool.Input.Delta,
  Tool.Input.Ended,
  Tool.Called,
  Tool.Progress,
  Tool.Success,
  Tool.Failed,
  Retried,
  ModelInvocation.Attempted,
  ModelInvocation.Retried,
  ModelInvocation.Usage,
  ModelInvocation.Completed,
  ModelInvocation.Failed,
  Worker.Scheduled,
  Worker.Resumed,
  Worker.Started,
  Worker.StopRequested,
  Worker.Stopped,
  Worker.Completed,
  Worker.Failed,
  Worker.Lease,
  Workflow.Bound,
  Tenant.Bound,
  Rls.Evaluated,
  Audit.Recorded,
  ReleaseGovernance.Evaluated,
  Compaction.Started,
  Compaction.Delta,
  Compaction.Ended,
  RevertEvent.Staged,
  RevertEvent.Cleared,
  RevertEvent.Committed,
)

export const Durable = Schema.Union(DurableDefinitions, { mode: "oneOf" })
  .pipe(Schema.toTaggedUnion("type"))
  .annotate({ identifier: "SessionDurableEvent" })
export type DurableEvent = typeof Durable.Type

export const All = Schema.Union(Definitions, { mode: "oneOf" }).pipe(Schema.toTaggedUnion("type"))
export type Event = typeof All.Type
export type Type = Event["type"]
