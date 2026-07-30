export type WorkerFenceToken = {
  readonly backend: "postgres"
  readonly runID: string
  readonly ownerID: string
  readonly fencingToken: number
}

export type ToolWorkerFence = WorkerFenceToken & {
  readonly idempotencyKey: string
}

export function toolWorkerFence(fence: WorkerFenceToken, toolCallID: string): ToolWorkerFence {
  return {
    ...fence,
    idempotencyKey: `${fence.runID}:${fence.fencingToken}:${toolCallID}`,
  }
}
