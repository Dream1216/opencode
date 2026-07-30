import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { Authorization } from "../middleware/authorization"

const root = "/experimental/worker-queue"

export const WorkerQueueAdminPaths = {
  readiness: `${root}/readiness`,
  metrics: `${root}/metrics`,
  recoverable: `${root}/recoverable`,
  requeue: `${root}/actions/requeue`,
  action: `${root}/actions/:actionID`,
  approve: `${root}/actions/:actionID/approve`,
  revoke: `${root}/actions/:actionID/approvals/:actorID/revoke`,
  expire: `${root}/actions/expire`,
  breakGlassRequeue: `${root}/actions/break-glass/requeue`,
} as const

export const WorkerQueueAdminApi = HttpApi.make("workerQueueAdmin").add(
  HttpApiGroup.make("workerQueueAdmin")
    .add(
      HttpApiEndpoint.get("workerQueueReadiness", WorkerQueueAdminPaths.readiness, {
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.get("workerQueueMetrics", WorkerQueueAdminPaths.metrics, {
        success: Schema.String,
      }),
      HttpApiEndpoint.get("workerQueueRecoverable", WorkerQueueAdminPaths.recoverable, {
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.post("workerQueueRequeue", WorkerQueueAdminPaths.requeue, {
        payload: Schema.Unknown,
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.get("workerQueueAction", WorkerQueueAdminPaths.action, {
        params: { actionID: Schema.String },
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.post("workerQueueApprove", WorkerQueueAdminPaths.approve, {
        params: { actionID: Schema.String },
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.post("workerQueueRevokeApproval", WorkerQueueAdminPaths.revoke, {
        params: { actionID: Schema.String, actorID: Schema.String },
        payload: Schema.Unknown,
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.post("workerQueueExpireApprovals", WorkerQueueAdminPaths.expire, {
        success: Schema.Unknown,
      }),
      HttpApiEndpoint.post("workerQueueBreakGlassRequeue", WorkerQueueAdminPaths.breakGlassRequeue, {
        payload: Schema.Unknown,
        success: Schema.Unknown,
      }),
    )
    .annotateMerge(
      OpenApi.annotations({
        title: "worker queue administration",
        description: "Signed, tenant- and team-authorized durable worker queue operations.",
      }),
    ),
).middleware(Authorization)
