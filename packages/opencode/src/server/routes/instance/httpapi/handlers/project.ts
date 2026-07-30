import * as InstanceState from "@/effect/instance-state"
import { Project } from "@/project/project"
import { ProjectV2 } from "@opencode-ai/core/project"
import { Effect } from "effect"
import { HttpServerRequest } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { ExecutionResourceBinding } from "@opencode-ai/core/identity/execution-resource-binding"
import { InstanceHttpApi } from "../api"
import { ProjectNotFoundError } from "../errors"
import { markInstanceForReload } from "../lifecycle"

export const projectHandlers = HttpApiBuilder.group(InstanceHttpApi, "project", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* Project.Service
    const project = yield* ProjectV2.Service

    const executionContext = Effect.fn("ProjectHttpApi.executionContext")(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest
      return ExecutionResourceBinding.requestContext(request.source)
    })

    const bindCurrent = Effect.fn("ProjectHttpApi.bindCurrent")(function* () {
      const binding = yield* executionContext()
      if (!binding) return
      const current = yield* InstanceState.context
      return yield* Effect.promise(() =>
        ExecutionResourceBinding.bindProject(binding, {
          projectID: current.project.id,
          worktree: current.project.worktree,
        }),
      )
    })

    const list = Effect.fn("ProjectHttpApi.list")(function* () {
      const binding = yield* executionContext()
      if (!binding) return yield* svc.list()
      yield* bindCurrent()
      const allowed = new Set(
        yield* Effect.promise(() => ExecutionResourceBinding.allowedResourceIDs(binding, "project")),
      )
      return (yield* svc.list()).filter((item) => allowed.has(item.id))
    })

    const current = Effect.fn("ProjectHttpApi.current")(function* () {
      yield* bindCurrent()
      return (yield* InstanceState.context).project
    })

    const initGit = Effect.fn("ProjectHttpApi.initGit")(function* () {
      yield* bindCurrent()
      const ctx = yield* InstanceState.context
      const next = yield* svc.initGit({ directory: ctx.directory, project: ctx.project })
      const binding = yield* executionContext()
      if (binding) {
        yield* Effect.promise(() =>
          ExecutionResourceBinding.bindProject(binding, {
            projectID: next.id,
            worktree: next.worktree,
          }),
        )
      }
      if (next.id === ctx.project.id && next.vcs === ctx.project.vcs && next.worktree === ctx.project.worktree)
        return next
      yield* markInstanceForReload(ctx, {
        directory: ctx.directory,
        worktree: ctx.directory,
        project: next,
      })
      return next
    })

    const update = Effect.fn("ProjectHttpApi.update")(function* (ctx: {
      params: { projectID: ProjectV2.ID }
      payload: Project.UpdatePayload
    }) {
      const binding = yield* executionContext()
      if (binding) {
        yield* Effect.promise(() =>
          ExecutionResourceBinding.assertResource(binding, "project", ctx.params.projectID),
        )
      }
      return yield* svc.update({ ...ctx.payload, projectID: ctx.params.projectID }).pipe(
        Effect.catchTag("Project.NotFoundError", (error) =>
          Effect.fail(
            new ProjectNotFoundError({
              projectID: error.projectID,
              message: `Project not found: ${error.projectID}`,
            }),
          ),
        ),
      )
    })

    const directories = Effect.fn("ProjectHttpApi.directories")(function* (ctx: {
      params: { projectID: ProjectV2.ID }
    }) {
      const binding = yield* executionContext()
      if (binding) {
        yield* Effect.promise(() =>
          ExecutionResourceBinding.assertResource(binding, "project", ctx.params.projectID),
        )
      }
      return yield* project.directories({ projectID: ctx.params.projectID })
    })

    return handlers
      .handle("list", list)
      .handle("current", current)
      .handle("initGit", initGit)
      .handle("update", update)
      .handle("directories", directories)
  }),
)
