import { Question } from "@opencode-ai/schema/question"
import { Cause, DateTime, Effect, Exit, Layer, Scope } from "effect"
import { SessionDurable } from "@opencode-ai/schema/durable-event-manifest"
import { Database } from "../../database/database"
import { EventV2 } from "../../event"
import { LocationServiceMap } from "../../location-service-map"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionRunCoordinator } from "../run-coordinator"
import { SessionRunner } from "../runner"
import { SessionSchema } from "../schema"
import { SessionEvent } from "../event"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { SessionWorkerCoordination, type Handle } from "../worker-coordination"
import { WorkerFenceRejectedError } from "../../database/postgres/worker-lease"
import * as SessionWorkerQueue from "../worker-queue"

/** Current-process routing for implicit-local Locations. Future remote placement belongs here. */
let nextLease = 0

const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    const database = yield* Database.Service
    const events = yield* EventV2.Service
    const coordination = yield* SessionWorkerCoordination.Service
    const queue = yield* SessionWorkerQueue.Service
    const scope = yield* Scope.Scope
    const publishWorker = {
      scheduled: Effect.fn("SessionExecutionWorker.scheduled")(function* (
        sessionID: SessionSchema.ID,
        reason: "wake" | "resume",
      ) {
        yield* events.publish(SessionEvent.Worker.Scheduled, {
          sessionID,
          timestamp: yield* DateTime.now,
          reason,
        })
      }),
      resumed: Effect.fn("SessionExecutionWorker.resumed")(function* (sessionID: SessionSchema.ID) {
        yield* events.publish(SessionEvent.Worker.Resumed, {
          sessionID,
          timestamp: yield* DateTime.now,
        })
      }),
      started: Effect.fn("SessionExecutionWorker.started")(function* (sessionID: SessionSchema.ID, force: boolean) {
        yield* events.publish(SessionEvent.Worker.Started, {
          sessionID,
          timestamp: yield* DateTime.now,
          force,
        })
      }),
      stopRequested: Effect.fn("SessionExecutionWorker.stopRequested")(function* (sessionID: SessionSchema.ID) {
        yield* events.publish(SessionEvent.Worker.StopRequested, {
          sessionID,
          timestamp: yield* DateTime.now,
        })
      }),
      stopped: Effect.fn("SessionExecutionWorker.stopped")(function* (sessionID: SessionSchema.ID, reason: string) {
        yield* events.publish(SessionEvent.Worker.Stopped, {
          sessionID,
          timestamp: yield* DateTime.now,
          reason,
        })
      }),
      completed: Effect.fn("SessionExecutionWorker.completed")(function* (sessionID: SessionSchema.ID) {
        yield* events.publish(SessionEvent.Worker.Completed, {
          sessionID,
          timestamp: yield* DateTime.now,
        })
      }),
      failed: Effect.fn("SessionExecutionWorker.failed")(function* (sessionID: SessionSchema.ID, message: string) {
        yield* events.publish(SessionEvent.Worker.Failed, {
          sessionID,
          timestamp: yield* DateTime.now,
          error: { type: "unknown", message },
        })
      }),
      lease: Effect.fn("SessionExecutionWorker.lease")(function* (
        sessionID: SessionSchema.ID,
        lease: WorkerLease,
        phase: "acquired" | "heartbeat" | "released" | "contended",
        reason?: string,
      ) {
        yield* events.publish(SessionEvent.Worker.Lease, {
          sessionID,
          timestamp: yield* DateTime.now,
          phase,
          lease,
          ...(reason === undefined ? {} : { reason }),
        })
      }),
    }
    const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, SessionRunner.RunError>({
      drain: Effect.fnUntraced(function* (sessionID: SessionSchema.ID, force) {
        const session = yield* store.get(sessionID)
        if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
        const acquisition = yield* coordination.acquire(sessionID)
        if (acquisition.status === "contended") {
          yield* publishWorker.lease(sessionID, sharedLease(acquisition.lease), "contended", "held-by-other-worker")
          yield* publishWorker.stopped(sessionID, "lease-contended")
          if (queue.enabled) {
            return yield* Effect.die(
              new Error(`PostgreSQL worker lease for ${sessionID} is held by another worker`),
            )
          }
          return
        }
        let handle: Handle | undefined = acquisition.status === "acquired" ? acquisition.handle : undefined
        let lease = handle === undefined ? makeLease() : sharedLease(handle.lease)
        yield* publishWorker.lease(sessionID, lease, "acquired")
        yield* publishWorker.started(sessionID, force)
        const runner = SessionRunner.Service.use((service) =>
          service.run({ sessionID, force, ...(handle === undefined ? {} : { workerFence: handle.fence }) }),
        ).pipe(
          Effect.provide(locations.get(session.location)),
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.logError("Failed to drain Session", cause).pipe(Effect.annotateLogs({ sessionID })),
          ),
        )
        const heartbeat =
          handle === undefined
            ? localHeartbeatLoop(sessionID, lease, publishWorker.lease)
            : sharedHeartbeatLoop(sessionID, handle, coordination, publishWorker.lease, (next) => {
                handle = next
                lease = sharedLease(next.lease)
              })
        const exit = yield* Effect.raceFirst(runner, heartbeat).pipe(Effect.exit)
        if (handle !== undefined && Exit.isSuccess(exit)) {
          const completed = yield* coordination.complete(handle).pipe(Effect.exit)
          if (Exit.isFailure(completed)) {
            yield* publishWorker.failed(sessionID, failureMessage(completed.cause))
            yield* publishWorker.lease(sessionID, lease, "released", "lease-completion-failed")
            return yield* Effect.failCause(completed.cause)
          }
        }
        if (handle !== undefined && Exit.isFailure(exit) && !leaseLost(exit.cause)) {
          yield* coordination.release(handle).pipe(Effect.catchCause(() => Effect.void))
        }
        if (Exit.isSuccess(exit)) yield* publishWorker.completed(sessionID)
        else if (leaseLost(exit.cause)) yield* publishWorker.stopped(sessionID, "lease-lost")
        else if (Cause.hasInterruptsOnly(exit.cause)) yield* publishWorker.stopped(sessionID, "interrupted")
        else yield* publishWorker.failed(sessionID, failureMessage(exit.cause))
        yield* publishWorker.lease(
          sessionID,
          lease,
          "released",
          Exit.isSuccess(exit) ? "completed" : leaseLost(exit.cause) ? "lease-lost" : "stopped",
        )
        if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause)
      }),
    })

    const resumeRecoveredQuestion = Effect.fnUntraced(function* (sessionID: SessionSchema.ID) {
      yield* publishWorker.resumed(sessionID)
      yield* publishWorker.scheduled(sessionID, "resume")
      if (queue.enabled) {
        yield* queue.enqueue(sessionID, "resume")
        return
      }
      yield* coordinator.run(sessionID)
    })
    const unsubscribeRecovery = yield* events.listen((event) => {
      if (event.type !== Question.Event.RecoveryRequested.type) return Effect.void
      const sessionID = (event.data as typeof Question.Event.RecoveryRequested.data.Type).sessionID
      return resumeRecoveredQuestion(sessionID).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Failed to resume Session after recovered Question reply", cause),
        ),
        Effect.forkIn(scope, { startImmediately: true }),
        Effect.asVoid,
      )
    })
    yield* Effect.addFinalizer(() => unsubscribeRecovery)

    const replay = Effect.fn("SessionExecutionLocal.replay")(function* (sessionID: SessionSchema.ID) {
      const output: SessionEvent.DurableEvent[] = []
      let after: number | undefined
      while (true) {
        const page = yield* EventV2.readAggregate(database.db, {
          aggregateID: sessionID,
          after,
          limit: 500,
          manifest: SessionDurable,
        })
        output.push(...page.events.filter(isWorkerEvent))
        if (!page.hasMore) return output
        const next = page.events.at(-1)?.durable?.seq
        if (next === undefined) return output
        after = next
      }
    })

    const processClaim = Effect.fnUntraced(function* (
      claim: import("../../database/postgres/worker-job").WorkerJobClaim,
    ) {
      const sessionID = SessionSchema.ID.make(claim.runID)
      const run =
        claim.reason === "resume"
          ? coordinator.run(sessionID)
          : Effect.gen(function* () {
              yield* coordinator.wake(sessionID)
              while ((yield* coordinator.active).has(sessionID)) yield* Effect.sleep(25)
            })
      const heartbeat = Effect.forever(
        Effect.sleep(queue.heartbeatIntervalMs).pipe(Effect.andThen(queue.heartbeat(claim))),
      )
      const exit = yield* Effect.raceFirst(run, heartbeat).pipe(Effect.exit)
      if (Exit.isSuccess(exit)) {
        yield* queue.complete(claim)
        return
      }
      yield* coordinator.interrupt(sessionID)
      yield* queue.fail(claim, failureMessage(exit.cause))
    })

    if (queue.enabled) {
      yield* Effect.forkScoped(
        Effect.forever(
          Effect.gen(function* () {
            const claim = yield* queue.claim()
            if (claim === undefined) return
            yield* processClaim(claim)
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logError(`PostgreSQL worker queue consumer: ${failureMessage(cause)}`),
            ),
            Effect.andThen(Effect.sleep(queue.pollIntervalMs)),
          ),
        ),
      )
    }

    return SessionExecution.Service.of({
      active: coordinator.active,
      interrupt: Effect.fn("SessionExecutionLocal.interrupt")(function* (sessionID) {
        yield* publishWorker.stopRequested(sessionID)
        if (queue.enabled) yield* queue.cancel(sessionID)
        yield* coordinator.interrupt(sessionID)
      }),
      resume: Effect.fn("SessionExecutionLocal.resume")(function* (sessionID) {
        yield* publishWorker.resumed(sessionID)
        yield* publishWorker.scheduled(sessionID, "resume")
        if (queue.enabled) {
          const generation = yield* queue.enqueue(sessionID, "resume")
          yield* queue.awaitCompletion(sessionID, generation)
          return
        }
        yield* coordinator.run(sessionID)
      }),
      wake: Effect.fn("SessionExecutionLocal.wake")(function* (sessionID) {
        yield* publishWorker.scheduled(sessionID, "wake")
        if (queue.enabled) {
          yield* queue.enqueue(sessionID, "wake")
          return
        }
        yield* coordinator.wake(sessionID)
      }),
      replay,
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [
    SessionStore.node,
    LocationServiceMap.node,
    Database.node,
    EventV2.node,
    SessionWorkerCoordination.node,
    SessionWorkerQueue.node,
  ],
})

export * as SessionExecutionLocal from "./local"

function isWorkerEvent(event: SessionEvent.DurableEvent) {
  return event.type.startsWith("session.next.worker.")
}

type WorkerLease = typeof SessionEvent.Worker.LeaseInfo.Type

function makeLease(): WorkerLease {
  nextLease += 1
  return {
    leaseID: `wlease_${Date.now().toString(36)}_${nextLease.toString(36)}`,
    ownerID: process.env.OPENCODE_WORKER_ID ?? `pid_${process.pid}`,
    ttlMs: Math.max(1000, Number(process.env.OPENCODE_WORKER_LEASE_TTL_MS ?? 15000)),
  }
}

const localHeartbeatLoop = (
  sessionID: SessionSchema.ID,
  lease: WorkerLease,
  heartbeat: (
    sessionID: SessionSchema.ID,
    lease: WorkerLease,
    phase: "acquired" | "heartbeat" | "released",
    reason?: string,
  ) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    while (true) {
      yield* heartbeat(sessionID, lease, "heartbeat")
      yield* Effect.sleep(Math.max(1000, Math.floor(lease.ttlMs / 3)))
    }
  })

const sharedHeartbeatLoop = (
  sessionID: SessionSchema.ID,
  initial: Handle,
  coordination: SessionWorkerCoordination.Interface,
  publish: (
    sessionID: SessionSchema.ID,
    lease: WorkerLease,
    phase: "acquired" | "heartbeat" | "released",
    reason?: string,
  ) => Effect.Effect<void>,
  update: (handle: Handle) => void,
) =>
  Effect.gen(function* () {
    let handle = initial
    while (true) {
      yield* Effect.sleep(Math.max(1000, Math.floor(handle.ttlMs / 3)))
      handle = yield* coordination.heartbeat(handle)
      update(handle)
      yield* publish(sessionID, sharedLease(handle.lease), "heartbeat")
    }
  })

function sharedLease(lease: {
  readonly runID: string
  readonly ownerID: string
  readonly fencingToken: number
  readonly leaseExpiresAt: number
}): WorkerLease {
  return {
    leaseID: `pg:${lease.runID}:${lease.fencingToken}`,
    ownerID: lease.ownerID,
    ttlMs: Math.max(1000, Number(process.env.OPENCODE_WORKER_LEASE_TTL_MS ?? 15000)),
    backend: "postgres",
    fencingToken: lease.fencingToken,
    expiresAt: lease.leaseExpiresAt,
  }
}

function leaseLost(cause: Cause.Cause<unknown>) {
  return Cause.squash(cause) instanceof WorkerFenceRejectedError
}

function failureMessage(cause: Cause.Cause<unknown>) {
  const failure = Cause.squash(cause)
  return failure instanceof Error ? failure.message : String(failure)
}
