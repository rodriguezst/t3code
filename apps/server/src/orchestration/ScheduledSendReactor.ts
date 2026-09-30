import {
  CommandId,
  EventId,
  ScheduledSendError,
  ScheduledSendSnapshot,
  ThreadTurnStartCommand,
  type ScheduledSendCreateInput,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../config.ts";
import { forkParked } from "../serverActivation.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { cleanupFailedUploadedAttachments, normalizeDispatchCommand } from "./Normalizer.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";

// A late wake must not turn yesterday's intent into today's unexpected work.
const DEADLINE_GRACE_MS = 60_000;
const MAX_PENDING_SENDS = 100;
const RECENT_RESULTS = 20;

export class ScheduledSendReactor extends Context.Service<
  ScheduledSendReactor,
  {
    readonly create: (
      input: ScheduledSendCreateInput,
    ) => Effect.Effect<ScheduledSendSnapshot, ScheduledSendError>;
    readonly cancel: (id: CommandId) => Effect.Effect<boolean, ScheduledSendError>;
    readonly stream: Stream.Stream<ReadonlyArray<ScheduledSendSnapshot>>;
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly drain: Effect.Effect<void>;
    /** @internal Deterministic deadline check for tests. */
    readonly checkDue: Effect.Effect<void>;
  }
>()("t3/orchestration/ScheduledSendReactor") {}

const snapshotJson = Schema.fromJsonString(ScheduledSendSnapshot);
const commandJson = Schema.fromJsonString(ThreadTurnStartCommand);
const encodeSnapshot = Schema.encodeSync(snapshotJson);
const decodeSnapshot = Schema.decodeUnknownSync(snapshotJson);
const encodeCommand = Schema.encodeSync(commandJson);
const decodeCommand = Schema.decodeUnknownSync(commandJson);

const asScheduleError = (error: unknown) =>
  new ScheduledSendError({
    message: error instanceof Error ? error.message : "Could not update the scheduled send.",
  });

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const lock = yield* Semaphore.make(1);
  const entries = new Map<
    CommandId,
    { snapshot: ScheduledSendSnapshot; command: ThreadTurnStartCommand }
  >();
  const state = yield* SubscriptionRef.make<ReadonlyArray<ScheduledSendSnapshot>>([]);

  const publish = Effect.suspend(() => {
    const all = Array.from(entries.values(), (entry) => entry.snapshot);
    const pending = all
      .filter((item) => item.phase === "pending")
      .sort((a, b) => a.scheduledAt.localeCompare(b.scheduledAt));
    const recent = all
      .filter((item) => item.phase !== "pending" && item.phase !== "cancelled")
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .slice(0, RECENT_RESULTS);
    const retained = new Set([...pending, ...recent].map((item) => item.id));
    for (const id of entries.keys()) if (!retained.has(id)) entries.delete(id);
    return SubscriptionRef.set(state, [...pending, ...recent]);
  });

  const save = Effect.fn("ScheduledSendReactor.save")(function* (
    snapshot: ScheduledSendSnapshot,
    command: ThreadTurnStartCommand,
  ) {
    const previousPhase = entries.get(snapshot.id)?.snapshot.phase;
    yield* sql`
      INSERT INTO scheduled_sends (id, thread_id, scheduled_at, phase, snapshot_json, command_json)
      VALUES (${snapshot.id}, ${snapshot.threadId}, ${snapshot.scheduledAt}, ${snapshot.phase},
        ${encodeSnapshot(snapshot)}, ${encodeCommand(command)})
      ON CONFLICT(id) DO UPDATE SET phase = excluded.phase, snapshot_json = excluded.snapshot_json,
        command_json = excluded.command_json
    `;
    entries.set(snapshot.id, { snapshot, command });
    yield* publish;
    if (
      snapshot.phase !== previousPhase &&
      (snapshot.phase === "expired" || snapshot.phase === "skipped" || snapshot.phase === "failed")
    ) {
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* engine
        .dispatch({
          type: "thread.activity.append",
          commandId: CommandId.make(
            `server:scheduled-send:outcome:${snapshot.id}:${snapshot.phase}`,
          ),
          threadId: snapshot.threadId,
          createdAt,
          activity: {
            id: EventId.make(`scheduled-send:outcome:${snapshot.id}:${snapshot.phase}`),
            tone: "error",
            kind: `scheduled-send.${snapshot.phase}`,
            summary: `Scheduled send ${snapshot.phase}`,
            payload: {
              scheduledAt: snapshot.scheduledAt,
              message: command.message,
              detail: `${snapshot.reason ?? "Message was not sent."}\n\n${command.message.text}`,
            },
            turnId: null,
            createdAt,
          },
        })
        .pipe(Effect.ignoreCause({ log: true }));
    }
  });

  // Runs before the server accepts RPCs. Shutdown hooks are insufficient for crashes or kills.
  const leftovers = yield* sql<{ snapshot_json: string; command_json: string }>`
    SELECT snapshot_json, command_json FROM scheduled_sends
    WHERE phase IN ('pending', 'dispatching')
  `;
  for (const row of leftovers) {
    const snapshot = decodeSnapshot(row.snapshot_json);
    const command = decodeCommand(row.command_json);
    yield* save(
      {
        ...snapshot,
        phase: snapshot.phase === "pending" ? "expired" : "failed",
        reason:
          snapshot.phase === "pending"
            ? "Server restarted. This scheduled send expired."
            : "Server restarted after dispatch. Delivery is unconfirmed; review the thread before sending again.",
      },
      command,
    );
  }
  const recent = yield* sql<{ snapshot_json: string; command_json: string }>`
    SELECT snapshot_json, command_json FROM scheduled_sends
    WHERE phase NOT IN ('pending', 'cancelled') ORDER BY scheduled_at DESC LIMIT ${RECENT_RESULTS}
  `;
  for (const row of recent) {
    const snapshot = decodeSnapshot(row.snapshot_json);
    entries.set(snapshot.id, { snapshot, command: decodeCommand(row.command_json) });
  }
  yield* publish;

  const create = Effect.fn("ScheduledSendReactor.create")(function* (
    input: ScheduledSendCreateInput,
  ) {
    // The client supplies a stable ID, so a lost acknowledgement cannot schedule twice.
    const existing = yield* sql<{ snapshot_json: string }>`
      SELECT snapshot_json FROM scheduled_sends WHERE id = ${input.commandId}
    `;
    if (existing[0]) return decodeSnapshot(existing[0].snapshot_json);
    const now = yield* DateTime.now;
    const scheduledMs = Date.parse(input.scheduledAt);
    if (!Number.isFinite(scheduledMs) || scheduledMs <= DateTime.toEpochMillis(now)) {
      return yield* new ScheduledSendError({ message: "Choose a time in the future." });
    }
    if (
      Array.from(entries.values()).filter((entry) => entry.snapshot.phase === "pending").length >=
      MAX_PENDING_SENDS
    ) {
      return yield* new ScheduledSendError({
        message: "Cancel a pending scheduled send before adding another.",
      });
    }
    if (input.message.text.trim().length === 0 && input.message.attachments.length === 0) {
      return yield* new ScheduledSendError({ message: "Write a message to schedule." });
    }
    const thread = yield* snapshots.getThreadShellById(input.threadId);
    if (Option.isNone(thread) || thread.value.archivedAt !== null) {
      return yield* new ScheduledSendError({
        message: "Schedule a send in an existing, unarchived thread.",
      });
    }
    const original = {
      type: "thread.turn.start" as const,
      commandId: CommandId.make(`server:scheduled-send:${input.commandId}`),
      threadId: input.threadId,
      message: input.message,
      modelSelection: input.modelSelection,
      runtimeMode: input.runtimeMode,
      interactionMode: input.interactionMode,
      createdAt: DateTime.formatIso(now),
    };
    // Claim uploads now: browser blobs and pending-upload expiry cannot back a future send.
    const normalized = yield* normalizeDispatchCommand(original).pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.provideService(ServerConfig, config),
      Effect.provideService(WorkspacePaths.WorkspacePaths, workspacePaths),
    );
    if (normalized.type !== "thread.turn.start") return yield* Effect.die("Expected a turn start.");
    const snapshot: ScheduledSendSnapshot = {
      id: input.commandId,
      threadId: input.threadId,
      threadTitle: thread.value.title,
      preview: input.message.text.slice(0, 240) || input.message.attachments[0]?.name || "Message",
      modelSelection: input.modelSelection,
      scheduledAt: DateTime.formatIso(DateTime.makeUnsafe(scheduledMs)),
      createdAt: original.createdAt,
      phase: "pending",
      reason: null,
    };
    yield* save(snapshot, normalized).pipe(
      Effect.tapError(() =>
        cleanupFailedUploadedAttachments(original, normalized).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(ServerConfig, config),
        ),
      ),
    );
    return snapshot;
  });

  const cancel = Effect.fn("ScheduledSendReactor.cancel")(function* (id: CommandId) {
    const entry = entries.get(id);
    if (!entry || entry.snapshot.phase !== "pending") return false;
    yield* save({ ...entry.snapshot, phase: "cancelled" }, entry.command);
    return true;
  });

  const attempt = Effect.fn("ScheduledSendReactor.attempt")(function* (id: CommandId) {
    const entry = entries.get(id);
    if (!entry || entry.snapshot.phase !== "pending") return;
    const now = yield* DateTime.now;
    const lateBy = DateTime.toEpochMillis(now) - Date.parse(entry.snapshot.scheduledAt);
    if (lateBy < 0) return;
    if (lateBy > DEADLINE_GRACE_MS) {
      yield* save(
        {
          ...entry.snapshot,
          phase: "expired",
          reason: "Deadline missed while the server was unavailable.",
        },
        entry.command,
      );
      return;
    }
    // Consume before dispatch. A crash between these writes may lose an attempt, but can never replay it.
    const command = {
      ...entry.command,
      createdAt: DateTime.formatIso(now),
      onlyIfIdle: true,
    };
    yield* save({ ...entry.snapshot, phase: "dispatching" }, command);
    const result = yield* Effect.result(engine.dispatch(command));
    if (result._tag === "Failure") {
      const error = result.failure;
      const reason = error.message;
      yield* save(
        {
          ...entry.snapshot,
          phase: reason.includes("Scheduled send skipped:") ? "skipped" : "failed",
          reason,
        },
        command,
      );
    } else {
      yield* save({ ...entry.snapshot, phase: "attempted" }, command);
    }
  });

  const processEvent = Effect.fn("ScheduledSendReactor.processEvent")(function* (
    event: OrchestrationEvent,
  ) {
    if (event.type === "thread.deleted" || event.type === "thread.archived") {
      for (const entry of entries.values()) {
        if (
          entry.snapshot.threadId === event.payload.threadId &&
          entry.snapshot.phase === "pending"
        ) {
          yield* save({ ...entry.snapshot, phase: "cancelled" }, entry.command);
        }
      }
    }
    if (
      event.type === "thread.activity-appended" &&
      event.payload.activity.kind === "provider.turn.start.failed"
    ) {
      const payload = event.payload.activity.payload;
      if (!payload || typeof payload !== "object" || !("requestId" in payload)) return;
      for (const entry of entries.values()) {
        if (
          entry.snapshot.phase === "attempted" &&
          entry.command.message.messageId === payload.requestId
        ) {
          yield* save(
            {
              ...entry.snapshot,
              phase: "failed",
              reason:
                "detail" in payload && typeof payload.detail === "string"
                  ? payload.detail
                  : event.payload.activity.summary,
            },
            entry.command,
          );
        }
      }
    }
  });

  const worker = yield* makeDrainableWorker((event: OrchestrationEvent | null) =>
    (event === null
      ? Effect.forEach(Array.from(entries.keys()), (id) => lock.withPermits(1)(attempt(id)), {
          discard: true,
        })
      : lock.withPermits(1)(processEvent(event))
    ).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.logError("Scheduled send check failed", cause),
      ),
    ),
  );
  const checkDue = worker.enqueue(null).pipe(Effect.andThen(worker.drain));
  let started = false;
  const start = Effect.fn("ScheduledSendReactor.start")(function* () {
    if (started) return;
    started = true;
    const events = yield* engine.subscribeDomainEvents;
    yield* forkParked(
      Stream.runForEach(events, (event) =>
        event.type === "thread.deleted" ||
        event.type === "thread.archived" ||
        (event.type === "thread.activity-appended" &&
          event.payload.activity.kind === "provider.turn.start.failed")
          ? worker.enqueue(event)
          : Effect.void,
      ),
    );
    yield* forkParked(checkDue.pipe(Effect.repeat(Schedule.spaced("10 seconds")), Effect.asVoid));
  });

  return ScheduledSendReactor.of({
    create: (input) => lock.withPermits(1)(create(input)).pipe(Effect.mapError(asScheduleError)),
    cancel: (id) => lock.withPermits(1)(cancel(id)).pipe(Effect.mapError(asScheduleError)),
    stream: SubscriptionRef.changes(state),
    start,
    checkDue,
    drain: worker.drain,
  });
});

export const layer = Layer.effect(ScheduledSendReactor, make);
