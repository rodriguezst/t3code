import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationCommand,
  type OrchestrationThreadShell,
  type ScheduledSendCreateInput,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";

import { ServerConfig } from "../config.ts";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { OrchestrationCommandInvariantError } from "./Errors.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ScheduledSendReactor from "./ScheduledSendReactor.ts";

const dependencies = Layer.mergeAll(SqlitePersistenceMemory, WorkspacePaths.layer).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-scheduled-send-test-" })),
  Layer.provideMerge(NodeServices.layer),
);

const thread: OrchestrationThreadShell = {
  id: ThreadId.make("scheduled-thread"),
  projectId: ProjectId.make("scheduled-project"),
  title: "Resume work",
  modelSelection: { instanceId: ProviderInstanceId.make("claude"), model: "sonnet" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  latestTurn: null,
  createdAt: "2026-09-30T00:00:00.000Z",
  updatedAt: "2026-09-30T00:00:00.000Z",
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
};

const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sends: Extract<OrchestrationCommand, { type: "thread.turn.start" }>[] = [];
  const outcomes: Extract<OrchestrationCommand, { type: "thread.activity.append" }>[] = [];
  const domainEvents = yield* PubSub.unbounded<OrchestrationEvent>();
  let reject = false;
  const services = Layer.mergeAll(
    Layer.mock(OrchestrationEngineService)({
      dispatch: (command) =>
        Effect.gen(function* () {
          if (command.type === "thread.turn.start") {
            const rows = yield* sql<{
              phase: string;
            }>`SELECT phase FROM scheduled_sends WHERE id = ${command.commandId.slice("server:scheduled-send:".length)}`.pipe(
              Effect.orDie,
            );
            assert.equal(rows[0]?.phase, "dispatching");
            sends.push(command);
            if (reject)
              return yield* new OrchestrationCommandInvariantError({
                commandType: command.type,
                detail: "Scheduled send skipped: thread busy.",
              });
          } else if (command.type === "thread.activity.append") outcomes.push(command);
          else return yield* Effect.die(`Unexpected command ${command.type}`);
          return { sequence: sends.length + outcomes.length };
        }),
      subscribeDomainEvents: PubSub.subscribe(domainEvents).pipe(
        Effect.map(Stream.fromSubscription),
      ),
    }),
    Layer.mock(ProjectionSnapshotQuery)({
      getThreadShellById: () => Effect.succeedSome(thread),
    }),
  );
  const restart = ScheduledSendReactor.make.pipe(Effect.provide(services));
  const reactor = yield* restart;
  const input = Effect.fnUntraced(function* (id = "schedule", delayMs = 3_600_000) {
    const now = yield* DateTime.now;
    return {
      commandId: CommandId.make(id),
      threadId: thread.id,
      message: {
        messageId: MessageId.make(`${id}:message`),
        role: "user" as const,
        text: "Continue the refactor",
        attachments: [],
      },
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" },
      runtimeMode: "approval-required" as const,
      interactionMode: "plan" as const,
      scheduledAt: DateTime.formatIso(DateTime.add(now, { milliseconds: delayMs })),
    } satisfies ScheduledSendCreateInput;
  });
  return {
    reactor,
    restart,
    sends,
    outcomes,
    input,
    domainEvents,
    reject: () => {
      reject = true;
    },
  };
});

it.effect("attempts exactly once at the deadline with the captured provider and modes", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    yield* f.reactor.create(input);
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 0);
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 1);
    assert.deepStrictEqual(f.sends[0]?.modelSelection, input.modelSelection);
    assert.equal(f.sends[0]?.runtimeMode, input.runtimeMode);
    assert.equal(f.sends[0]?.interactionMode, input.interactionMode);
    assert.equal(f.sends[0]?.message.text, input.message.text);
    assert.equal(f.sends[0]?.onlyIfIdle, true);
    assert.equal(f.sends[0]?.createdAt, input.scheduledAt);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("two subscribers see the same pending actions and cancellation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const browser = yield* Stream.toPull(f.reactor.stream);
    const phone = yield* Stream.toPull(f.reactor.stream);
    assert.deepStrictEqual(yield* browser, [[]]);
    assert.deepStrictEqual(yield* phone, [[]]);
    const input = yield* f.input();
    const scheduled = yield* f.reactor.create(input);
    assert.deepStrictEqual(yield* browser, [[scheduled]]);
    assert.deepStrictEqual(yield* phone, [[scheduled]]);
    assert.equal(yield* f.reactor.cancel(input.commandId), true);
    assert.deepStrictEqual(yield* browser, [[]]);
    assert.deepStrictEqual(yield* phone, [[]]);
    assert.equal(yield* f.reactor.cancel(input.commandId), false);
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 0);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("duplicate create acknowledgements cannot produce two scheduled sends", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    const first = yield* f.reactor.create(input);
    assert.deepStrictEqual(yield* f.reactor.create(input), first);
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 1);
    assert.equal((yield* f.reactor.create(input)).phase, "attempted");
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("expires pending work on restart even when its deadline is still in the future", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    yield* f.reactor.create(input);
    const restarted = yield* f.restart;
    const list = yield* Stream.runHead(restarted.stream);
    assert.equal(Option.getOrThrow(list)[0]?.phase, "expired");
    yield* TestClock.adjust("1 hour");
    yield* restarted.checkDue;
    assert.equal(f.sends.length, 0);
    assert.equal(f.outcomes[0]?.activity.kind, "scheduled-send.expired");
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("a server that sleeps through the deadline expires the action without catch-up", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.reactor.create(yield* f.input());
    yield* TestClock.adjust("2 hours");
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 0);
    const list = yield* Stream.runHead(f.reactor.stream);
    assert.equal(Option.getOrThrow(list)[0]?.phase, "expired");
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("a busy-thread rejection is retained and never retried", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    yield* f.reactor.create(input);
    f.reject();
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    yield* f.reactor.checkDue;
    assert.equal(f.sends.length, 1);
    const list = yield* Stream.runHead(f.reactor.stream);
    assert.equal(Option.getOrThrow(list)[0]?.phase, "skipped");
    assert.equal(f.outcomes[0]?.activity.kind, "scheduled-send.skipped");
    const sql = yield* SqlClient.SqlClient;
    const stored = yield* sql<{
      command_json: string;
    }>`SELECT command_json FROM scheduled_sends WHERE id = ${input.commandId}`;
    assert.ok(stored[0]?.command_json.includes(input.message.text));
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("an acknowledged dispatch remains attempted after restart and is never replayed", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.reactor.create(yield* f.input());
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    const restarted = yield* f.restart;
    yield* restarted.checkDue;
    assert.equal(f.sends.length, 1);
    const list = yield* Stream.runHead(restarted.stream);
    assert.equal(Option.getOrThrow(list)[0]?.phase, "attempted");
    assert.equal(Option.getOrThrow(list)[0]?.reason, null);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("an unacknowledged dispatch is retained as unconfirmed on restart without replay", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    yield* f.reactor.create(input);
    // The persisted crash boundary: consumed, before the dispatch acknowledgement.
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE scheduled_sends SET phase = 'dispatching', snapshot_json = json_set(snapshot_json, '$.phase', 'dispatching') WHERE id = ${input.commandId}`;
    const restarted = yield* f.restart;
    yield* TestClock.adjust("1 hour");
    yield* restarted.checkDue;
    assert.equal(f.sends.length, 0);
    const list = yield* Stream.runHead(restarted.stream);
    assert.equal(Option.getOrThrow(list)[0]?.phase, "failed");
    assert.ok(Option.getOrThrow(list)[0]?.reason?.includes("unconfirmed"));
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("claims attachments before client upload cleanup and sends the retained copy", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const config = yield* ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const attachmentId = "pending-00000000-0000-4000-8000-0000000000aa";
    const pendingPath = path.join(config.attachmentsDir, `${attachmentId}.png`);
    yield* fs.writeFileString(pendingPath, "pixels");
    const input = yield* f.input();
    yield* f.reactor.create({
      ...input,
      message: {
        ...input.message,
        attachments: [
          {
            type: "image",
            id: attachmentId,
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 6,
          },
        ],
      },
    });
    yield* fs.remove(pendingPath);
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    const delivered = f.sends[0]?.message.attachments[0];
    assert.ok(delivered);
    assert.notEqual(delivered.id, attachmentId);
    const retainedPath = resolveAttachmentPath({
      attachmentsDir: config.attachmentsDir,
      attachment: delivered,
    });
    assert.ok(retainedPath);
    assert.equal(yield* fs.readFileString(retainedPath), "pixels");
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

const domainEvent = (
  payload: Extract<OrchestrationEvent, { type: "thread.activity-appended" }>["payload"],
) => ({
  sequence: 1,
  type: "thread.activity-appended" as const,
  payload,
  aggregateKind: "thread" as const,
  aggregateId: thread.id,
  eventId: EventId.make("provider-failure"),
  occurredAt: thread.createdAt,
  commandId: null,
  causationEventId: null,
  correlationId: null,
  metadata: {},
});

it.effect("provider start failures retain the reason and prompt without retrying", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    yield* f.reactor.create(input);
    yield* TestClock.adjust("1 hour");
    yield* f.reactor.checkDue;
    yield* f.reactor.start();
    yield* PubSub.publish(
      f.domainEvents,
      domainEvent({
        threadId: thread.id,
        activity: {
          id: EventId.make("quota-failure"),
          kind: "provider.turn.start.failed",
          tone: "error",
          summary: "Provider turn start failed",
          payload: { requestId: input.message.messageId, detail: "Quota exhausted" },
          turnId: null,
          createdAt: input.scheduledAt,
        },
      }),
    );
    const list = yield* Stream.runHead(
      f.reactor.stream.pipe(Stream.filter((items) => items[0]?.phase === "failed")),
    );
    yield* f.reactor.drain;
    assert.equal(Option.getOrThrow(list)[0]?.reason, "Quota exhausted");
    assert.equal(f.sends.length, 1);
    const payload = f.outcomes[0]?.activity.payload;
    assert.ok(payload && typeof payload === "object" && "detail" in payload);
    assert.ok(typeof payload.detail === "string" && payload.detail.includes(input.message.text));
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("archiving a thread cancels its pending schedules for every subscriber", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.reactor.create(yield* f.input());
    yield* f.reactor.start();
    const event = domainEvent({
      threadId: thread.id,
      activity: {
        id: EventId.make("unused"),
        kind: "unused",
        tone: "info",
        summary: "unused",
        payload: {},
        turnId: null,
        createdAt: thread.createdAt,
      },
    });
    yield* PubSub.publish(f.domainEvents, {
      ...event,
      type: "thread.archived",
      payload: {
        threadId: thread.id,
        archivedAt: thread.createdAt,
        updatedAt: thread.createdAt,
      },
    });
    yield* Stream.runHead(f.reactor.stream.pipe(Stream.filter((items) => items.length === 0)));
    yield* f.reactor.drain;
    const restarted = yield* f.restart;
    assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(restarted.stream)), []);
    assert.equal(f.sends.length, 0);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

it.effect("rejects empty messages and invalid or past deadlines", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = yield* f.input();
    for (const invalid of [
      { ...input, scheduledAt: "invalid" },
      { ...input, scheduledAt: "1970-01-01T00:00:00.000Z" },
      { ...input, message: { ...input.message, text: " " } },
    ]) {
      assert.equal((yield* Effect.result(f.reactor.create(invalid)))._tag, "Failure");
    }
    assert.equal(f.sends.length, 0);
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);
