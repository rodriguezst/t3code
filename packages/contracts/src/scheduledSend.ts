import * as Schema from "effect/Schema";

import { CommandId, IsoDateTime, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ClientThreadTurnStartCommand, ModelSelection } from "./orchestration.ts";

/** A single attempt during the owning server's current session. */
export const ScheduledSendCreateInput = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  message: ClientThreadTurnStartCommand.fields.message,
  modelSelection: ModelSelection,
  runtimeMode: ClientThreadTurnStartCommand.fields.runtimeMode,
  interactionMode: ClientThreadTurnStartCommand.fields.interactionMode,
  scheduledAt: IsoDateTime,
});
export type ScheduledSendCreateInput = typeof ScheduledSendCreateInput.Type;

export const ScheduledSendPhase = Schema.Literals([
  "pending",
  "dispatching",
  "attempted",
  "failed",
  "skipped",
  "expired",
  "cancelled",
]);
export type ScheduledSendPhase = typeof ScheduledSendPhase.Type;

/** Only summaries are streamed; attachment bytes and full prompts stay on the host. */
export const ScheduledSendSnapshot = Schema.Struct({
  id: CommandId,
  threadId: ThreadId,
  threadTitle: TrimmedNonEmptyString,
  preview: Schema.String,
  modelSelection: ModelSelection,
  scheduledAt: IsoDateTime,
  createdAt: IsoDateTime,
  phase: ScheduledSendPhase,
  reason: Schema.NullOr(Schema.String),
});
export type ScheduledSendSnapshot = typeof ScheduledSendSnapshot.Type;

export const ScheduledSendList = Schema.Array(ScheduledSendSnapshot);
export const ScheduledSendCancelInput = Schema.Struct({ id: CommandId });
export const ScheduledSendCancelResult = Schema.Struct({ cancelled: Schema.Boolean });

export class ScheduledSendError extends Schema.TaggedError<ScheduledSendError>()(
  "ScheduledSendError",
  { message: Schema.String },
) {}
