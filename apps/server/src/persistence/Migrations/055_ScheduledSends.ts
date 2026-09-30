import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE scheduled_sends (
      id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      scheduled_at TEXT NOT NULL,
      phase TEXT NOT NULL,
      snapshot_json TEXT NOT NULL,
      command_json TEXT NOT NULL
    )
  `;
  yield* sql`CREATE INDEX scheduled_sends_due ON scheduled_sends(phase, scheduled_at)`;
});
