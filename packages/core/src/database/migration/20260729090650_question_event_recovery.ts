import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260729090650_question_event_recovery",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`postgres_replication_outbox\` (
          \`sequence\` integer PRIMARY KEY AUTOINCREMENT,
          \`id\` text NOT NULL UNIQUE,
          \`operation\` text NOT NULL,
          \`event_id\` text,
          \`tenant_id\` text NOT NULL,
          \`actor_id\` text NOT NULL,
          \`aggregate_id\` text NOT NULL,
          \`seq\` integer,
          \`type\` text,
          \`data\` text,
          \`owner_id\` text,
          \`status\` text DEFAULT 'pending' NOT NULL,
          \`attempts\` integer DEFAULT 0 NOT NULL,
          \`next_attempt_at\` integer DEFAULT 0 NOT NULL,
          \`lease_owner\` text,
          \`lease_expires_at\` integer,
          \`last_error\` text,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          \`time_applied\` integer
        );
      `)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS \`event_type_aggregate_seq_idx\` ON \`event\` (\`type\`,\`aggregate_id\`,\`seq\`);`)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`postgres_replication_outbox_pending_idx\` ON \`postgres_replication_outbox\` (\`status\`,\`next_attempt_at\`,\`lease_expires_at\`,\`sequence\`);`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS \`postgres_replication_outbox_aggregate_idx\` ON \`postgres_replication_outbox\` (\`tenant_id\`,\`aggregate_id\`,\`sequence\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
