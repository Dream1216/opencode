import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260728000100_postgres_replication_outbox",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`postgres_replication_outbox\` (
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
          \`status\` text NOT NULL DEFAULT 'pending',
          \`attempts\` integer NOT NULL DEFAULT 0,
          \`next_attempt_at\` integer NOT NULL DEFAULT 0,
          \`lease_owner\` text,
          \`lease_expires_at\` integer,
          \`last_error\` text,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          \`time_applied\` integer
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`postgres_replication_outbox_pending_idx\` ON \`postgres_replication_outbox\` (\`status\`,\`next_attempt_at\`,\`lease_expires_at\`,\`sequence\`);`,
      )
      yield* tx.run(
        `CREATE INDEX \`postgres_replication_outbox_aggregate_idx\` ON \`postgres_replication_outbox\` (\`tenant_id\`,\`aggregate_id\`,\`sequence\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
