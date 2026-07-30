import { sqliteTable, text, integer, index, uniqueIndex } from "drizzle-orm/sqlite-core"
import type { EventV2 } from "../event"

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  owner_id: text(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<EventV2.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
    index("event_type_aggregate_seq_idx").on(table.type, table.aggregate_id, table.seq),
  ],
)

export type PostgresReplicationOutboxStatus = "pending" | "applied" | "dead_letter"
export type PostgresReplicationOutboxOperation = "append" | "claim" | "remove"

export const PostgresReplicationOutboxTable = sqliteTable(
  "postgres_replication_outbox",
  {
    sequence: integer().primaryKey({ autoIncrement: true }),
    id: text().notNull().unique(),
    operation: text().$type<PostgresReplicationOutboxOperation>().notNull(),
    event_id: text(),
    tenant_id: text().notNull(),
    actor_id: text().notNull(),
    aggregate_id: text().notNull(),
    seq: integer(),
    type: text(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>(),
    owner_id: text(),
    status: text().$type<PostgresReplicationOutboxStatus>().notNull().default("pending"),
    attempts: integer().notNull().default(0),
    next_attempt_at: integer().notNull().default(0),
    lease_owner: text(),
    lease_expires_at: integer(),
    last_error: text(),
    time_created: integer().notNull(),
    time_updated: integer().notNull(),
    time_applied: integer(),
  },
  (table) => [
    index("postgres_replication_outbox_pending_idx").on(
      table.status,
      table.next_attempt_at,
      table.lease_expires_at,
      table.sequence,
    ),
    index("postgres_replication_outbox_aggregate_idx").on(table.tenant_id, table.aggregate_id, table.sequence),
  ],
)
