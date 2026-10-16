// Drizzle schema for db/001_schema.sql. Keys are snake_case on purpose: the
// Realtime Drizzle adapter needs each selected key to match its column name.
import { integer, pgTable, primaryKey, text, timestamp } from 'drizzle-orm/pg-core'

export const stores = pgTable('stores', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
})

export const storeMembers = pgTable(
  'store_members',
  {
    store_id: integer('store_id').notNull(),
    user_id: text('user_id').notNull(),
  },
  (t) => [primaryKey({ columns: [t.store_id, t.user_id] })],
)

export const customers = pgTable('customers', {
  id: integer('id').primaryKey(),
  store_id: integer('store_id').notNull(),
  name: text('name').notNull(),
})

export const orders = pgTable('orders', {
  id: integer('id').primaryKey().generatedAlwaysAsIdentity(),
  store_id: integer('store_id').notNull(),
  customer_id: integer('customer_id').notNull(),
  status: text('status', { enum: ['new', 'preparing', 'ready', 'done'] }).notNull().default('new'),
  total_cents: integer('total_cents').notNull(),
  seq: integer('seq'),
  created_at: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
})

export type Order = typeof orders.$inferSelect
export const STATUSES = ['new', 'preparing', 'ready', 'done'] as const
