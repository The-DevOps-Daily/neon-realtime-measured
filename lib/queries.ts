// The three live query shapes the measurements compare, all written with Drizzle.
import { and, eq, sql } from 'drizzle-orm'
import type { Db } from './drizzle.ts'
import { customers, orders, storeMembers } from '../db/schema.ts'

// 1. Plain: one table, one filter.
export function plainQuery(db: Db, storeId: number) {
  return db
    .select({
      id: orders.id,
      customer_id: orders.customer_id,
      status: orders.status,
      total_cents: orders.total_cents,
      seq: orders.seq,
    })
    .from(orders)
    .where(eq(orders.store_id, storeId))
}

// 2. Join: orders with the customer's name, scoped by membership. The join through
// store_members is also the authorization: the user id comes from a verified token.
export function joinQuery(db: Db, userId: string, storeId: number) {
  return db
    .select({
      id: orders.id,
      status: orders.status,
      total_cents: orders.total_cents,
      seq: orders.seq,
      customer: sql<string>`${customers.name}`.as('customer'),
    })
    .from(orders)
    .innerJoin(customers, eq(customers.id, orders.customer_id))
    .innerJoin(storeMembers, eq(storeMembers.store_id, orders.store_id))
    .where(and(eq(storeMembers.user_id, userId), eq(orders.store_id, storeId)))
}

// 3. Aggregate: the board's header, orders and revenue per status.
export function aggregateQuery(db: Db, storeId: number) {
  return db
    .select({
      status: orders.status,
      orders: sql<number>`count(*)::int`.as('orders'),
      revenue_cents: sql<number>`sum(${orders.total_cents})::int`.as('revenue_cents'),
    })
    .from(orders)
    .where(eq(orders.store_id, storeId))
    .groupBy(orders.status)
}

export type Shape = 'plain' | 'join' | 'aggregate'
export const SHAPES: Shape[] = ['plain', 'join', 'aggregate']

export function queryFor(db: Db, shape: Shape, storeId: number, userId: string) {
  if (shape === 'plain') return plainQuery(db, storeId)
  if (shape === 'join') return joinQuery(db, userId, storeId)
  return aggregateQuery(db, storeId)
}

// The incremental way to scope by membership: join only store_members, pinned to one
// store by the WHERE clause. Used for the primary-impact test, where 100 users each
// hold their own live query and every write touches all of them.
export function memberQuery(db: Db, userId: string, storeId: number) {
  return db
    .select({ id: orders.id, status: orders.status, total_cents: orders.total_cents, seq: orders.seq })
    .from(orders)
    .innerJoin(storeMembers, eq(storeMembers.store_id, orders.store_id))
    .where(and(eq(storeMembers.user_id, userId), eq(orders.store_id, storeId)))
}
