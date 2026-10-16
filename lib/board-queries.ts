// The board's live queries. Each one joins through store_members on the user id
// from the verified token, so the authorization is part of the query and stays live.
//
// The board needs customer names next to orders. One three-table join would be
// re-sent in full on every write (see results/explore-incremental-*.txt), so the
// board subscribes to two incremental queries and joins them in the browser.
import { and, eq, ne, sql } from 'drizzle-orm'
import type { Db } from './drizzle.ts'
import { customers, orders, storeMembers } from '../db/schema.ts'

export function openOrdersQuery(db: Db, userId: string, storeId: number) {
  return db
    .select({
      id: orders.id,
      customer_id: orders.customer_id,
      status: orders.status,
      total_cents: orders.total_cents,
      created_at: orders.created_at,
    })
    .from(orders)
    .innerJoin(storeMembers, eq(storeMembers.store_id, orders.store_id))
    .where(and(eq(storeMembers.user_id, userId), eq(orders.store_id, storeId), ne(orders.status, 'done')))
}

export function storeCustomersQuery(db: Db, userId: string, storeId: number) {
  return db
    .select({ id: customers.id, name: customers.name })
    .from(customers)
    .innerJoin(storeMembers, eq(storeMembers.store_id, customers.store_id))
    .where(and(eq(storeMembers.user_id, userId), eq(customers.store_id, storeId)))
}

// The header. Aggregates are re-sent whole on each write, which is fine at four rows.
export function statusCountsQuery(db: Db, userId: string, storeId: number) {
  return db
    .select({
      status: orders.status,
      orders: sql<number>`count(*)::int`.as('orders'),
      revenue_cents: sql<number>`sum(${orders.total_cents})::int`.as('revenue_cents'),
    })
    .from(orders)
    .innerJoin(storeMembers, eq(storeMembers.store_id, orders.store_id))
    .where(and(eq(storeMembers.user_id, userId), eq(orders.store_id, storeId)))
    .groupBy(orders.status)
}

export type OpenOrder = Awaited<ReturnType<typeof openOrdersQuery>>[number]
export type StoreCustomer = Awaited<ReturnType<typeof storeCustomersQuery>>[number]
export type StatusCount = Awaited<ReturnType<typeof statusCountsQuery>>[number]
