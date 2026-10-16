// Smoke test writes for `neon realtime subscribe`: seed one store, then three
// separate transactions (insert, update, insert+update) a few seconds apart.
import { directClient } from '../lib/db.ts'

const c = directClient()
await c.connect()
const step = async (label: string, sql: string) => {
  const { rows } = await c.query(sql)
  console.log(`[${new Date().toISOString()}] ${label}`, rows[0] ?? '')
  await new Promise((r) => setTimeout(r, 3000))
}
await c.query(`insert into stores (id, name) values (1, 'Main St') on conflict do nothing`)
await c.query(`insert into customers (id, store_id, name) values (1, 1, 'Ada'), (2, 1, 'Grace') on conflict do nothing`)
await step('insert order', `insert into orders (store_id, customer_id, total_cents) values (1, 1, 1250) returning id, pg_current_xact_id()::text as txid`)
await step('update order', `update orders set status = 'preparing' where id = (select min(id) from orders) returning id, pg_current_xact_id()::text as txid`)
await step('insert + update in one transaction', `with i as (insert into orders (store_id, customer_id, total_cents) values (1, 2, 830) returning id)
  update orders set status = 'ready' where id = (select min(id) from orders) returning (select id from i) as new_id, pg_current_xact_id()::text as txid`)
await c.end()
