// Exploration, second set: the board's own live queries and a few more forms, under
// three kinds of write: an order insert, an order status change and a customer insert.
// Records, per variant and per write, whether it got an incremental batch, a full
// reset, or nothing, and how many bytes arrived.
//   node scripts/explore-variants.ts --rows 300 --store 903
import { mkdirSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { createRealtimeClient } from '@neon/realtime/client'
import { rawSql } from '@neon/realtime/server'
import { installWireTap, sockets } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { codeHash } from '../lib/run.ts'

const { values } = parseArgs({ options: { rows: { type: 'string', default: '300' }, store: { type: 'string', default: '903' } } })
const rows = Number(values.rows), S = Number(values.store), U = `explore-user-${S}`

const variants: [string, string, unknown[]][] = [
  ['board: open orders (members join + status filter)', `SELECT o.id, o.customer_id, o.status, o.total_cents FROM orders o JOIN store_members m ON m.store_id = o.store_id WHERE m.user_id = $1 AND o.store_id = $2 AND o.status <> 'done'`, [U, S]],
  ['board: customers (members join)', 'SELECT c.id, c.name FROM customers c JOIN store_members m ON m.store_id = c.store_id WHERE m.user_id = $1 AND c.store_id = $2', [U, S]],
  ['board: counts per status (members join + GROUP BY)', 'SELECT o.status, count(*)::int AS orders FROM orders o JOIN store_members m ON m.store_id = o.store_id WHERE m.user_id = $1 AND o.store_id = $2 GROUP BY o.status', [U, S]],
  ['join orders-customers, both pinned to the store', 'SELECT o.id, o.status, c.name AS customer FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.store_id = $1 AND c.store_id = $1', [S]],
  ['plain + ORDER BY id, no LIMIT', 'SELECT id, status FROM orders WHERE store_id = $1 ORDER BY id', [S]],
  ['plain + LIMIT 50, no ORDER BY', 'SELECT id, status FROM orders WHERE store_id = $1 LIMIT 50', [S]],
  ['SELECT DISTINCT status', 'SELECT DISTINCT status FROM orders WHERE store_id = $1', [S]],
  ['plain filter (control)', 'SELECT id, status, total_cents FROM orders WHERE store_id = $1', [S]],
]

installWireTap()
const { pool } = createDb(2)
const c = await pool.connect()
await createStore(c, S, `explore ${S}`, [U])
await c.query('delete from orders where store_id = $1', [S])
await c.query('delete from customers where store_id = $1 and id > $2', [S, S * 1000 + 20])
await prefillOrders(c, S, rows, S)
const sealer = createSealer()

type Bucket = { keyed: number; resets: number; bytes: number }
const stats = new Map<string, Record<string, Bucket>>()
let current: string | undefined
const subs: any[] = []
for (const [name, sql, params] of variants) {
  const before = new Set(sockets)
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sub = client.subscribe(await sealer.seal({ query: rawSql(sql, params as any) }), { materialize: false })
  const result = await new Promise<string>((resolve) => sub.onStateChange((s: any) => {
    if (s.status === 'live') resolve('live')
    if (s.status === 'error') resolve(`error: ${s.error?.code} ${s.error?.message}`)
  }))
  const socket = [...sockets].find((s) => !before.has(s))
  const per: Record<string, Bucket> = {}
  stats.set(name, per)
  socket?.addEventListener('message', (event: any) => {
    if (!current) return
    const text = String(event.data)
    const msg = JSON.parse(text)
    if (msg.type === 'progress' || msg.type === 'ping') return
    per[current] ??= { keyed: 0, resets: 0, bytes: 0 }
    per[current].bytes += Buffer.byteLength(text)
    if (msg.type === 'reset_required') per[current].resets++
    if (msg.type === 'keyed_results') per[current].keyed++
  })
  subs.push({ client, sub })
  console.log(`${name}: ${result}`)
}

const writes: [string, string, unknown[]][] = [
  ['order insert', `insert into orders (store_id, customer_id, status, total_cents) values ($1, $2, 'new', 1000)`, [S, S * 1000 + 1]],
  ['order status -> done', `update orders set status = 'done' where id = (select min(id) from orders where store_id = $1 and status <> 'done')`, [S]],
  ['customer insert', `insert into customers (id, store_id, name) values ($1, $2, 'New customer') on conflict (id) do update set name = excluded.name || ' again'`, [S * 1000 + 21, S]],
]
for (const [label, sql, params] of writes) {
  current = label
  await c.query(sql, params)
  await new Promise((r) => setTimeout(r, 2500))
}
current = undefined
c.release()

const table = [...stats].map(([name, per]) => Object.fromEntries([['variant', name], ...writes.map(([label]) => {
  const b = per[label]
  return [label, !b ? 'nothing' : b.resets ? `reset, ${b.bytes} B` : b.keyed ? `incremental, ${b.bytes} B` : `${b.bytes} B`]
})]))
console.log(`\nstore ${S}: ${rows} prefilled orders`)
console.table(table)
mkdirSync('results/explore', { recursive: true })
writeFileSync(`results/explore/variants-${rows}-rows-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`,
  JSON.stringify({ code_hash: codeHash(['scripts/explore-variants.ts']), rows, store: S, variants: variants.map(([n, s]) => ({ name: n, sql: s })), table }, null, 2), { flag: 'wx' })
for (const s of subs) { s.sub.unsubscribe(); s.client.close() }
await pool.end()
