// Exploration: which query forms stay incremental (keyed_results) and which get a
// full reset (reset_required + a new baseline) on every write?
// Each variant gets its own client. Three single-row inserts, then a summary.
//   node scripts/explore-incremental.ts --rows 300 --store 902
import { parseArgs } from 'node:util'
import { createRealtimeClient } from '@neon/realtime/client'
import { rawSql } from '@neon/realtime/server'
import { installWireTap, now, sockets } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'

const { values } = parseArgs({ options: { rows: { type: 'string', default: '300' }, store: { type: 'string', default: '902' }, user: { type: 'string' } } })
const rows = Number(values.rows)
const S = Number(values.store)
// A user who belongs to this store only, so every variant returns the same orders.
const U = values.user ?? `explore-user-${S}`

const variants: [string, string, unknown[]][] = [
  ['plain filter', 'SELECT id, status, total_cents FROM orders WHERE store_id = $1', [S]],
  ['plain + ORDER BY id DESC LIMIT 20', 'SELECT id, status, total_cents FROM orders WHERE store_id = $1 ORDER BY id DESC LIMIT 20', [S]],
  ['join orders-customers', 'SELECT o.id, o.status, c.name AS customer FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.store_id = $1', [S]],
  ['join through store_members', 'SELECT o.id, o.status FROM orders o JOIN store_members m ON m.store_id = o.store_id WHERE m.user_id = $1 AND o.store_id = $2', [U, S]],
  ['EXISTS membership check', 'SELECT o.id, o.status FROM orders o WHERE o.store_id = $2 AND EXISTS (SELECT 1 FROM store_members m WHERE m.store_id = o.store_id AND m.user_id = $1)', [U, S]],
  ['IN subquery membership', 'SELECT o.id, o.status FROM orders o WHERE o.store_id IN (SELECT m.store_id FROM store_members m WHERE m.user_id = $1)', [U]],
  ['aggregate GROUP BY status', 'SELECT status, count(*)::int AS n FROM orders WHERE store_id = $1 GROUP BY status', [S]],
  ['count(*) only', 'SELECT count(*)::int AS n FROM orders WHERE store_id = $1', [S]],
]

installWireTap()
const { pool } = createDb(2)
const c = await pool.connect()
await createStore(c, S, `explore ${S}`, [U])
await c.query('delete from orders where store_id = $1', [S])
await prefillOrders(c, S, rows, S)
c.release()
const sealer = createSealer()

type Stat = { frames: number; bytes: number; resets: number; keyed: number; baselineBytes: number; resultRows: number }
const stats = new Map<string, Stat>()
let recording = false
const subs: any[] = []
for (const [name, sql, params] of variants) {
  const before = new Set(sockets)
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sealed = await sealer.seal({ query: rawSql(sql, params as any) })
  const sub = client.subscribe(sealed, { materialize: false })
  let resultRows = 0
  sub.onUpdate((u: any) => { if (u.type === 'rows') resultRows = u.rows.length })
  const result = await new Promise<string>((resolve) => {
    sub.onStateChange((s: any) => {
      if (s.status === 'live') resolve('live')
      if (s.status === 'error') resolve(`error: ${s.error?.code} ${s.error?.message}`)
    })
  })
  const socket = [...sockets].find((s) => !before.has(s))
  const st: Stat = { frames: 0, bytes: 0, resets: 0, keyed: 0, baselineBytes: 0, resultRows }
  stats.set(name, st)
  socket?.addEventListener('message', (event: any) => {
    if (!recording) return
    const text = String(event.data)
    const msg = JSON.parse(text)
    st.frames++
    st.bytes += Buffer.byteLength(text)
    if (msg.type === 'reset_required') st.resets++
    if (msg.type === 'keyed_results') st.keyed++
    if (msg.type === 'baseline_sync_batch') st.baselineBytes += Buffer.byteLength(text)
  })
  subs.push({ client, sub })
  console.log(`${name}: ${result}`)
}

recording = true
for (let seq = 1; seq <= 3; seq++) {
  await new Promise((r) => setTimeout(r, 2000))
  await pool.query(`insert into orders (store_id, customer_id, status, total_cents, seq) values ($1, $2, 'new', 1000, $3)`, [S, S * 1000 + 1, seq])
}
await new Promise((r) => setTimeout(r, 3000))
recording = false

console.log(`\nstore ${S}: ${rows} prefilled orders, 3 single-row inserts`)
console.table(
  [...stats].map(([name, s]) => ({
    variant: name,
    'result rows': s.resultRows,
    'incremental batches': s.keyed,
    'full resets': s.resets,
    'bytes per write': Math.round(s.bytes / 3),
    'of which re-sent rows': Math.round(s.baselineBytes / 3),
  })),
)
for (const s of subs) { s.sub.unsubscribe(); s.client.close() }
await pool.end()
console.log(`finished ${new Date(now()).toISOString()}`)
