// Exploration: what does each query shape receive on the wire per write?
// Seeds one store with --rows orders, opens one client per shape, makes five
// single-row inserts two seconds apart, and records every frame.
//   node scripts/explore-shapes.ts --rows 1000 --store 901
import { parseArgs } from 'node:util'
import { createRealtimeClient } from '@neon/realtime/client'
import { installWireTap, now, tap } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { queryFor, SHAPES, type Shape } from '../lib/queries.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'

const { values } = parseArgs({ options: { rows: { type: 'string', default: '1000' }, store: { type: 'string', default: '901' } } })
const rows = Number(values.rows)
const storeId = Number(values.store)
const user = 'explore-user'

installWireTap()
const { db, pool } = createDb(2)
const c = await pool.connect()
await createStore(c, storeId, `explore ${storeId}`, [user])
await c.query('delete from orders where store_id = $1', [storeId])
await prefillOrders(c, storeId, rows, storeId)
c.release()

const sealer = createSealer()
const frames: Record<Shape, { t: number; bytes: number; type: string; txids?: string[] }[]> = { plain: [], join: [], aggregate: [] }
let current: Shape | undefined
tap.onFrame = (f) => {
  if (current) frames[current].push({ t: f.t, bytes: f.bytes, type: f.msg.type, txids: f.msg.txids })
}

// One client per shape, opened one at a time so each socket's frames are attributed to its shape.
const subs: any[] = []
for (const shape of SHAPES) {
  current = shape
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sealed = await sealer.seal({ query: queryFor(db, shape, storeId, user) as any })
  const sub = client.subscribe(sealed, { materialize: false })
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  subs.push({ shape, client, sub })
  // route later frames by socket: tag the socket that just opened
}
current = undefined
console.log(`store ${storeId}: ${rows} prefilled orders; all three subscriptions live`)

// Route frames by which client received them: wrap each client's socket listener.
import { sockets } from '../lib/wiretap.ts'
const socketShape = new Map<any, Shape>()
;[...sockets].forEach((s, i) => socketShape.set(s, SHAPES[i]))
for (const [s, shape] of socketShape) {
  s.addEventListener('message', (event: any) => {
    const text = String(event.data)
    const msg = JSON.parse(text)
    frames[shape].push({ t: now(), bytes: Buffer.byteLength(text), type: `after:${msg.type}`, txids: msg.txids })
  })
}

const writes: { seq: number; txid: string; tIssue: number; tAck: number }[] = []
for (let seq = 1; seq <= 5; seq++) {
  await new Promise((r) => setTimeout(r, 2000))
  const tIssue = now()
  const { rows: r } = await pool.query(
    `insert into orders (store_id, customer_id, status, total_cents, seq) values ($1, $2, 'new', 1000, $3)
     returning pg_current_xact_id()::text as txid`, [storeId, storeId * 1000 + 1, seq])
  writes.push({ seq, txid: r[0].txid, tIssue, tAck: now() })
}
await new Promise((r) => setTimeout(r, 3000))

for (const shape of SHAPES) {
  const after = frames[shape].filter((f) => f.type.startsWith('after:'))
  const byType: Record<string, { n: number; bytes: number }> = {}
  for (const f of after) {
    byType[f.type] ??= { n: 0, bytes: 0 }
    byType[f.type].n++
    byType[f.type].bytes += f.bytes
  }
  const total = after.reduce((a, f) => a + f.bytes, 0)
  console.log(`\n${shape}: ${after.length} frames, ${total} payload bytes for 5 writes (${Math.round(total / 5)} per write)`)
  console.table(byType)
}
console.log('\nwrites:', writes.map((w) => `seq ${w.seq} txid ${w.txid} ack ${(w.tAck - w.tIssue).toFixed(1)} ms`).join('; '))
for (const s of subs) { s.sub.unsubscribe(); s.client.close() }
await pool.end()
