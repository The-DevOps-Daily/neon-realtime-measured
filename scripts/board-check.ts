// The board, end to end, without a browser: the same API routes and SDK calls the
// web app makes. Seeds the board first (scripts/seed-board.ts).
//   node scripts/board-check.ts
import { readFileSync } from 'node:fs'
import { createRealtimeClient } from '@neon/realtime/client'
import { drizzleParsers } from '@neon/realtime-drizzle/client'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { createApp } from '../server/app.ts'

const now = () => performance.timeOrigin + performance.now()
const creds = JSON.parse(readFileSync('data/users.json', 'utf8'))
const { db, pool } = createDb(3)
const app = createApp(db, createSealer())
const call = async (token: string | undefined, method: string, path: string, body?: unknown) => {
  const res = await app.request(path, { method, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) }, body: body ? JSON.stringify(body) : undefined })
  return { status: res.status, json: await res.json().catch(() => null) as any }
}
const check = (label: string, ok: boolean, detail = '') => { console.log(`${ok ? 'ok  ' : 'FAIL'}  ${label}${detail ? `: ${detail}` : ''}`); if (!ok) process.exitCode = 1 }

const alice = (await call(undefined, 'POST', '/api/session', creds.alice)).json
const eve = (await call(undefined, 'POST', '/api/session', creds.eve)).json
check('alice signs in through Neon Auth', !!alice?.token)
const stores = (await call(alice.token, 'GET', '/api/stores')).json
check('alice sees only her store', stores.length === 1 && stores[0].name === 'Main St', JSON.stringify(stores))
const storeId = stores[0].id

const client = createRealtimeClient({ url: realtimeUrl(), parsers: { ...drizzleParsers } })
const parts = ['orders', 'customers', 'stats'] as const
const subs: Record<string, any> = {}
for (const part of parts) {
  const sealed = (await call(alice.token, 'POST', '/api/board/live', { storeId, part })).json
  subs[part] = client.subscribe(sealed)
  await new Promise<void>((resolve) => subs[part].onStateChange((s: any) => s.status === 'live' && resolve()))
}
const open = () => subs.orders.getSnapshot().data as any[]
check('three live queries are live', true, `orders ${open().length}, customers ${subs.customers.getSnapshot().data.length}, status groups ${subs.stats.getSnapshot().data.length}`)

const customerId = subs.customers.getSnapshot().data[0].id
const t0 = now()
const created = (await call(alice.token, 'POST', '/api/orders', { storeId, customerId, totalCents: 4200 })).json
await subs.orders.awaitTxId(created.txid, 10_000)
check('a new order appears on the board', open().some((o) => o.id === created.id), `${Math.round(now() - t0)} ms from POST to awaitTxId`)

const t1 = now()
const moved = (await call(alice.token, 'PATCH', `/api/orders/${created.id}`, { status: 'preparing' })).json
await subs.orders.awaitTxId(moved.txid, 10_000)
check('moving it to preparing shows up', open().find((o) => o.id === created.id)?.status === 'preparing', `${Math.round(now() - t1)} ms`)
const t2 = now()
const done = (await call(alice.token, 'PATCH', `/api/orders/${created.id}`, { status: 'done' })).json
await subs.orders.awaitTxId(done.txid, 10_000)
check('a done order leaves the open board', !open().some((o) => o.id === created.id), `${Math.round(now() - t2)} ms`)
await subs.stats.awaitTxId(done.txid, 10_000)
check('the header counts include it', subs.stats.getSnapshot().data.some((r: any) => r.status === 'done' && r.orders >= 1))

// The API seals without a database lookup; the membership join in the query is
// what keeps eve out. Her sealed Main St board must come back empty.
{
  const sealed = (await call(eve.token, 'POST', '/api/board/live', { storeId, part: 'orders' })).json
  const sub = client.subscribe(sealed)
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  const rows = sub.getSnapshot().data ?? []
  check("eve's sealed Main St board has no rows", rows.length === 0, `${rows.length} rows; alice sees ${open().length}`)
  sub.unsubscribe()
}
check('eve cannot add an order to Main St', (await call(eve.token, 'POST', '/api/orders', { storeId, customerId, totalCents: 1 })).status === 403)
check('no token, no sealed query', (await call(undefined, 'POST', '/api/board/live', { storeId, part: 'orders' })).status === 401)

for (const s of Object.values(subs)) s.unsubscribe()
client.close()
await pool.end()
process.exit(process.exitCode ?? 0)
