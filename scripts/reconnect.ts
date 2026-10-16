// What a reconnect delivers. Two materialized subscriptions (the plain query and
// the three-table join) watch one store while a writer adds an order every 500 ms.
// The network is then cut twice, with every socket destroyed and new connects sent
// to an address that never answers:
//   short: 5 s, sealed queries still valid
//   long:  75 s, longer than the 60 s sealed-query lifetime, with renewal failing
//          too, as it would for a browser that is offline
// After each cut: time to `live`, which update types arrived, and
// whether the result matches the database exactly.
//   node scripts/reconnect.ts
import { mkdirSync, writeFileSync } from 'node:fs'
import { createRealtimeClient } from '@neon/realtime/client'
import { cutAll, installWireTap, now, tap } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { joinQuery, plainQuery } from '../lib/queries.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { renewBeforeExpiry } from '../lib/renew.ts'
import { codeHash } from '../lib/run.ts'

const STORE = 70, USER = 'reconnect-user'
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
installWireTap()
const { db, pool } = createDb(3)
const admin = await pool.connect()
await createStore(admin, STORE, 'reconnect', [USER])
await admin.query('delete from orders where store_id = $1', [STORE])
await prefillOrders(admin, STORE, 100, STORE)
const sealer = createSealer()

const t0 = now()
const events: any[] = []
const ev = (who: string, what: string, extra: Record<string, unknown> = {}) => {
  const e = { s: Math.round((now() - t0) / 10) / 100, who, what, ...extra }
  events.push(e)
  console.log(`${e.s.toFixed(2).padStart(7)}s  ${who.padEnd(5)} ${what}`, Object.keys(extra).length ? JSON.stringify(extra) : '')
}

let networkUp = true
const shapes = { plain: () => plainQuery(db, STORE), join: () => joinQuery(db, USER, STORE) }
const subs: Record<string, any> = {}
for (const [name, q] of Object.entries(shapes)) {
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sealed = await sealer.seal({ query: q() as any })
  const sub = client.subscribe(sealed)
  const rec = { name, client, sub, updates: [] as any[] }
  sub.onStateChange((s: any) => ev(name, `state ${s.status}`, s.error ? { code: s.error.code, retryable: s.error.retryable } : {}))
  sub.onUpdate((u: any) => rec.updates.push({ t: now(), type: u.type, n: u.type === 'rows' ? u.rows.length : u.changes.length }))
  // The browser would fetch a fresh sealed query from the API; offline, that fails too.
  renewBeforeExpiry(sub as any, sealed, async () => {
    if (!networkUp) throw new Error('offline')
    return sealer.seal({ query: q() as any })
  }, (e) => ev(name, e.ok ? 'renewed' : `renew failed: ${e.message}`))
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  subs[name] = rec
}
ev('setup', `both live: plain ${subs.plain.sub.getSnapshot().data.length} rows, join ${subs.join.sub.getSnapshot().data.length} rows`)

// Writer: one order every 500 ms for the whole test.
let writing = true
let paused = false
let written = 0
const writer = (async () => {
  while (writing) {
    if (paused) { await sleep(50); continue }
    try {
      await admin.query(`insert into orders (store_id, customer_id, total_cents, seq) values ($1, $2, 900, $3)`, [STORE, STORE * 1000 + 2, ++written])
    } catch (err: any) { ev('write', `error ${err.message}`) }
    await sleep(500)
  }
})()

async function truth(name: string) {
  const q = name === 'plain' ? plainQuery(db, STORE) : joinQuery(db, USER, STORE)
  const rows = await q
  return new Set(rows.map((r: any) => r.id))
}
// Pause the writer so the live result and the database are compared at rest.
async function check(label: string) {
  paused = true
  await sleep(1500)
  for (const name of Object.keys(subs)) {
    const snap = subs[name].sub.getSnapshot()
    const got = new Set((snap.data ?? []).map((r: any) => r.id))
    const want = await truth(name)
    const missing = [...want].filter((x) => !got.has(x)).length
    const extra = [...got].filter((x) => !want.has(x)).length
    ev(name, `${label}: ${got.size} rows, database ${want.size}, missing ${missing}, extra ${extra}`, { status: snap.status })
  }
  paused = false
}

async function outage(label: string, ms: number) {
  await sleep(8000)
  const marks = Object.fromEntries(Object.entries(subs).map(([n, r]) => [n, r.updates.length]))
  const writtenBefore = written
  ev('net', `${label}: cut for ${ms / 1000} s`)
  networkUp = false
  tap.blackhole = true
  cutAll()
  await sleep(ms)
  tap.blackhole = false
  networkUp = true
  const restored = now()
  ev('net', `${label}: restored; ${written - writtenBefore} orders were written during the cut`)
  const result: Record<string, unknown> = {}
  for (const [name, rec] of Object.entries(subs)) {
    const liveAt = await (async () => {
      const end = now() + 180_000
      while (now() < end) { if (rec.sub.getState().status === 'live' && rec.updates.slice(marks[name]).some((u: any) => u.t > restored)) return now(); await sleep(20) }
    })()
    const after = rec.updates.slice(marks[name]).filter((u: any) => u.t > restored)
    const first = after[0]
    result[name] = { live_s_after_restore: liveAt ? Math.round((liveAt - restored) / 10) / 100 : null,
      first_update_after_restore: first ? `${first.type} (${first.n} ${first.type === 'rows' ? 'rows' : 'changes'})` : null }
  }
  await sleep(2000)
  await check(`${label} +2 s`)
  return result
}

const short = await outage('short', 5000)
const long = await outage('long', 75_000)
writing = false
await writer
await sleep(2000)
await check('final')

for (const r of Object.values(subs)) r.sub.unsubscribe(), r.client.close()
admin.release()
await pool.end()
mkdirSync('results/reconnect', { recursive: true })
const file = `results/reconnect/reconnect-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`
writeFileSync(file, JSON.stringify({ code_hash: codeHash(['scripts/reconnect.ts']), short, long, events }, null, 2), { flag: 'wx' })
console.log(JSON.stringify({ short, long }, null, 2))
process.exit(0)
