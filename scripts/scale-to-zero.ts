// Does an open live query keep the compute awake? Uses the Neon API to shorten the
// compute's suspend timeout to 60 s, then polls its state every 5 s through:
//   control:   no subscriptions, no connections
//   held:      one live subscription held open (and renewed) with no writes
//   wake:      a write while the compute is suspended and the subscription is open
//   cold sub:  a new subscription while the compute is suspended, no connection
// The suspend timeout is restored at the end. No database connection is held
// open while waiting; the writes open one, write, and close it.
//   node scripts/scale-to-zero.ts
import { mkdirSync, writeFileSync } from 'node:fs'
import { createRealtimeClient } from '@neon/realtime/client'
import { endpoints, neonApi, projectId } from '../lib/neon-api.ts'
import { directClient } from '../lib/db.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { plainQuery } from '../lib/queries.ts'
import { createStore } from '../lib/seed.ts'
import { renewBeforeExpiry } from '../lib/renew.ts'
import { codeHash } from '../lib/run.ts'

const STORE = 90
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const now = () => performance.timeOrigin + performance.now()
const t0 = now()
const events: any[] = []
const ev = (what: string, extra: Record<string, unknown> = {}) => {
  const e = { s: Math.round((now() - t0) / 100) / 10, at: new Date().toISOString(), what, ...extra }
  events.push(e)
  console.log(`${e.s.toFixed(1).padStart(7)}s  ${what}`, Object.keys(extra).length ? JSON.stringify(extra) : '')
}

const [ep] = await endpoints()
const original = ep.suspend_timeout_seconds
const setSuspend = (s: number) => neonApi(`/projects/${projectId()}/endpoints/${ep.id}`, { method: 'PATCH', body: JSON.stringify({ endpoint: { suspend_timeout_seconds: s } }) })
const state = async () => (await endpoints()).find((e) => e.id === ep.id)!
const usage = async () => { const { project } = await neonApi(`/projects/${projectId()}`); return { compute_time_seconds: project.compute_time_seconds, active_time_seconds: project.active_time_seconds, data_transfer_bytes: project.data_transfer_bytes } }

async function oneShot(sql: string, params: unknown[] = []) {
  const t = now()
  const c = directClient()
  await c.connect()
  const connected = now()
  const r = await c.query(sql, params)
  const done = now()
  await c.end()
  return { rows: r.rows, connect_ms: Math.round(connected - t), query_ms: Math.round(done - connected) }
}

// Poll until the compute reports idle, or the deadline passes. Returns seconds waited.
async function untilIdle(label: string, maxS: number) {
  const start = now()
  let last = ''
  while (now() - start < maxS * 1000) {
    const e = await state()
    if (e.current_state !== last) { ev(`${label}: compute ${e.current_state}`, { last_active: e.last_active }); last = e.current_state }
    if (e.current_state === 'idle') return Math.round((now() - start) / 1000)
    await sleep(5000)
  }
  return null
}

const usageBefore = await usage()
ev('usage before', usageBefore)
await setSuspend(60)
ev(`suspend timeout ${original} s -> 60 s`)
{
  const c = directClient(); await c.connect()
  await createStore(c, STORE, 'scale-to-zero', ['s2z-user'])
  await c.end()
}

const results: Record<string, unknown> = {}
try {
  // Control: nothing open.
  await oneShot('select 1')
  ev('control: last query done, nothing open')
  results.control_idle_after_s = await untilIdle('control', 480)

  // Held: one subscription, renewed every ~50 s, no writes.
  const { db, pool } = createDb(1)
  await pool.end() // the Drizzle client only builds the query; no connection is needed
  const sealer = createSealer()
  const seal = () => sealer.seal({ query: plainQuery(db, STORE) })
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sealed = await seal()
  const sub = client.subscribe(sealed)
  const states: any[] = []
  sub.onStateChange((s: any) => { states.push({ t: now(), status: s.status }); ev(`subscription ${s.status}`, s.error ? { code: s.error.code } : {}) })
  let renewals = 0
  renewBeforeExpiry(sub as any, sealed, seal, (e) => { if (e.ok) renewals++; else ev(`renew failed ${e.message}`) })
  const subStart = now()
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  results.first_result_while_active_ms = Math.round(now() - subStart)
  ev('held: subscription live, no writes from here')
  results.held_idle_after_s = await untilIdle('held', 480)
  ev(`held: ${renewals} renewals so far, subscription is ${sub.getState().status}`)
  await sleep(60_000)
  const stillIdle = (await state()).current_state
  ev(`held: 60 s later the compute is ${stillIdle}, subscription is ${sub.getState().status}`)
  results.held_still_idle_60s_later = stillIdle === 'idle'
  results.renewals_while_held = renewals

  // Wake: write while suspended. How long until the open subscription sees it?
  const seen = new Promise<number>((resolve) => sub.onChange((snap: any) => { if ((snap.data ?? []).some((r: any) => r.seq === 777)) resolve(now()) }))
  const wIssue = now()
  const w = await oneShot(`insert into orders (store_id, customer_id, total_cents, seq) values ($1, $2, 100, 777) returning pg_current_xact_id()::text as txid`, [STORE, STORE * 1000 + 1])
  const wAck = now()
  const seenAt = await Promise.race([seen, sleep(60_000).then(() => undefined)])
  results.wake = { connect_ms: w.connect_ms, insert_ms: w.query_ms, issue_to_ack_ms: Math.round(wAck - wIssue),
    issue_to_update_ms: seenAt ? Math.round(seenAt - wIssue) : null }
  ev('wake: write while suspended', results.wake as any)

  // Cold subscription: let it suspend again, close everything, then subscribe fresh.
  results.after_wake_idle_after_s = await untilIdle('after wake', 480)
  sub.unsubscribe(); client.close()
  ev('cold: all subscriptions closed, compute idle')
  await sleep(10_000)
  const client2 = createRealtimeClient({ url: realtimeUrl() })
  const t = now()
  const sub2 = client2.subscribe(await seal())
  await new Promise<void>((resolve) => sub2.onStateChange((s: any) => s.status === 'live' && resolve()))
  results.cold_first_result_ms = Math.round(now() - t)
  const afterCold = await state()
  ev('cold: new subscription live', { first_result_ms: results.cold_first_result_ms, compute: afterCold.current_state })
  results.cold_woke_compute = afterCold.current_state !== 'idle'
  sub2.unsubscribe(); client2.close()
} finally {
  await setSuspend(original)
  ev(`suspend timeout restored to ${original} s`)
}
await sleep(5000)
const usageAfter = await usage()
ev('usage after (the API counters can lag)', usageAfter)
mkdirSync('results/scale-to-zero', { recursive: true })
const file = `results/scale-to-zero/scale-to-zero-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`
writeFileSync(file, JSON.stringify({ code_hash: codeHash(['scripts/scale-to-zero.ts']), results, usageBefore, usageAfter, events }, null, 2), { flag: 'wx' })
console.log(JSON.stringify(results, null, 2))
process.exit(0)
