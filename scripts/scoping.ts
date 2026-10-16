// Per-user scoping without row-level security, measured through the board API with
// real Neon Auth users. Three cases:
//   1. the board's query joins through store_members: remove a member, time how
//      long their live result takes to empty, then check nothing new reaches them
//   2. a seal-time check only (the query itself has no membership join): remove a
//      member and count the new orders that still reach them, and for how long
//   3. a sealed query copied to another user: how long it keeps working
//   node scripts/scoping.ts
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { randomBytes } from 'node:crypto'
import { and, eq, ne } from 'drizzle-orm'
import { createRealtimeClient, type SealedLiveQuery } from '@neon/realtime/client'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { signIn } from '../lib/auth.ts'
import { createStore } from '../lib/seed.ts'
import { renewBeforeExpiry } from '../lib/renew.ts'
import { codeHash } from '../lib/run.ts'
import { createApp } from '../server/app.ts'
import { orders, storeMembers } from '../db/schema.ts'

const STORE = 60
const now = () => performance.timeOrigin + performance.now()
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const log: any[] = []
const t0 = now()
const note = (who: string, what: string, extra: Record<string, unknown> = {}) => {
  const e = { s: Math.round((now() - t0) / 100) / 10, who, what, ...extra }
  log.push(e)
  console.log(`${String(e.s).padStart(6)}s  ${who.padEnd(6)} ${what}`, Object.keys(extra).length ? JSON.stringify(extra) : '')
}

// Test users on example.com, which receives no mail. Passwords are generated once
// and kept in data/users.json (git-ignored).
mkdirSync('data', { recursive: true })
const usersFile = 'data/users.json'
const creds = existsSync(usersFile) ? JSON.parse(readFileSync(usersFile, 'utf8'))
  : Object.fromEntries(['alice', 'bob', 'eve'].map((n) => [n, { email: `${n}.realtime@example.com`, password: randomBytes(12).toString('base64url') }]))
writeFileSync(usersFile, JSON.stringify(creds, null, 2))
const users: Record<string, { token: string; userId: string }> = {}
for (const name of ['alice', 'bob', 'eve']) users[name] = await signIn(creds[name].email, creds[name].password)

const { db, pool } = createDb(3)
const sealer = createSealer()
const app = createApp(db, sealer)
const admin = await pool.connect()
await createStore(admin, STORE, 'Harbour', [users.alice.userId, users.bob.userId])
await admin.query('delete from store_members where store_id = $1 and user_id = $2', [STORE, users.eve.userId])
const addOrder = async () => {
  const { rows } = await admin.query(`insert into orders (store_id, customer_id, total_cents) values ($1, $2, 1500) returning id`, [STORE, STORE * 1000 + 1])
  return rows[0].id as number
}
await addOrder()

async function api(name: string, body: unknown): Promise<SealedLiveQuery<any>> {
  const res = await app.request('/api/board/live', { method: 'POST', headers: { Authorization: `Bearer ${users[name].token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  if (!res.ok) throw new Error(`HTTP ${res.status} ${await res.text()}`)
  return res.json()
}

// The anti-pattern for case 2: membership is checked when sealing, but the sealed
// query itself is not scoped to the member.
async function sealTimeOnly(name: string) {
  const member = await db.select().from(storeMembers).where(and(eq(storeMembers.store_id, STORE), eq(storeMembers.user_id, users[name].userId)))
  if (!member.length) throw new Error('HTTP 403 not a member')
  return sealer.seal({ query: db.select({ id: orders.id, status: orders.status }).from(orders).where(and(eq(orders.store_id, STORE), ne(orders.status, 'done'))) })
}

function watch(who: string, first: SealedLiveQuery<any>, refresh: () => Promise<SealedLiveQuery<any>>) {
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sub = client.subscribe(first)
  const seen = new Set<number>()
  let lastNewAt: number | undefined
  sub.onStateChange((s: any) => note(who, `state ${s.status}`, s.error ? { code: s.error.code } : {}))
  sub.onChange((snap: any) => {
    for (const r of snap.data ?? []) if (!seen.has(r.id)) { seen.add(r.id); lastNewAt = now() }
  })
  // A refused renewal is retried every second; log the first refusal and count the rest.
  let refusals = 0
  const stop = renewBeforeExpiry(sub as any, first, refresh, (e) => {
    if (e.ok) return note(who, 'renewed')
    if (refusals++ === 0) note(who, `renew refused: ${e.message} (retried every second from here)`)
  })
  const live = new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  return { sub, client, seen, live, rows: () => sub.getSnapshot().data ?? [], lastNewAt: () => lastNewAt, refusals: () => refusals,
    close: () => { stop(); sub.unsubscribe(); client.close() } }
}
const waitFor = async (cond: () => boolean, ms: number) => { const end = now() + ms; while (!cond() && now() < end) await sleep(5); return cond() ? now() : undefined }

const summary: Record<string, unknown> = {}

// Case 1: the join is the authorization.
{
  const sealed = await api('alice', { storeId: STORE, part: 'orders' })
  const w = watch('alice', sealed, () => api('alice', { storeId: STORE, part: 'orders' }))
  await w.live
  note('alice', `live with ${w.rows().length} open orders`)
  const removedAt = now()
  await admin.query('delete from store_members where store_id = $1 and user_id = $2', [STORE, users.alice.userId])
  const committed = now()
  note('admin', 'removed alice from Harbour', { commit_ms: Math.round(committed - removedAt) })
  const emptyAt = await waitFor(() => w.rows().length === 0, 10_000)
  note('alice', emptyAt ? `result empty ${Math.round(emptyAt - committed)} ms after the removal committed` : 'result NOT empty after 10 s')
  const after: number[] = []
  for (let i = 0; i < 6; i++) { after.push(await addOrder()); await sleep(10_000) }
  const leaked = after.filter((id) => w.seen.has(id))
  note('alice', `${leaked.length} of ${after.length} orders added after removal reached her`)
  summary.case1 = { emptied_ms_after_commit: emptyAt ? Math.round(emptyAt - committed) : null, orders_after_removal: after.length, leaked: leaked.length }
  w.close()
}

// Case 2: membership checked at seal time only.
{
  const sealed = await sealTimeOnly('bob')
  const w = watch('bob', sealed, () => sealTimeOnly('bob'))
  await w.live
  note('bob', `live with ${w.rows().length} open orders (seal-time check only)`)
  const removedAt = now()
  await admin.query('delete from store_members where store_id = $1 and user_id = $2', [STORE, users.bob.userId])
  note('admin', 'removed bob from Harbour')
  const after: { id: number; at: number }[] = []
  for (let i = 0; i < 18; i++) { after.push({ id: await addOrder(), at: now() }); await sleep(5_000) }
  await sleep(3000)
  const leaked = after.filter((o) => w.seen.has(o.id))
  const lastLeak = leaked.at(-1)
  note('bob', `${leaked.length} of ${after.length} orders added after removal reached him; the last was added ${lastLeak ? Math.round((lastLeak.at - removedAt) / 1000) : '-'} s after removal`)
  summary.case2 = { orders_after_removal: after.length, leaked: leaked.length, last_leak_s_after_removal: lastLeak ? Math.round((lastLeak.at - removedAt) / 100) / 10 : null,
    sealed_expires_s_after_removal: Math.round((sealed.expiresAt - removedAt) / 100) / 10 }
  w.close()
  await admin.query('insert into store_members (store_id, user_id) values ($1, $2) on conflict do nothing', [STORE, users.bob.userId])
}

// Case 3: bob's sealed query in eve's hands. Eve cannot renew it: the API refuses her.
{
  const bobs = await api('bob', { storeId: STORE, part: 'orders' })
  const issuedAt = now()
  const w = watch('eve', bobs, () => api('eve', { storeId: STORE, part: 'orders' }))
  await w.live
  note('eve', `live on bob's sealed query with ${w.rows().length} of Harbour's open orders`)
  const after: { id: number; at: number }[] = []
  for (let i = 0; i < 18; i++) { after.push({ id: await addOrder(), at: now() }); await sleep(5_000) }
  await sleep(3000)
  const leaked = after.filter((o) => w.seen.has(o.id))
  const last = leaked.at(-1)
  note('eve', `${leaked.length} of ${after.length} new orders reached her; the last was added ${last ? Math.round((last.at - issuedAt) / 1000) : '-'} s after bob's query was sealed`)
  summary.case3 = { initial_rows: w.rows().length, orders_added: after.length, received: leaked.length,
    last_received_s_after_seal: last ? Math.round((last.at - issuedAt) / 100) / 10 : null, sealed_lifetime_s: Math.round((bobs.expiresAt - issuedAt) / 100) / 10 }
  w.close()
}

admin.release()
await pool.end()
mkdirSync('results/scoping', { recursive: true })
const file = `results/scoping/scoping-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`
writeFileSync(file, JSON.stringify({ code_hash: codeHash(['scripts/scoping.ts', 'server']), summary, log }, null, 2), { flag: 'wx' })
console.log(JSON.stringify(summary, null, 2))
process.exit(0)
