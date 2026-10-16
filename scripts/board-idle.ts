// Does the board itself let the compute scale to zero? Alice's three board queries,
// sealed and renewed through the board's API exactly as the browser does (the React
// hook renews 10 s before each 60 s expiry), with the compute's suspend timeout at
// 60 s and its state polled every 5 s. Two versions of the API:
//   check-on-seal: looks the user up in store_members on every seal and renewal
//   no-check:      seals without touching Postgres (what the board ships with)
// Restores the suspend timeout at the end.
//   node scripts/board-idle.ts
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { createRealtimeClient, type SealedLiveQuery } from '@neon/realtime/client'
import { drizzleParsers } from '@neon/realtime-drizzle/client'
import { endpoints, neonApi, projectId } from '../lib/neon-api.ts'
import pg from 'pg'
import { drizzle } from 'drizzle-orm/node-postgres'
import * as schema from '../db/schema.ts'
import { directUrl } from '../lib/db.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { renewBeforeExpiry } from '../lib/renew.ts'
import { signIn } from '../lib/auth.ts'
import { codeHash } from '../lib/run.ts'
import { createApp } from '../server/app.ts'

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
const state = async () => (await endpoints()).find((e) => e.id === ep.id)!.current_state as string
async function untilIdle(label: string, maxS: number) {
  const start = now()
  let last = ''
  while (now() - start < maxS * 1000) {
    const s = await state()
    if (s !== last) { ev(`${label}: compute ${s}`); last = s }
    if (s === 'idle') return Math.round((now() - start) / 1000)
    await sleep(5000)
  }
  return null
}

const creds = JSON.parse(readFileSync('data/users.json', 'utf8'))
// Idle connections close after 1 s, so nothing held open by this script keeps the
// compute awake; the pool reconnects when the API needs it.
const pool = new pg.Pool({ connectionString: directUrl(), max: 2, idleTimeoutMillis: 1000 })
pool.on('error', () => {})
const db = drizzle(pool, { schema })
const sealer = createSealer()
await setSuspend(60)
ev(`suspend timeout ${original} s -> 60 s`)
const results: Record<string, unknown> = {}
try {
  for (const variant of ['check-on-seal', 'no-check'] as const) {
    const app = createApp(db, sealer, { checkMembershipOnSeal: variant === 'check-on-seal' })
    // sign in first (Neon Auth reads and writes its tables in this database)
    const { token } = await signIn(creds.alice.email, creds.alice.password)
    const seal = async (part: string): Promise<SealedLiveQuery<any>> => {
      const res = await app.request('/api/board/live', { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ storeId: 10, part }) })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return res.json()
    }
    // warm the JWKS cache, then let the compute suspend
    await seal('orders')
    ev(`${variant}: waiting for the compute to suspend before opening the board`)
    const before = await untilIdle(`${variant} before`, 600)
    if (before === null) throw new Error('compute did not suspend before the test')
    const client = createRealtimeClient({ url: realtimeUrl(), parsers: { ...drizzleParsers } })
    let renewals = 0
    const stops: (() => void)[] = []
    for (const part of ['orders', 'customers', 'stats']) {
      const first = await seal(part)
      const sub = client.subscribe(first)
      await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
      stops.push(renewBeforeExpiry(sub as any, first, () => seal(part), (e) => { if (e.ok) renewals++; else ev(`${variant}: renew failed ${e.message}`) }))
    }
    ev(`${variant}: board live (3 queries), no writes from here`)
    const idleAfter = await untilIdle(`${variant} board open`, 300)
    ev(`${variant}: ${idleAfter === null ? 'compute never suspended in 300 s' : `compute suspended ${idleAfter} s after the board opened`}; ${renewals} renewals`)
    results[variant] = { idle_after_s: idleAfter, renewals, watched_s: idleAfter ?? 300 }
    for (const stop of stops) stop()
    client.close()
  }
} finally {
  await setSuspend(original)
  ev(`suspend timeout restored to ${original} s`)
  await pool.end()
}
mkdirSync('results/board-idle', { recursive: true })
writeFileSync(`results/board-idle/board-idle-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`,
  JSON.stringify({ code_hash: codeHash(['scripts/board-idle.ts', 'server']), results, events }, null, 2), { flag: 'wx' })
console.log(JSON.stringify(results, null, 2))
process.exit(0)
