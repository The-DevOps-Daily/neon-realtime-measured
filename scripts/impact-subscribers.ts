// Helper for primary-impact.ts: holds N live queries (one per user, 10 per client)
// open until SIGTERM, counting the updates they receive. Prints LIVE when all are live.
//   node scripts/impact-subscribers.ts --store 80 --users 100 --out <file>
import { writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { createRealtimeClient } from '@neon/realtime/client'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { memberQuery } from '../lib/queries.ts'
import { renewBeforeExpiry } from '../lib/renew.ts'

const { values } = parseArgs({ options: { store: { type: 'string' }, users: { type: 'string' }, out: { type: 'string' } } })
const store = Number(values.store), n = Number(values.users)
const { db, pool } = createDb(1)
await pool.end()
const sealer = createSealer()
let updates = 0, errors = 0
const clients: any[] = []
const subs: any[] = []
const lives: Promise<void>[] = []
for (let i = 0; i < n; i++) {
  if (i % 10 === 0) clients.push(createRealtimeClient({ url: realtimeUrl() }))
  const q = () => sealer.seal({ query: memberQuery(db, `impact-user-${i}`, store) })
  const sealed = await q()
  const sub = clients.at(-1).subscribe(sealed, { materialize: false })
  sub.onUpdate(() => updates++)
  lives.push(new Promise<void>((resolve) => sub.onStateChange((s: any) => { if (s.status === 'live') resolve(); if (s.status === 'error') errors++ })))
  renewBeforeExpiry(sub, sealed, q)
  subs.push(sub)
}
await Promise.all(lives)
console.log('LIVE')
process.on('SIGTERM', () => {
  writeFileSync(values.out!, JSON.stringify({ subscriptions: n, updates, errors }))
  for (const s of subs) s.unsubscribe()
  for (const c of clients) c.close()
  process.exit(0)
})
