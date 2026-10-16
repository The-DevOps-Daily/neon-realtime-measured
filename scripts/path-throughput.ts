// How much can the path from this machine to the region carry? A store with
// --rows orders, read three ways, three times each:
//   - a live query's initial result over the Live SQL API (WebSocket)
//   - the same rows over a direct Postgres connection
// Used to tell a server-side backlog from a full network pipe in the latency runs.
//   node scripts/path-throughput.ts --rows 20000 --store 95
import { mkdirSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { createRealtimeClient } from '@neon/realtime/client'
import { installWireTap, now, tap } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { joinQuery } from '../lib/queries.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { codeHash, round } from '../lib/run.ts'

const { values } = parseArgs({ options: { rows: { type: 'string', default: '20000' }, store: { type: 'string', default: '95' } } })
const N = Number(values.rows), S = Number(values.store), U = 'throughput-user'
installWireTap()
const { db, pool } = createDb(2)
const c = await pool.connect()
await createStore(c, S, 'throughput', [U])
const { rows: [{ n }] } = await c.query('select count(*)::int as n from orders where store_id = $1', [S])
if (n < N) await prefillOrders(c, S, N - n, S)
const sealer = createSealer()
const results: any[] = []

for (let i = 0; i < 3; i++) {
  let bytes = 0, first = 0, last = 0
  tap.onFrame = (f) => {
    if (f.msg.type === 'baseline_sync_start') first = f.t
    if (f.msg.type === 'baseline_sync_batch') { bytes += f.bytes; last = f.t }
  }
  const client = createRealtimeClient({ url: realtimeUrl() })
  const t0 = now()
  const sub = client.subscribe(await sealer.seal({ query: joinQuery(db, U, S) }), { materialize: false })
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  const tLive = now()
  sub.unsubscribe(); client.close()
  results.push({ path: 'Live SQL API initial result', bytes, ms: round(last - first), mb_per_s: round(bytes / 1e6 / ((last - first) / 1000), 2), subscribe_to_live_ms: round(tLive - t0) })

  const p0 = now()
  const r = await c.query({ text: `select o.id, o.status, o.total_cents, o.seq, cu.name as customer from orders o
    join customers cu on cu.id = o.customer_id join store_members m on m.store_id = o.store_id
    where m.user_id = $1 and o.store_id = $2`, values: [U, S] })
  const p1 = now()
  const approx = Buffer.byteLength(JSON.stringify(r.rows))
  results.push({ path: 'direct Postgres connection, same rows', rows: r.rowCount, json_bytes: approx, ms: round(p1 - p0), mb_per_s_json_equiv: round(approx / 1e6 / ((p1 - p0) / 1000), 2) })
}
c.release()
await pool.end()
console.table(results)
mkdirSync('results/throughput', { recursive: true })
writeFileSync(`results/throughput/throughput-${N}-rows-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`,
  JSON.stringify({ code_hash: codeHash(['scripts/path-throughput.ts']), rows: N, results }, null, 2), { flag: 'wx' })
