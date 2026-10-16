// What does the Realtime client SDK cost the subscribing machine per update?
// For each shape: a fresh 200-row store, one SDK subscription (materialized, as an
// app would use it), 10 s idle to measure the baseline, then 300 writes at 20/s
// from a separate writer process. CPU time of this process during the writes,
// minus the idle baseline, divided by the updates received.
//   node scripts/sdk-cost.ts
import { spawn } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { createRealtimeClient } from '@neon/realtime/client'
import { now } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { queryFor, SHAPES } from '../lib/queries.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { codeHash, round } from '../lib/run.ts'

// No wire tap here: the client uses Node's built-in WebSocket, so only the SDK's own work is measured.
const { db, pool } = createDb(2)
const sealer = createSealer()
const cpuMs = () => { const u = process.cpuUsage(); return (u.user + u.system) / 1000 }
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const results: any[] = []
const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)

for (const [i, shape] of SHAPES.entries()) {
  const store = 96 + i, user = `cost-user-${store}`
  const c = await pool.connect()
  await createStore(c, store, `cost ${store}`, [user])
  await c.query('delete from orders where store_id = $1', [store])
  await prefillOrders(c, store, 200, store)
  c.release()
  const client = createRealtimeClient({ url: realtimeUrl() })
  const sub = client.subscribe(await sealer.seal({ query: queryFor(db, shape, store, user) as any }))
  let updates = 0
  sub.onUpdate(() => updates++)
  await new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
  const idle0 = cpuMs(), t0 = now()
  await sleep(10_000)
  const idleRate = (cpuMs() - idle0) / (now() - t0)

  const dir = `results/sdk-cost/${shape}-${stamp}`
  mkdirSync(dir, { recursive: true })
  writeFileSync(`${dir}/meta.json`, JSON.stringify({ run_id: `${shape}-${stamp}`, code_hash: codeHash(['scripts/sdk-cost.ts', 'scripts/bench-writer.ts']), seed: store }))
  const u0 = updates, c0 = cpuMs(), w0 = now()
  const writer = spawn('node', ['scripts/bench-writer.ts', '--dir', dir, '--store', String(store), '--rate', '20', '--duration', '15'], { stdio: 'ignore' })
  await new Promise((r) => writer.on('exit', r))
  await sleep(3000)
  const elapsed = now() - w0, used = cpuMs() - c0 - idleRate * elapsed, got = updates - u0
  results.push({ shape, updates: got, cpu_ms_total: round(used), cpu_ms_per_update: round(used / got, 2),
    max_updates_per_s_one_core: Math.round(1000 / (used / got)) })
  sub.unsubscribe(); client.close()
}
await pool.end()
console.table(results)
writeFileSync(`results/sdk-cost/sdk-cost-${stamp}.json`, JSON.stringify({ code_hash: codeHash(['scripts/sdk-cost.ts', 'scripts/bench-writer.ts']), results }, null, 2), { flag: 'wx' })
process.exit(0)
