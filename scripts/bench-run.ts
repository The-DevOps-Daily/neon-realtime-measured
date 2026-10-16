// One latency run: a fresh store with a fixed window of orders, one subscriber,
// one open-loop writer, then a drain. Raw output lands in results/latency/<run id>/.
//   node scripts/bench-run.ts --shape plain --rate 10 --duration 180 [--window 200] [--label A]
import { spawn } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { loadavg } from 'node:os'
import { readFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { directClient } from '../lib/db.ts'
import { acquireLock, codeHash } from '../lib/run.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { endpoints } from '../lib/neon-api.ts'
import { SHAPES, type Shape } from '../lib/queries.ts'

const { values } = parseArgs({
  options: {
    shape: { type: 'string' }, rate: { type: 'string' }, duration: { type: 'string' },
    window: { type: 'string', default: '200' }, label: { type: 'string', default: 'A' },
    root: { type: 'string', default: 'results/latency' }, 'max-load': { type: 'string', default: '2.5' },
    subscriber: { type: 'string', default: 'sdk' }, // 'raw': the diagnostic recorder without the SDK
  },
})
const shape = values.shape as Shape
if (!SHAPES.includes(shape)) throw new Error(`--shape must be one of ${SHAPES.join(', ')}`)
const rate = Number(values.rate)
const duration = Number(values.duration)
const window = Number(values.window)
const root = values.root!
const release = acquireLock(root)

// Other work on this machine adds scheduling noise to every timestamp. Wait (up to
// 15 minutes) for the 1-minute load average to drop under --max-load, and record it.
const maxLoad = Number(values['max-load'])
// Swapping freezes the recorders, so also wait for free memory.
const memory = () => Object.fromEntries(readFileSync('/proc/meminfo', 'utf8').split('\n')
  .filter((l) => /^(MemAvailable|SwapTotal|SwapFree):/.test(l)).map((l) => [l.split(':')[0], Math.round(Number(l.split(/\s+/)[1]) / 1024)]))
const waitStart = Date.now()
while ((loadavg()[0] > maxLoad || memory().MemAvailable < 700) && Date.now() - waitStart < 15 * 60_000) await new Promise((r) => setTimeout(r, 15_000))
const loadAtStart = loadavg()
const memoryAtStart = memory()

const c = directClient()
await c.connect()
const { rows: [{ next }] } = await c.query('select greatest(coalesce(max(id), 0) + 1, 1000) as next from stores')
const storeId = Number(next)
const user = `bench-user-${storeId}`
await createStore(c, storeId, `bench ${storeId}`, [user])
await prefillOrders(c, storeId, window, storeId)
await c.end()

const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
const raw = values.subscriber === 'raw'
const runId = `${shape}-${rate}ps-w${window}${raw ? '-raw' : ''}-${values.label}-${stamp}`
const dir = `${root}/${runId}`
mkdirSync(dir, { recursive: true })
const [ep] = await endpoints()
const meta = {
  run_id: runId, code_hash: codeHash(['scripts/bench-run.ts', 'scripts/bench-subscriber.ts', 'scripts/bench-writer.ts', 'scripts/raw-subscriber.ts']), shape, rate, duration, window, label: values.label,
  subscriber: raw ? 'raw protocol, no SDK (diagnostic)' : '@neon/realtime client SDK',
  store_id: storeId, user, seed: storeId, started_at: new Date().toISOString(),
  node: process.version, sdk: '@neon/realtime@0.1.0', region: 'aws-eu-central-1',
  compute: { id: ep.id, min_cu: ep.autoscaling_limit_min_cu, max_cu: ep.autoscaling_limit_max_cu },
  loadavg_at_start: loadAtStart, memory_mb_at_start: memoryAtStart, waited_for_load_s: Math.round((Date.now() - waitStart) / 1000),
}
writeFileSync(`${dir}/meta.json`, JSON.stringify(meta, null, 2))
console.log(`run ${runId}: store ${storeId}, window ${window}, ${rate}/s for ${duration}s`)

const sub = spawn('node', [raw ? 'scripts/raw-subscriber.ts' : 'scripts/bench-subscriber.ts', '--dir', dir, '--shape', shape, '--store', String(storeId), '--user', user], { stdio: ['ignore', 'pipe', 'inherit'] })
await new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('subscriber did not get live + pinged within 90 s')), 90_000)
  sub.stdout.on('data', (d) => {
    if (String(d).includes('PINGED')) {
      clearTimeout(timer)
      resolve()
    }
  })
  sub.on('exit', (code) => reject(new Error(`subscriber exited early (${code})`)))
})
await new Promise((r) => setTimeout(r, 3000))

const writer = spawn('node', ['scripts/bench-writer.ts', '--dir', dir, '--store', String(storeId), '--rate', String(rate), '--duration', String(duration)], { stdio: 'inherit' })
const writerCode = await new Promise<number | null>((resolve) => writer.on('exit', resolve))
await new Promise((r) => setTimeout(r, 10_000)) // drain
sub.removeAllListeners('exit')
sub.kill('SIGTERM')
await new Promise((resolve) => sub.on('exit', resolve))

writeFileSync(`${dir}/done.json`, JSON.stringify({ finished_at: new Date().toISOString(), writer_exit: writerCode, loadavg_at_end: loadavg(), memory_mb_at_end: memory() }, null, 2))
const cleanup = directClient()
await cleanup.connect()
await cleanup.query('delete from stores where id = $1', [storeId]) // cascades to its orders, customers, members
await cleanup.end()
release()
console.log(`done ${runId}`)
