// Do live queries slow down writes on the primary? A writer runs at a fixed rate the
// whole time while 100 live queries (one per user, each touched by every write) are
// switched on and off in alternating 60 s blocks: off, on, off, on, off, on.
// Writes during each switch and in the 20 s after it are tagged as transitions and
// left out. The client's CPU clock follows its load (ondemand governor), which
// would make the busier "on" blocks look faster; --pin-clock runs a lowest-priority
// busy loop for the whole test so the clock stays the same in both conditions, and
// the CPU frequency is sampled every second either way.
// Recorded per block: client-side write latency, and pg_stat_statements' mean
// execution time for the write statement on the primary.
//   node scripts/primary-impact.ts [--rate 20] [--users 100]
import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { parseArgs } from 'node:util'
import { directClient, directPool } from '../lib/db.ts'
import { createStore, CUSTOMERS_PER_STORE, mulberry32, prefillOrders, STATUSES } from '../lib/seed.ts'
import { codeHash, pct, round } from '../lib/run.ts'

const { values } = parseArgs({ options: { rate: { type: 'string', default: '20' }, users: { type: 'string', default: '100' }, 'pin-clock': { type: 'boolean', default: false } } })
const RATE = Number(values.rate), USERS = Number(values.users), STORE = 80
const BLOCK_S = 60, SETTLE_S = 20
const plan = ['off', 'on', 'off', 'on', 'off', 'on'] as const
const now = () => performance.timeOrigin + performance.now()
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
const dir = `results/primary-impact/${stamp}`
mkdirSync(dir, { recursive: true })

const admin = directClient()
await admin.connect()
await createStore(admin, STORE, 'impact', Array.from({ length: USERS }, (_, i) => `impact-user-${i}`))
await admin.query('delete from orders where store_id = $1', [STORE])
await prefillOrders(admin, STORE, 200, STORE)
await admin.query('create extension if not exists pg_stat_statements')
// The write deletes the store's oldest order. Without an index on (store_id, id) the
// planner can switch mid-test to scanning the primary key past other stores' rows,
// which made the write's own execution time jump tenfold in our first run. Pin it.
await admin.query('create index if not exists orders_store_id_id_idx on orders (store_id, id)')
await admin.query('analyze orders')

const WRITE = `with ins as (insert into orders (store_id, customer_id, status, total_cents, seq) values ($1, $2, $3, $4, $5) returning id),
del as (delete from orders where id = (select id from orders where store_id = $1 order by id limit 1 for update skip locked) returning id)
select pg_current_xact_id()::text as txid`
const pool = directPool(8)
await Promise.all(Array.from({ length: 8 }, () => pool.connect())).then((cs) => cs.forEach((c) => c.release()))
const rand = mulberry32(STORE)
const writes: any[] = []
const eld = monitorEventLoopDelay({ resolution: 10 })
let block = -1, phase: 'transition' | 'settle' | 'measure' = 'settle'
let seq = 0, running = true
const writer = (async () => {
  const start = now()
  const inflight: Promise<void>[] = []
  while (running) {
    const target = start + (seq * 1000) / RATE
    const wait = target - now()
    if (wait > 0) await sleep(wait)
    const s = ++seq, b = block, p = phase, tIssue = now()
    inflight.push(pool.query(WRITE, [STORE, STORE * 1000 + 1 + Math.floor(rand() * CUSTOMERS_PER_STORE), STATUSES[Math.floor(rand() * 3)], 500 + Math.floor(rand() * 9500), s])
      .then(() => { writes.push({ seq: s, block: b, phase: p, ms: now() - tIssue }) }, (e) => { writes.push({ seq: s, block: b, phase: p, error: String(e.message) }) }))
  }
  await Promise.all(inflight)
})()

const freqFile = '/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq'
const freq: { block: number; phase: string; mhz: number }[] = []
const freqTimer = setInterval(() => {
  if (existsSync(freqFile)) freq.push({ block, phase, mhz: Math.round(Number(readFileSync(freqFile, 'utf8')) / 1000) })
}, 1000)
const burner = values['pin-clock'] ? spawn('nice', ['-n', '19', 'node', '-e', 'for(;;){}'], { stdio: 'ignore' }) : undefined

// 30 s of writes before the first block, left out, so block 0 is not the warm-up.
phase = 'transition'
await sleep(30_000)

const blocks: any[] = []
let subs: ChildProcess | undefined
for (let i = 0; i < plan.length; i++) {
  const cond = plan[i]
  phase = 'transition'
  if (cond === 'on') {
    subs = spawn('nice', ['-n', '10', 'node', 'scripts/impact-subscribers.ts', '--store', String(STORE), '--users', String(USERS), '--out', `${dir}/subscribers-block${i}.json`], { stdio: ['ignore', 'pipe', 'inherit'] })
    await new Promise<void>((resolve) => subs!.stdout!.on('data', (d) => String(d).includes('LIVE') && resolve()))
  } else if (subs) {
    subs.kill('SIGTERM')
    await new Promise((r) => subs!.on('exit', r))
    subs = undefined
  }
  block = i; phase = 'settle'
  await sleep(SETTLE_S * 1000)
  await admin.query('select pg_stat_statements_reset()')
  phase = 'measure'
  eld.reset(); eld.enable()
  await sleep(BLOCK_S * 1000)
  eld.disable()
  const { rows } = await admin.query(`select calls, mean_exec_time, stddev_exec_time from pg_stat_statements where query like 'with ins as (insert into orders%' order by calls desc limit 1`)
  const subFile = cond === 'on' ? `${dir}/subscribers-block${i}.json` : undefined
  blocks.push({ block: i, cond, pgss_calls: rows[0]?.calls, pgss_mean_exec_ms: round(rows[0]?.mean_exec_time, 3), writer_eld_p99_ms: round(eld.percentile(99) / 1e6), subFile })
  console.log(`block ${i} ${cond}: done`)
}
phase = 'transition'
if (subs) { subs.kill('SIGTERM'); await new Promise((r) => subs!.on('exit', r)) }
clearInterval(freqTimer)
burner?.kill('SIGKILL')
running = false
await writer
await pool.end()
await admin.end()

for (const b of blocks) {
  const ms = writes.filter((w) => w.block === b.block && w.phase === 'measure' && w.ms !== undefined).map((w) => w.ms)
  const mhz = freq.filter((f) => f.block === b.block && f.phase === 'measure').map((f) => f.mhz)
  Object.assign(b, { cpu_mhz_median: pct(mhz, 50), writes: ms.length, errors: writes.filter((w) => w.block === b.block && w.error).length,
    p50: round(pct(ms, 50)), p90: round(pct(ms, 90)), p99: round(pct(ms, 99)) })
  if (b.subFile) Object.assign(b, { subscriptions: JSON.parse(readFileSync(b.subFile, 'utf8')) })
  delete b.subFile
}

// Difference in median write latency (on - off), with a bootstrap 95% interval.
const pooled = (cond: string) => writes.filter((w) => w.phase === 'measure' && w.ms !== undefined && plan[w.block] === cond).map((w) => w.ms)
const on = pooled('on'), off = pooled('off')
const r = mulberry32(42)
const draw = (a: number[]) => Array.from({ length: a.length }, () => a[Math.floor(r() * a.length)])
const diffs = Array.from({ length: 2000 }, () => pct(draw(on), 50) - pct(draw(off), 50)).sort((a, b) => a - b)
const result = { rate: RATE, users: USERS, block_s: BLOCK_S, settle_s: SETTLE_S, pin_clock: values['pin-clock'], code_hash: codeHash(['scripts/primary-impact.ts', 'scripts/impact-subscribers.ts']),
  median_on_ms: round(pct(on, 50)), median_off_ms: round(pct(off, 50)), p99_on_ms: round(pct(on, 99)), p99_off_ms: round(pct(off, 99)),
  median_diff_ms: round(pct(on, 50) - pct(off, 50), 2), median_diff_95ci_ms: [round(diffs[50], 2), round(diffs[1949], 2)], blocks }
writeFileSync(`${dir}/writes.jsonl`, writes.map((w) => JSON.stringify(w)).join('\n') + '\n', { flag: 'wx' })
writeFileSync(`${dir}/result.json`, JSON.stringify(result, null, 2), { flag: 'wx' })
console.table(blocks.map(({ subscriptions, ...b }) => ({ ...b, updates: subscriptions?.updates })))
console.log(JSON.stringify({ ...result, blocks: undefined }, null, 2))
process.exit(0)
