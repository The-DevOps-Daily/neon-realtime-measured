// Stage 2 of a latency run: open-loop writes at a fixed rate.
// Each transaction inserts one order and deletes the store's oldest order, so the
// live result stays at a constant size (the window) for the whole run.
//   node scripts/bench-writer.ts --dir <run dir> --store <id> --rate 10 --duration 180
import { parseArgs } from 'node:util'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { directPool } from '../lib/db.ts'
import { Jsonl, readMeta } from '../lib/run.ts'
import { CUSTOMERS_PER_STORE, mulberry32, STATUSES } from '../lib/seed.ts'
import { now } from '../lib/wiretap.ts'
import { startStallMonitor } from '../lib/stalls.ts'

const { values } = parseArgs({
  options: {
    dir: { type: 'string' }, store: { type: 'string' }, rate: { type: 'string' },
    duration: { type: 'string' }, pool: { type: 'string', default: '12' },
  },
})
const dir = values.dir!
const meta = readMeta(dir)
const storeId = Number(values.store)
const rate = Number(values.rate)
const total = Math.round(rate * Number(values.duration))
const out = new Jsonl(`${dir}/writer.jsonl`, meta.run_id)
const pool = directPool(Number(values.pool))
const rand = mulberry32(meta.seed)
const eld = monitorEventLoopDelay({ resolution: 10 })
eld.enable()
const stallMonitor = startStallMonitor()

const WRITE = `
with ins as (
  insert into orders (store_id, customer_id, status, total_cents, seq)
  values ($1, $2, $3, $4, $5) returning id
), del as (
  delete from orders where id = (
    select id from orders where store_id = $1 order by id limit 1 for update skip locked
  ) returning id
)
select (select id from ins) as id, (select id from del) as deleted_id,
       pg_current_xact_id()::text as txid, pg_current_wal_insert_lsn()::text as lsn`

// Warm every pooled connection, and record the plain round trip from this machine.
const warm = await Promise.all(Array.from({ length: Number(values.pool) }, () => pool.connect()))
warm.forEach((c) => c.release())
const rtt: number[] = []
for (let i = 0; i < 30; i++) {
  const t0 = now()
  await pool.query('select 1')
  rtt.push(now() - t0)
}
out.write({ k: 'rtt', what: 'select 1 over the direct connection', samples: rtt })

const start = now() + 200
let inflight = 0
let errors = 0
const done: Promise<void>[] = []
for (let seq = 1; seq <= total; seq++) {
  const target = start + ((seq - 1) * 1000) / rate
  const wait = target - now()
  if (wait > 0) await new Promise((r) => setTimeout(r, wait))
  const tIssue = now()
  const waiting = pool.waitingCount
  inflight++
  const params = [storeId, storeId * 1000 + 1 + Math.floor(rand() * CUSTOMERS_PER_STORE),
    STATUSES[Math.floor(rand() * 3)], 500 + Math.floor(rand() * 9500), seq]
  done.push(
    pool.query(WRITE, params).then(
      ({ rows: [r] }) => {
        out.write({ k: 'write', seq, txid: r.txid, lsn: r.lsn, id: r.id, deleted: r.deleted_id,
          t_target: target, t_issue: tIssue, t_ack: now(), queued: waiting })
      },
      (err) => {
        errors++
        out.write({ k: 'error', seq, t_issue: tIssue, t_err: now(), message: String(err.message) })
      },
    ).finally(() => inflight--),
  )
}
await Promise.all(done)
eld.disable()
stallMonitor.stop()
out.write({ k: 'stalls', list: stallMonitor.stalls })
out.write({ k: 'end', writes: total, errors, eld_p99_ms: eld.percentile(99) / 1e6, eld_max_ms: eld.max / 1e6 })
out.close()
await pool.end()
console.log(`writer: ${total} writes, ${errors} errors`)
