// Stage 1 of a latency run: one browser-style Realtime client subscribed to one
// sealed query, recording every SDK update and every wire frame with a timestamp.
// Prints LIVE once the first result is in, and stops on SIGTERM.
//   node scripts/bench-subscriber.ts --dir <run dir> --shape plain --store <id> --user <id>
import { parseArgs } from 'node:util'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { createRealtimeClient } from '@neon/realtime/client'
import { installWireTap, now, tap, wsPing } from '../lib/wiretap.ts'
import { frameSummary } from '../lib/frames.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { queryFor, type Shape } from '../lib/queries.ts'
import { Jsonl, readMeta } from '../lib/run.ts'
import { startStallMonitor } from '../lib/stalls.ts'

const { values } = parseArgs({
  options: { dir: { type: 'string' }, shape: { type: 'string' }, store: { type: 'string' }, user: { type: 'string' } },
})
const dir = values.dir!
const meta = readMeta(dir)
const out = new Jsonl(`${dir}/subscriber.jsonl`, meta.run_id)
const eld = monitorEventLoopDelay({ resolution: 10 })
eld.enable()
const stallMonitor = startStallMonitor()

installWireTap()
tap.onFrame = (f) => out.write({ k: 'wire', t: f.t, bytes: f.bytes, ...frameSummary(f.msg) })
tap.onSend = (f) => out.write({ k: 'sent', t: f.t, bytes: f.bytes, type: f.msg.type })

const { db, pool } = createDb(1)
const sealer = createSealer()
const query = queryFor(db, values.shape as Shape, Number(values.store), values.user!)
const seal = () => sealer.seal({ query: query as any })

const client = createRealtimeClient({ url: realtimeUrl() })
let sealed = await seal()
out.write({ k: 'sealed', t: now(), expires_at: sealed.expiresAt, fingerprint: sealed.queryFingerprint.slice(0, 12) })
const sub = client.subscribe(sealed, { materialize: false })

sub.onStateChange((s: any) => {
  out.write({ k: 'state', t: now(), status: s.status, error: s.error ? { code: s.error.code, message: s.error.message } : undefined })
  if (s.status === 'live') console.log('LIVE')
})
sub.onUpdate((u: any) => {
  const t = now()
  if (u.type === 'changes') {
    const upserts = u.changes.filter((c: any) => c.type === 'upsert')
    out.write({ k: 'update', t, type: 'changes', txids: u.batch.txids, upserts: upserts.length,
      removes: u.changes.length - upserts.length, seqs: upserts.map((c: any) => c.row.seq ?? null),
      rows: values.shape === 'aggregate' ? upserts.map((c: any) => c.row) : undefined })
  } else {
    out.write({ k: 'update', t, type: 'rows', n: u.rows.length,
      seqs: values.shape === 'aggregate' ? undefined : u.rows.map((r: any) => r.row.seq ?? null),
      rows: values.shape === 'aggregate' ? u.rows.map((r: any) => r.row) : undefined })
  }
})

// Renew the sealed query 10 s before it expires, as the React binding does.
let renewTimer: NodeJS.Timeout
const scheduleRenew = () => {
  renewTimer = setTimeout(async () => {
    try {
      sealed = await seal()
      await sub.renew(sealed)
      out.write({ k: 'renew', t: now(), ok: true, expires_at: sealed.expiresAt })
    } catch (err: any) {
      out.write({ k: 'renew', t: now(), ok: false, message: String(err?.message ?? err) })
    }
    scheduleRenew()
  }, Math.max(0, sealed.expiresAt - Date.now() - 10_000))
}
scheduleRenew()

// After the first result: WebSocket ping/pong round trips on the same connection.
const live = new Promise<void>((resolve) => sub.onStateChange((s: any) => s.status === 'live' && resolve()))
await live
out.write({ k: 'ws_rtt', samples: await wsPing(20, 50) })
console.log('PINGED')

process.on('SIGTERM', () => {
  clearTimeout(renewTimer)
  eld.disable()
  stallMonitor.stop()
  out.write({ k: 'stalls', list: stallMonitor.stalls })
  out.write({ k: 'end', t: now(), eld_p99_ms: eld.percentile(99) / 1e6, eld_max_ms: eld.max / 1e6 })
  sub.unsubscribe()
  client.close()
  out.close()
  pool.end().finally(() => process.exit(0))
})
