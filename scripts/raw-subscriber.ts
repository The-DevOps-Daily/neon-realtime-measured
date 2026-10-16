// Diagnostic recorder: the same job as bench-subscriber.ts, without the SDK. It
// speaks the wire protocol directly and never decodes rows, so it costs the client
// almost nothing per update. Comparing it with the SDK recorder separates Neon's
// delivery from the client's own processing. Not for apps: Neon asks apps to use
// the SDK, because the protocol may still change during the beta.
//   node scripts/raw-subscriber.ts --dir <run dir> --shape join --store <id> --user <id>
import { parseArgs } from 'node:util'
import WS from 'ws'
import { now } from '../lib/wiretap.ts'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { queryFor, type Shape } from '../lib/queries.ts'
import { Jsonl, readMeta } from '../lib/run.ts'
import { startStallMonitor } from '../lib/stalls.ts'

const { values } = parseArgs({ options: { dir: { type: 'string' }, shape: { type: 'string' }, store: { type: 'string' }, user: { type: 'string' } } })
const meta = readMeta(values.dir!)
const out = new Jsonl(`${values.dir}/subscriber.jsonl`, meta.run_id)
const stallMonitor = startStallMonitor()
const { db, pool } = createDb(1)
await pool.end()
const sealer = createSealer()
const query = queryFor(db, values.shape as Shape, Number(values.store), values.user!)
const seal = () => sealer.seal({ query: query as any })

const ws = new WS(realtimeUrl(), 'neon.realtime.v1')
let liveId: string | undefined
let pendingTxids: string[] = []
let live = false
let rows = 0
ws.on('message', async (data) => {
  const t = now()
  const text = String(data)
  const bytes = Buffer.byteLength(text)
  if (text.length > 4096) {
    const type = /"type"\s*:\s*"([a-z_]+)"/.exec(text.slice(0, 256))?.[1]
    out.write({ k: 'wire', t, bytes, type, large: true })
    return
  }
  const msg = JSON.parse(text)
  const summary: any = { k: 'wire', t, bytes, type: msg.type }
  if (msg.mvcc) summary.mvcc = msg.mvcc
  if (msg.txids) summary.txids = msg.txids
  if (msg.code) Object.assign(summary, { code: msg.code, message: msg.message })
  if (Array.isArray(msg.rows)) rows += msg.rows.length
  out.write(summary)
  if (msg.type === 'ready') {
    const sealed = await seal()
    out.write({ k: 'sealed', t: now(), expires_at: sealed.expiresAt })
    ws.send(JSON.stringify({ type: 'subscribe', request_id: '1', authorization: sealed.capability }))
  }
  if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong', token: msg.token }))
  if (msg.type === 'subscribed') liveId = msg.live_id
  if (msg.type === 'baseline_sync_start') rows = 0
  if (msg.type === 'baseline_sync_end') {
    out.write({ k: 'update', t, type: 'rows', n: null })
    if (!live) { live = true; out.write({ k: 'state', t, status: 'live' }); console.log('LIVE'); console.log('PINGED') }
  }
  if (msg.type === 'keyed_results') pendingTxids.push(...msg.txids)
  if (msg.type === 'commit' && pendingTxids.length) {
    out.write({ k: 'update', t, type: 'changes', txids: pendingTxids })
    pendingTxids = []
  }
})
// Renew every 50 s with a fresh capability for the same query.
const renew = setInterval(async () => {
  if (!liveId) return
  const sealed = await seal()
  ws.send(JSON.stringify({ type: 'renew', live_id: liveId, authorization: sealed.capability }))
  out.write({ k: 'renew', t: now(), ok: true })
}, 50_000)
process.on('SIGTERM', () => {
  clearInterval(renew)
  stallMonitor.stop()
  out.write({ k: 'stalls', list: stallMonitor.stalls })
  out.write({ k: 'end', t: now() })
  out.close()
  ws.terminate()
  process.exit(0)
})
