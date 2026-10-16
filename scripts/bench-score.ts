// Stage 3: score latency runs. Joins each write (by its transaction id) to the first
// update that made it visible, checks every update's rows against the writes, and
// writes summary.json next to the raw files plus a combined table.
//   node scripts/bench-score.ts results/latency [--out results/latency/summary.json]
import { existsSync, readdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'
import { pct, readJsonl, readMeta, round } from '../lib/run.ts'

const { values, positionals } = parseArgs({ options: { out: { type: 'string' } }, allowPositionals: true })
const root = positionals[0] ?? 'results/latency'
const dirs = readdirSync(root).map((d) => join(root, d)).filter((d) => existsSync(join(d, 'meta.json'))).sort()

// Postgres visibility rule for a snapshot: committed before xmin, or below xmax and not in progress.
const visibleIn = (txid: bigint, m: { xmin: string; xmax: string; xip: string[] }) =>
  txid < BigInt(m.xmin) || (txid < BigInt(m.xmax) && !m.xip.includes(String(txid)))

const summaries: any[] = []
const hashes = new Set<string>()
for (const dir of dirs) {
  const meta = readMeta(dir)
  if (!existsSync(join(dir, 'done.json'))) {
    console.log(`skip ${meta.run_id}: no done.json (interrupted run)`)
    continue
  }
  hashes.add(meta.code_hash)
  const w = readJsonl(join(dir, 'writer.jsonl'), meta.run_id)
  const s = readJsonl(join(dir, 'subscriber.jsonl'), meta.run_id)
  const writes = w.filter((x) => x.k === 'write')
  const errors = w.filter((x) => x.k === 'error')
  const byTxid = new Map<string, any>(writes.map((x) => [x.txid, x]))
  const updates = s.filter((x) => x.k === 'update')
  const wire = s.filter((x) => x.k === 'wire')
  const baselines = wire.filter((x) => x.type === 'baseline_sync_start')

  const visibleAt = new Map<string, number>()
  const viaReset = new Set<string>()
  let mismatches = 0
  const mismatchExamples: string[] = []
  const visibleSet = new Set<string>()
  const note = (msg: string) => { mismatches++; if (mismatchExamples.length < 5) mismatchExamples.push(msg) }

  for (const u of updates) {
    let newly: string[] = []
    if (u.type === 'changes') {
      for (const txid of u.txids) if (!visibleAt.has(txid) && byTxid.has(txid)) newly.push(txid)
      // every upserted row must be one of this batch's writes
      if (meta.shape !== 'aggregate' && u.seqs) {
        const expected = u.txids.map((t: string) => byTxid.get(t)?.seq).filter((x: any) => x != null).sort((a: number, b: number) => a - b)
        const got = [...u.seqs].filter((x) => x != null).sort((a: number, b: number) => a - b)
        if (JSON.stringify(expected) !== JSON.stringify(got)) note(`changes txids ${u.txids} upserted seqs ${got} expected ${expected}`)
        // and it must remove exactly one row for each of its writes that deleted one
        if (u.removes !== undefined) {
          const removals = u.txids.filter((t: string) => byTxid.get(t)?.deleted != null).length
          if (u.removes !== removals) note(`changes txids ${u.txids} removed ${u.removes} rows, expected ${removals}`)
        }
      }
    } else {
      const b = baselines.filter((x) => x.t <= u.t).at(-1)
      if (!b) { note('rows update with no baseline_sync_start before it'); continue }
      for (const [txid] of byTxid) if (!visibleAt.has(txid) && visibleIn(BigInt(txid), b.mvcc)) { newly.push(txid); viaReset.add(txid) }
    }
    for (const t of newly) { visibleAt.set(t, u.t); visibleSet.add(t) }

    // The full result must match exactly what the visible writes imply.
    if (u.type === 'rows' && meta.shape !== 'aggregate' && u.seqs) {
      const vis = [...visibleSet].map((t) => byTxid.get(t))
      const deleted = new Set(vis.map((x) => x.deleted).filter((x) => x != null))
      const expected = vis.filter((x) => !deleted.has(x.id)).map((x) => x.seq).sort((a, b) => a - b)
      const got = u.seqs.filter((x: any) => x != null).sort((a: number, b: number) => a - b)
      if (JSON.stringify(expected) !== JSON.stringify(got)) note(`rows update at ${u.t}: ${got.length} seqs, expected ${expected.length}`)
      // and the whole result, seed rows included, must have the size the window implies
      if (typeof u.n === 'number') {
        const size = meta.window + vis.filter((x) => x.deleted == null).length
        if (u.n !== size) note(`rows update at ${u.t}: ${u.n} rows, expected ${size}`)
      }
    }
    if (u.rows && meta.shape === 'aggregate' && u.type === 'rows') {
      const total = u.rows.reduce((a: number, r: any) => a + r.orders, 0)
      const vis = [...visibleSet].map((t) => byTxid.get(t))
      const expected = meta.window + vis.filter((x) => x.deleted == null).length
      if (total !== expected) note(`aggregate total ${total}, expected ${expected}`)
    }
  }

  const delivered = writes.filter((x) => visibleAt.has(x.txid))

  // Stalls of the recorder processes themselves (this machine, not Neon). A write
  // whose window from issue to update overlaps one has a late timestamp somewhere.
  const wStalls = w.find((x) => x.k === 'stalls')?.list ?? []
  const sStalls = s.find((x) => x.k === 'stalls')?.list ?? []
  const stallData = w.some((x) => x.k === 'stalls') && s.some((x) => x.k === 'stalls')
  const stalls = [...wStalls, ...sStalls]
  const overlapsStall = (a: number, b: number) => stalls.some((st: any) => st.end > a && st.start < b)
  const clean = delivered.filter((x) => !overlapsStall(x.t_issue, visibleAt.get(x.txid)!))
  const latClean = clean.map((x) => visibleAt.get(x.txid)! - x.t_issue)
  const ackClean = clean.map((x) => visibleAt.get(x.txid)! - x.t_ack)
  const stallSummary = (list: any[]) => ({ count: list.length, total_ms: list.reduce((a, x) => a + x.ms, 0), max_ms: list.reduce((a, x) => Math.max(a, x.ms), 0) })
  const lat = delivered.map((x) => visibleAt.get(x.txid)! - x.t_issue)
  const afterAck = delivered.map((x) => visibleAt.get(x.txid)! - x.t_ack)
  const writeLat = writes.map((x) => x.t_ack - x.t_issue)
  const firstIssue = Math.min(...writes.map((x) => x.t_issue))
  const lastVisible = Math.max(...delivered.map((x) => visibleAt.get(x.txid)!))
  const phaseWire = wire.filter((x) => x.t >= firstIssue)
  const bytes = phaseWire.reduce((a, x) => a + x.bytes, 0)
  const rtt = w.find((x) => x.k === 'rtt')?.samples ?? []
  const wsRtt = s.find((x) => x.k === 'ws_rtt')?.samples ?? []
  const sealedAt = s.find((x) => x.k === 'sealed')?.t
  const firstRows = updates.find((x) => x.type === 'rows')?.t
  const subEnd = s.find((x) => x.k === 'end')
  const wEnd = w.find((x) => x.k === 'end')
  const sum = {
    run_id: meta.run_id, code_hash: meta.code_hash, shape: meta.shape, rate: meta.rate, window: meta.window,
    label: meta.label, duration_s: meta.duration,
    writes: writes.length, errors: errors.length, delivered: delivered.length,
    missing: writes.length - delivered.length, consistency_mismatches: mismatches, mismatch_examples: mismatchExamples,
    delivered_via_reset: viaReset.size,
    updates: { changes: updates.filter((x) => x.type === 'changes').length, rows: updates.filter((x) => x.type === 'rows').length },
    resets: phaseWire.filter((x) => x.type === 'reset_required').length,
    latency_ms: { p50: round(pct(lat, 50)), p90: round(pct(lat, 90)), p99: round(pct(lat, 99)), max: round(Math.max(...lat)) },
    after_commit_ack_ms: { p50: round(pct(afterAck, 50)), p99: round(pct(afterAck, 99)) },
    write_ms: { p50: round(pct(writeLat, 50)), p99: round(pct(writeLat, 99)) },
    select1_rtt_ms_p50: round(pct(rtt, 50)), ws_rtt_ms_p50: round(pct(wsRtt, 50)),
    est_server_side_ms_p50: round(pct(lat, 50) - (pct(rtt, 50) + pct(wsRtt, 50)) / 2),
    stall_data: stallData,
    client_stalls: { writer: stallSummary(wStalls), subscriber: stallSummary(sStalls) },
    writes_overlapping_client_stall: stallData ? delivered.length - clean.length : null,
    latency_clean_ms: stallData ? { p50: round(pct(latClean, 50)), p90: round(pct(latClean, 90)), p99: round(pct(latClean, 99)), max: round(Math.max(...latClean)) } : null,
    after_commit_ack_clean_ms: stallData ? { p50: round(pct(ackClean, 50)), p99: round(pct(ackClean, 99)) } : null,
    wire_bytes_during_writes: bytes, wire_bytes_per_write: Math.round(bytes / writes.length),
    wire_mb_per_s: round(bytes / 1e6 / ((lastVisible - firstIssue) / 1000), 3),
    time_to_first_result_ms: sealedAt && firstRows ? round(firstRows - sealedAt) : null,
    writes_queued_in_pool: writes.filter((x) => x.queued > 0).length,
    eld_p99_ms: { subscriber: round(subEnd?.eld_p99_ms), writer: round(wEnd?.eld_p99_ms) },
    loadavg_at_start: meta.loadavg_at_start?.map((x: number) => round(x, 2)),
  }
  // Server-side errors the SDK recovered from, and episodes of slow updates, split by
  // where the time went: the write's own commit ack, or after it.
  const serverErrors = wire.filter((x) => x.type === 'connection_error' || x.type === 'subscription_error')
    .map((x) => ({ t: x.t, type: x.type, code: x.code, message: x.message }))
  const slow = delivered.map((x) => ({ t: x.t_issue, lat: visibleAt.get(x.txid)! - x.t_issue, ack: x.t_ack - x.t_issue, vis: visibleAt.get(x.txid)! }))
    .filter((x) => x.lat > 300).sort((a, b) => a.t - b.t)
  const episodes: { start: number; writes: number; worst_ms: number; worst_ack_ms: number }[] = []
  for (const x of slow) {
    const e = episodes.at(-1)
    if (e && x.t - (e as any)._last < 5000) { e.writes++; e.worst_ms = Math.max(e.worst_ms, x.lat); e.worst_ack_ms = Math.max(e.worst_ack_ms, x.ack); (e as any)._last = x.t; (e as any)._end = Math.max((e as any)._end, x.vis) }
    else episodes.push({ start: x.t, writes: 1, worst_ms: x.lat, worst_ack_ms: x.ack, _last: x.t, _end: x.vis } as any)
  }
  for (const e of episodes) {
    delete (e as any)._last
    e.worst_ms = round(e.worst_ms)!
    e.worst_ack_ms = round(e.worst_ack_ms)!
    // Blame this machine only when its own stalls inside the episode add up to at
    // least half of the worst write's excess over the run's median.
    const end = (e as any)._end
    const stalledMs = stalls.filter((st: any) => st.end > e.start && st.start < end).reduce((a: number, st: any) => a + st.ms, 0)
    ;(e as any).client_stall_ms = stalledMs
    ;(e as any).cause = stalledMs >= 0.5 * (e.worst_ms - pct(lat, 50)) ? 'client stall (this machine)'
      : serverErrors.some((x) => x.t >= e.start - 2000 && x.t <= e.start + 5000) ? 'server ended the live query session'
      : e.worst_ack_ms > 300 ? 'write path (slow commit ack)' : 'update path'
    delete (e as any)._end
  }
  Object.assign(sum, { server_errors: serverErrors, slow_episodes: episodes })
  writeFileSync(join(dir, 'summary.json'), JSON.stringify(sum, null, 2))
  writeFileSync(join(dir, 'latencies.json'), JSON.stringify({ run_id: meta.run_id, issue_to_update_ms: lat.map((x) => round(x, 2)), ack_to_update_ms: afterAck.map((x) => round(x, 2)), write_ms: writeLat.map((x) => round(x, 2)),
    issue_to_update_clean_ms: latClean.map((x) => round(x, 2)), ack_to_update_clean_ms: ackClean.map((x) => round(x, 2)),
    timeline: delivered.map((x) => [round(x.t_issue - firstIssue, 0), round(visibleAt.get(x.txid)! - x.t_issue, 1)]) }))
  summaries.push(sum)
}
if (hashes.size > 1) throw new Error(`runs from ${hashes.size} code versions (${[...hashes]}): score them separately`)
writeFileSync(values.out ?? join(root, 'summary.json'), JSON.stringify(summaries, null, 2))
console.table(summaries.map((x) => ({
  run: `${x.shape} ${x.rate}/s ${x.label}`, writes: x.writes, missing: x.missing, bad: x.consistency_mismatches,
  resets: x.resets, p50: x.latency_ms.p50, p90: x.latency_ms.p90, p99: x.latency_ms.p99, max: x.latency_ms.max,
  'write p50': x.write_ms.p50, 'B/write': x.wire_bytes_per_write, load: x.loadavg_at_start?.[0],
  stalled: x.writes_overlapping_client_stall, 'clean p99': x.latency_clean_ms?.p99,
})))
