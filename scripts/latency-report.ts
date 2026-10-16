// Stage 4: pool the scored runs per shape and rate (both rounds), and write the
// tables and chart data the write-up quotes. Reads only files bench-score.ts wrote.
//   node scripts/latency-report.ts results/latency
import { readdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { pct, round } from '../lib/run.ts'

const root = process.argv[2] ?? 'results/latency'
const runs = readdirSync(root).map((d) => join(root, d)).filter((d) => existsSync(join(d, 'summary.json')))
  .map((d) => ({ s: JSON.parse(readFileSync(join(d, 'summary.json'), 'utf8')), l: JSON.parse(readFileSync(join(d, 'latencies.json'), 'utf8')) }))
const hashes = new Set(runs.map((r) => r.s.code_hash))
if (hashes.size !== 1) throw new Error(`expected one code hash, found ${[...hashes]}`)

const shapes = ['plain', 'join', 'aggregate'], rates = [1, 10, 100]
const rows: any[] = []
for (const rate of rates) for (const shape of shapes) {
  const rs = runs.filter((r) => r.s.shape === shape && r.s.rate === rate)
  if (!rs.length) continue
  const lat = rs.flatMap((r) => r.l.issue_to_update_ms), ack = rs.flatMap((r) => r.l.ack_to_update_ms), wr = rs.flatMap((r) => r.l.write_ms)
  const latC = rs.flatMap((r) => r.l.issue_to_update_clean_ms ?? []), ackC = rs.flatMap((r) => r.l.ack_to_update_clean_ms ?? [])
  const bytes = rs.reduce((a, r) => a + r.s.wire_bytes_during_writes, 0), writes = rs.reduce((a, r) => a + r.s.writes, 0)
  rows.push({
    shape, rate, runs: rs.map((r) => r.s.label).sort().join('+'), writes, missing: rs.reduce((a, r) => a + r.s.missing, 0),
    mismatches: rs.reduce((a, r) => a + r.s.consistency_mismatches, 0),
    p50: round(pct(lat, 50)), p90: round(pct(lat, 90)), p99: round(pct(lat, 99)), max: round(Math.max(...lat)),
    ack_p50: round(pct(ack, 50)), ack_p99: round(pct(ack, 99)), write_p50: round(pct(wr, 50)), write_p99: round(pct(wr, 99)),
    stalled_writes: rs.reduce((a, r) => a + (r.s.writes_overlapping_client_stall ?? 0), 0),
    clean_p50: round(pct(latC, 50)), clean_p90: round(pct(latC, 90)), clean_p99: round(pct(latC, 99)), clean_max: round(Math.max(...latC)),
    clean_ack_p50: round(pct(ackC, 50)), clean_ack_p99: round(pct(ackC, 99)),
    client_stalls_max_ms: Math.max(...rs.map((r) => Math.max(r.s.client_stalls?.writer.max_ms ?? 0, r.s.client_stalls?.subscriber.max_ms ?? 0))),
    per_round_p50: rs.map((r) => `${r.s.label}:${r.s.latency_ms.p50}`).sort().join(' '),
    per_round_p99: rs.map((r) => `${r.s.label}:${r.s.latency_ms.p99}`).sort().join(' '),
    bytes_per_write: Math.round(bytes / writes),
    resets: rs.reduce((a, r) => a + r.s.resets, 0),
    session_losses: rs.reduce((a, r) => a + r.s.server_errors.length, 0),
    select1_rtt_p50: round(pct(rs.map((r) => r.s.select1_rtt_ms_p50), 50)), ws_rtt_p50: round(pct(rs.map((r) => r.s.ws_rtt_ms_p50), 50)),
  })
}
const episodes = runs.flatMap((r) => r.s.slow_episodes.map((e: any) => ({ run: `${r.s.shape} ${r.s.rate}/s ${r.s.label}`, at: new Date(e.start).toISOString().slice(11, 19), ...e, start: undefined })))
const losses = runs.flatMap((r) => r.s.server_errors.map((e: any) => ({ run: `${r.s.shape} ${r.s.rate}/s ${r.s.label}`, at: new Date(e.t).toISOString().slice(11, 23), code: e.code })))
  .sort((a, b) => a.at.localeCompare(b.at))

console.table(rows.map(({ per_round_p50, per_round_p99, ...r }) => r))
console.table(episodes)
console.table(losses)
writeFileSync(join(root, 'report.json'), JSON.stringify({ code_hash: [...hashes][0], pooled: rows, episodes, session_losses: losses }, null, 2))
