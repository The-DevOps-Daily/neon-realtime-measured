// Compares the Realtime traffic our recorders received with the project's public
// network transfer as Neon's consumption API reports it, hour by hour. Our count is
// WebSocket message payload only (no framing or TLS), so it understates the traffic.
//   node scripts/egress-meter.ts --from 2026-10-16T14:00:00Z --to 2026-10-16T19:00:00Z --org <org id>
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'
import { gunzipSync } from 'node:zlib'
import { neonApi, projectId } from '../lib/neon-api.ts'

const { values } = parseArgs({ options: { from: { type: 'string' }, to: { type: 'string' }, org: { type: 'string' } } })
const walk = (d: string): string[] => readdirSync(d).flatMap((f) => { const p = join(d, f); return statSync(p).isDirectory() ? walk(p) : [p] })
const text = (p: string) => (p.endsWith('.gz') ? gunzipSync(readFileSync(p)).toString('utf8') : readFileSync(p, 'utf8'))

const received: Record<string, number> = {}
const logs = walk('results').filter((p) => /subscriber\.jsonl(\.gz)?$/.test(p))
for (const f of logs) {
  for (const line of text(f).split('\n')) {
    if (!line.includes('"k":"wire"')) continue
    const x = JSON.parse(line)
    const hour = new Date(x.t).toISOString().slice(0, 13) + ':00:00Z'
    received[hour] = (received[hour] ?? 0) + x.bytes
  }
}
const q = new URLSearchParams({ from: values.from!, to: values.to!, granularity: 'hourly', org_id: values.org!, project_ids: projectId(), metrics: 'public_network_transfer_bytes' })
const api = await neonApi(`/consumption_history/v2/projects?${q}`)
const metered: Record<string, number> = {}
for (const c of api.projects[0].periods.flatMap((p: any) => p.consumption)) {
  metered[c.timeframe_start] = c.metrics.find((m: any) => m.metric_name === 'public_network_transfer_bytes')?.value ?? 0
}
const hours = [...new Set([...Object.keys(received), ...Object.keys(metered)])].sort()
const rows = hours.map((h) => ({ hour: h, realtime_payload_received_bytes: received[h] ?? 0, neon_public_network_transfer_bytes: metered[h] ?? null }))
console.table(rows)
const out = `results/egress-meter-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`
writeFileSync(out, JSON.stringify({ subscriber_logs: logs.length, note: 'payload bytes of WebSocket messages received by the bench recorders only (results/**/subscriber.jsonl), excluding framing and TLS. The explore, CLI, reconnect, scoping, board, primary-impact and scale-to-zero subscriptions keep no wire log and are not counted, so an hour with 0 here is unrecorded, not idle', rows, api }, null, 2), { flag: 'wx' })
console.log(`wrote ${out}`)
