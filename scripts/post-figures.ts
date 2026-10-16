// Builds the write-up's chart specs from recorded results, so no number in a chart
// is typed by hand. Prints JSON to results/figures.json.
//   node scripts/post-figures.ts
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { pct, round } from '../lib/run.ts'

const read = (f: string) => JSON.parse(readFileSync(f, 'utf8'))
const runs = (root: string) => readdirSync(root).map((d) => join(root, d)).filter((d) => existsSync(join(d, 'summary.json')))
  .map((d) => ({ s: read(join(d, 'summary.json')), l: read(join(d, 'latencies.json')) }))
const lat = runs('results/latency')
const rates = [1, 10, 100]
const label = (r: number) => (r === 1 ? '1 write/s' : `${r} writes/s`)
const p50 = (shape: string, rate: number) => round(pct(lat.filter((r) => r.s.shape === shape && r.s.rate === rate).flatMap((r) => r.l.issue_to_update_ms), 50))
const writeP50 = (rate: number) => round(pct(lat.filter((r) => r.s.rate === rate).flatMap((r) => r.l.write_ms), 50))

const latencyChart = {
  type: 'line',
  title: 'Write issued to update applied, p50',
  unit: 'ms',
  caption: 'Neon Realtime beta, aws-eu-central-1, client about 35 ms away. Each point pools two runs: 600 writes at 1/s, 3,600 at 10/s, 12,000 at 100/s. The live result is held at 200 rows. Dashed: how long the write itself took to be acknowledged, all shapes.',
  x: rates.map(label),
  series: [
    { name: 'plain', data: rates.map((r) => p50('plain', r)) },
    { name: 'join', data: rates.map((r) => p50('join', r)) },
    { name: 'aggregate', data: rates.map((r) => p50('aggregate', r)) },
    { name: 'write acknowledged', data: rates.map(writeP50), dash: '6 5' },
  ],
}

// The join at 100 writes/s over time: SDK client vs the raw recorder.
const buckets = (timeline: [number, number][]) => {
  const out: number[][] = []
  for (const [t, v] of timeline) (out[Math.floor(t / 5000)] ??= []).push(v)
  return out.map((b) => round(pct(b, 50)))
}
const diag = runs('results/diagnostic')
const pick = (list: any[], label: string) => list.find((r) => r.s.shape === 'join' && r.s.rate === 100 && r.s.label === label)
const series: any[] = []
for (const [name, r] of [['SDK client, round A', pick(lat, 'A')], ['SDK client, round B', pick(lat, 'B')], ['raw recorder, run D1', pick(diag, 'D1')], ['raw recorder, round B', pick(diag, 'B')]] as const) {
  if (r) series.push({ name, data: buckets(r.l.timeline), ...(name.startsWith('raw') ? { dash: '6 5' } : {}) })
}
const backlogChart = {
  type: 'line',
  title: 'The 200-row join at 100 writes/s, p50 per 5 seconds',
  unit: 'ms',
  caption: 'Every write re-sends the whole 200-row result (about 27 KB). The raw recorder speaks the wire protocol without decoding rows; the SDK client decodes and applies each full result. Same project, same writer.',
  x: Array.from({ length: Math.max(...series.map((s) => s.data.length)) }, (_, i) => `${i * 5}s`),
  series,
}

writeFileSync('results/figures.json', JSON.stringify({ latencyChart, backlogChart }, null, 2))
console.log(JSON.stringify({ latencyChart, backlogChart }, null, 2))
