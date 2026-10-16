// Every server-side connection or subscription error any recorder saw, with the
// subscription time each results tree covers.
//   node scripts/session-events.ts results/latency results/latency-v1
import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { gunzipSync } from 'node:zlib'

const walk = (d: string): string[] => readdirSync(d).flatMap((f) => { const p = join(d, f); return statSync(p).isDirectory() ? walk(p) : [p] })
const text = (p: string) => (p.endsWith('.gz') ? gunzipSync(readFileSync(p)).toString('utf8') : readFileSync(p, 'utf8'))
const report: any = {}
for (const root of process.argv.slice(2)) {
  let ms = 0
  const events: any[] = []
  for (const f of walk(root).filter((p) => /subscriber\.jsonl(\.gz)?$/.test(p))) {
    const lines = text(f).split('\n').filter(Boolean).map((l) => JSON.parse(l))
    ms += lines.at(-1).t - lines[0].t
    for (const x of lines) if (x.type === 'connection_error' || x.type === 'subscription_error') events.push({ at: new Date(x.t).toISOString(), run: x.run, type: x.type, code: x.code })
  }
  events.sort((a, b) => a.at.localeCompare(b.at))
  report[root] = { subscription_minutes: Math.round(ms / 600) / 100, events }
  console.log(`${root}: ${events.length} events in ${report[root].subscription_minutes} subscription minutes`)
  console.table(events.map((e) => ({ at: e.at.slice(11, 23), code: e.code, run: e.run })))
}
writeFileSync('results/session-events.json', JSON.stringify(report, null, 2))
