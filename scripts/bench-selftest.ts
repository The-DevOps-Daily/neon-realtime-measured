// Proves the latency scorer can fail. Copies recorded smoke runs, corrupts each copy
// in one specific way, scores it, and checks the scorer reports exactly that.
//   node scripts/bench-selftest.ts results/smoke
import { cpSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const src = process.argv[2] ?? 'results/smoke'
const pick = (shape: string) => readdirSync(src).find((d) => d.startsWith(`${shape}-10ps`))!
const lines = (f: string) => readFileSync(f, 'utf8').split('\n').filter(Boolean)
// Index of the nth SDK update line of a given type (wire frames are interleaved).
const nthUpdate = (l: string[], type: string, n: number) =>
  l.map((x, i) => [x, i] as const).filter(([x]) => x.includes('"k":"update"') && x.includes(`"type":"${type}"`))[n][1]

type Case = { name: string; shape: string; mutate: (dir: string) => void; check: (s: any) => boolean; expect: string }
const cases: Case[] = [
  { name: 'untouched copy', shape: 'plain', mutate: () => {}, check: (s) => s.missing === 0 && s.consistency_mismatches === 0, expect: 'clean' },
  { name: 'drop one incremental update', shape: 'plain', expect: 'missing 1',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); l.splice(nthUpdate(l, 'changes', 5), 1); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.missing === 1 },
  { name: 'wrong row in an incremental update', shape: 'plain', expect: 'mismatch',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); const i = nthUpdate(l, 'changes', 3); const o = JSON.parse(l[i]); o.seqs = o.seqs.map((n: number) => n + 1); l[i] = JSON.stringify(o); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.consistency_mismatches >= 1 },
  { name: 'one removal too many in an update', shape: 'plain', expect: 'mismatch',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); const i = nthUpdate(l, 'changes', 7); const o = JSON.parse(l[i]); o.removes += 1; l[i] = JSON.stringify(o); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.consistency_mismatches >= 1 },
  { name: 'a seed row missing from a full result', shape: 'join', expect: 'mismatch',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); const i = nthUpdate(l, 'rows', 12); const o = JSON.parse(l[i]); o.n -= 1; o.seqs.splice(o.seqs.indexOf(null), 1); l[i] = JSON.stringify(o); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.consistency_mismatches >= 1 },
  { name: 'one row missing from a full join result', shape: 'join', expect: 'mismatch',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); const idx = nthUpdate(l, 'rows', 10); const o = JSON.parse(l[idx]); o.seqs = o.seqs.filter((x: any) => x !== null).slice(1); l[idx] = JSON.stringify(o); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.consistency_mismatches >= 1 },
  { name: 'aggregate total off by one', shape: 'aggregate', expect: 'mismatch',
    mutate: (d) => { const f = join(d, 'subscriber.jsonl'); const l = lines(f); const idx = nthUpdate(l, 'rows', 10); const o = JSON.parse(l[idx]); o.rows[0].orders += 1; l[idx] = JSON.stringify(o); writeFileSync(f, l.join('\n') + '\n') },
    check: (s) => s.consistency_mismatches >= 1 },
  { name: 'a client stall over one write', shape: 'plain', expect: 'stalled 1',
    mutate: (d) => {
      const wf = join(d, 'writer.jsonl'), sf = join(d, 'subscriber.jsonl')
      const wl = lines(wf), run = JSON.parse(wl[0]).run
      const w = wl.map((x) => JSON.parse(x)).filter((x) => x.k === 'write')[20]
      // a 1 ms stall inside the window of write 21 only (writes are 100 ms apart at 10/s)
      writeFileSync(wf, readFileSync(wf, 'utf8') + JSON.stringify({ run, k: 'stalls', list: [{ start: w.t_issue + 1, end: w.t_issue + 2, ms: 1 }] }) + '\n')
      writeFileSync(sf, readFileSync(sf, 'utf8') + JSON.stringify({ run, k: 'stalls', list: [] }) + '\n')
    },
    check: (s) => s.writes_overlapping_client_stall === 1 && s.missing === 0 },
  { name: 'a line from another run', shape: 'plain', expect: 'refused',
    mutate: (d) => { const f = join(d, 'writer.jsonl'); writeFileSync(f, readFileSync(f, 'utf8') + JSON.stringify({ run: 'some-other-run', k: 'write' }) + '\n') },
    check: () => false },
]

let failures = 0
for (const c of cases) {
  const root = mkdtempSync(join(tmpdir(), 'selftest-'))
  const run = pick(c.shape)
  cpSync(join(src, run), join(root, run), { recursive: true })
  c.mutate(join(root, run))
  let got: string, ok: boolean
  try {
    execFileSync('node', ['scripts/bench-score.ts', root], { stdio: 'pipe' })
    const s = JSON.parse(readFileSync(join(root, run, 'summary.json'), 'utf8'))
    ok = c.check(s)
    got = `missing ${s.missing}, mismatches ${s.consistency_mismatches}, stalled ${s.writes_overlapping_client_stall}`
  } catch (err: any) {
    ok = c.expect === 'refused' && String(err.stderr).includes('refusing')
    got = 'scorer refused the files'
  }
  if (!ok) failures++
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${c.name.padEnd(40)} expected ${c.expect.padEnd(9)} got ${got}`)
}
console.log(failures ? `${failures} self-test case(s) failed` : `all ${cases.length} self-test cases behaved as expected`)
process.exit(failures ? 1 : 0)
