// Run directories: every recorded run gets a meta.json stamped with a hash of the
// code that produced it, and every JSONL line carries the run id. The scorers
// refuse files that are unstamped, mixed, or from a different code hash.
import { createHash } from 'node:crypto'
import { closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, statSync, unlinkSync, writeSync } from 'node:fs'
import { gunzipSync } from 'node:zlib'
import { join } from 'node:path'

// What a run's numbers depend on: the shared library, the schema, the lockfile and
// the scripts that recorded it. Scorers are left out, so fixing a scorer does not
// orphan recorded runs; the scorer re-reads the raw files every time.
const SHARED = ['lib', 'db', 'package-lock.json']

function filesUnder(path: string): string[] {
  const st = statSync(path)
  if (st.isFile()) return [path]
  return readdirSync(path).sort().flatMap((f) => filesUnder(join(path, f)))
}

export function codeHash(recorders: string[]): string {
  const h = createHash('sha256')
  for (const file of [...SHARED, ...recorders].flatMap(filesUnder)) {
    h.update(file)
    h.update(readFileSync(file))
  }
  return h.digest('hex').slice(0, 16)
}

// One writer per file: the 'wx' flag fails if the file already exists.
export class Jsonl {
  private fd: number
  private runId: string
  constructor(path: string, runId: string) {
    this.runId = runId
    this.fd = openSync(path, 'wx')
  }
  write(obj: Record<string, unknown>) {
    writeSync(this.fd, JSON.stringify({ run: this.runId, ...obj }) + '\n')
  }
  close() {
    closeSync(this.fd)
  }
}

// Recorded runs are committed gzipped (file.jsonl.gz); either form reads the same.
export function readJsonl(path: string, runId: string): any[] {
  const text = existsSync(path) ? readFileSync(path, 'utf8') : gunzipSync(readFileSync(`${path}.gz`)).toString('utf8')
  const lines = text.split('\n').filter(Boolean).map((l) => JSON.parse(l))
  const foreign = lines.filter((l) => l.run !== runId)
  if (foreign.length) throw new Error(`${path}: ${foreign.length} lines from another run (${foreign[0].run}); refusing`)
  return lines
}

export function readMeta(dir: string) {
  const meta = JSON.parse(readFileSync(join(dir, 'meta.json'), 'utf8'))
  if (!meta.run_id || !meta.code_hash) throw new Error(`${dir}: meta.json is not stamped; refusing`)
  return meta
}

// A directory-wide lock so two harnesses never record into the same results tree.
export function acquireLock(dir: string) {
  mkdirSync(dir, { recursive: true })
  const path = join(dir, '.lock')
  const fd = openSync(path, 'wx')
  writeSync(fd, String(process.pid))
  closeSync(fd)
  const release = () => {
    try {
      unlinkSync(path)
    } catch {}
  }
  process.on('exit', release)
  return release
}

// Nearest-rank percentile on a sorted copy.
export function pct(values: number[], p: number): number {
  if (!values.length) return NaN
  const s = [...values].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))]
}

export const round = (x: number, d = 1) => (Number.isFinite(x) ? Math.round(x * 10 ** d) / 10 ** d : null)
