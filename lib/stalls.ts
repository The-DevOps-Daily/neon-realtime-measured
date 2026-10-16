// Records every time this process's event loop could not run for a while. A 10 ms
// timer that fires more than 60 ms after the previous tick means the process was
// stalled (busy, starved of CPU, or paged out) for the gap. Timestamps taken during
// or right after a stall are late, so the scorer sets those writes apart.
import { now } from './wiretap.ts'

export type Stall = { start: number; end: number; ms: number }

export function startStallMonitor(thresholdMs = 60) {
  const stalls: Stall[] = []
  let last = now()
  const timer = setInterval(() => {
    const t = now()
    if (t - last > thresholdMs) stalls.push({ start: last, end: t, ms: Math.round(t - last) })
    last = t
  }, 10)
  timer.unref()
  return { stalls, stop: () => clearInterval(timer) }
}
