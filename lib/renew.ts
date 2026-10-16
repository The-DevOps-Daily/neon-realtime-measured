// Renew a sealed query 10 s before it expires, retrying a failed fetch after 1 s.
// The same timing as the React and TanStack DB bindings (from Neon's subscribing guide).
import type { SealedLiveQuery } from '@neon/realtime/client'

export function renewBeforeExpiry(
  sub: { renew: (q: SealedLiveQuery<any>) => Promise<void> },
  first: SealedLiveQuery<any>,
  fetchSealed: () => Promise<SealedLiveQuery<any>>,
  onEvent: (e: { ok: boolean; message?: string }) => void = () => {},
) {
  let timer: NodeJS.Timeout | undefined
  let stopped = false
  const schedule = (ms: number) => { clearTimeout(timer); if (!stopped) timer = setTimeout(renew, ms) }
  const beforeExpiry = (q: SealedLiveQuery<any>) => schedule(Math.max(0, q.expiresAt - Date.now() - 10_000))
  async function renew() {
    let next: SealedLiveQuery<any>
    try {
      next = await fetchSealed()
    } catch (err: any) {
      onEvent({ ok: false, message: String(err?.message ?? err) })
      return schedule(1000)
    }
    beforeExpiry(next)
    sub.renew(next).then(() => onEvent({ ok: true }), (err) => { onEvent({ ok: false, message: `renew rejected: ${err?.message}` }); stop() })
  }
  function stop() { stopped = true; clearTimeout(timer) }
  beforeExpiry(first)
  return stop
}
