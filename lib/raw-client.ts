// A minimal raw Live SQL API client for the tamper tests. Neon asks apps to use the
// SDK because the protocol may still change; an attacker is not bound by that, so
// the tamper script speaks the wire format (from the SDK's own JSON schema) directly.
import WS from 'ws'

export type RawOutcome = {
  result: 'delivered' | 'subscribe_rejected' | 'subscription_error' | 'connection_error' | 'closed' | 'timeout'
  code?: string
  message?: string
  rows: Record<string, string | null>[] // rows of the first result
  changeRows: Record<string, string | null>[] // rows in incremental changes, any phase
  types: string[] // every server message type, in order
  renew?: { result: string; code?: string; rows: Record<string, string | null>[] }
}

const decodeCell = (cell: any): string | null =>
  cell === null || cell === undefined ? null : typeof cell === 'string' ? cell : cell.base64 ? Buffer.from(cell.base64, 'base64').toString('utf8') : JSON.stringify(cell)

export async function rawSubscribe(
  url: string,
  authorization: string,
  opts: {
    extra?: Record<string, unknown>
    waitMs?: number
    renewWith?: string
    // run once the first result is in (for example, a write that should arrive as a change)
    afterBaseline?: () => Promise<void>
    // run once the renew message is sent
    afterRenew?: () => Promise<void>
  } = {},
): Promise<RawOutcome> {
  const ws = new WS(url, 'neon.realtime.v1')
  const out: RawOutcome = { result: 'timeout', rows: [], changeRows: [], types: [] }
  let columns: string[] = []
  let liveId: string | undefined
  let phase: 'initial' | 'renew' = 'initial'
  return await new Promise<RawOutcome>((resolve) => {
    const finish = () => {
      clearTimeout(timer)
      try { ws.terminate() } catch {}
      resolve(out)
    }
    const timer = setTimeout(finish, opts.waitMs ?? 8000)
    ws.on('error', (err) => { out.result = 'connection_error'; out.message = String(err.message); finish() })
    ws.on('close', () => { if (out.result === 'timeout') out.result = 'closed'; finish() })
    ws.on('message', (data) => {
      const msg = JSON.parse(String(data))
      out.types.push(msg.type)
      if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong', token: msg.token }))
      if (msg.type === 'ready') ws.send(JSON.stringify({ type: 'subscribe', request_id: '1', authorization, ...(opts.extra ?? {}) }))
      if (msg.type === 'subscribed') { liveId = msg.live_id; columns = msg.columns.map((c: any) => c.name) }
      if (msg.type === 'baseline_sync_batch') {
        const rows = msg.rows.map((r: any) => Object.fromEntries(r.values.map((v: any, i: number) => [columns[i], decodeCell(v)])))
        if (phase === 'initial') out.rows.push(...rows)
        else out.renew!.rows.push(...rows)
      }
      if (msg.type === 'keyed_results') {
        for (const ch of msg.changes ?? []) {
          if (Array.isArray(ch.values)) out.changeRows.push(Object.fromEntries(ch.values.map((v: any, i: number) => [columns[i], decodeCell(v)])))
        }
      }
      if (msg.type === 'baseline_sync_end' && phase === 'initial') {
        out.result = 'delivered'
        clearTimeout(timer)
        const next = async () => {
          if (opts.afterBaseline) { await opts.afterBaseline(); await new Promise((r) => setTimeout(r, 3000)) }
          if (!opts.renewWith) return finish()
          phase = 'renew'
          out.renew = { result: 'timeout', rows: [] }
          ws.send(JSON.stringify({ type: 'renew', live_id: liveId, authorization: opts.renewWith }))
          if (opts.afterRenew) await opts.afterRenew()
          setTimeout(finish, 5000) // collect whatever the renewal brings
        }
        next()
      }
      if (msg.type === 'renewed' && out.renew) out.renew.result = 'renewed'
      if (msg.type === 'subscribe_rejected') { out.result = 'subscribe_rejected'; out.code = msg.code; out.message = msg.message; finish() }
      if (msg.type === 'subscription_error') {
        if (phase === 'renew') { out.renew!.result = 'subscription_error'; out.renew!.code = msg.code }
        else { out.result = 'subscription_error'; out.code = msg.code; out.message = msg.message; finish() }
      }
      if (msg.type === 'connection_error') {
        if (phase === 'renew') { out.renew!.result = 'connection_error'; out.renew!.code = msg.code }
        else { out.result = 'connection_error'; out.code = msg.code; out.message = msg.message }
        finish()
      }
    })
  })
}
