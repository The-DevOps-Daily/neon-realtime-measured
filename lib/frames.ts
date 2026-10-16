// The small, row-free summary of a wire frame that the runs record.
export function frameSummary(msg: any) {
  const out: Record<string, unknown> = { type: msg.type }
  for (const key of ['live_id', 'publication_id', 'index', 'epoch', 'baseline_sync_attempt', 'batch_count', 'body_count', 'code', 'retry_after_ms']) {
    if (msg[key] !== undefined) out[key] = msg[key]
  }
  if (msg.txids) out.txids = msg.txids
  if (msg.frontier) out.frontier = msg.frontier
  if (msg.mvcc) out.mvcc = msg.mvcc
  if (Array.isArray(msg.rows)) out.rows = msg.rows.length
  if (Array.isArray(msg.changes)) out.changes = msg.changes.length
  if (msg.type === 'subscription_error' || msg.type === 'connection_error' || msg.type === 'subscribe_rejected') out.message = msg.message
  return out
}
