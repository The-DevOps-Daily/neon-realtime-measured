import { useCallback, useEffect, useMemo, useOptimistic, useState, useTransition } from 'react'
import { useLiveQuery } from '@neon/realtime-react'
import type { SealedLiveQuery } from '@neon/realtime/client'
import type { OpenOrder, StatusCount, StoreCustomer } from '../../lib/board-queries.ts'
import { addOrder, sealBoard, setStatus } from './api.ts'

const COLUMNS = ['new', 'preparing', 'ready'] as const
const NEXT: Record<string, string> = { new: 'preparing', preparing: 'ready', ready: 'done' }
const money = (cents: number) => `$${(cents / 100).toFixed(2)}`

// Fetch a sealed query once per store and part; the hook renews it through the same call.
function useSealed<Row>(storeId: number, part: 'orders' | 'customers' | 'stats') {
  const [sealed, setSealed] = useState<SealedLiveQuery<Row>>()
  useEffect(() => {
    let cancelled = false
    sealBoard<Row>(storeId, part).then((q) => { if (!cancelled) setSealed(q) })
    return () => { cancelled = true }
  }, [storeId, part])
  const refresh = useCallback(() => sealBoard<Row>(storeId, part), [storeId, part])
  return { sealed, refresh }
}

export function Board({ storeId }: { storeId: number }) {
  const orders = useSealed<OpenOrder>(storeId, 'orders')
  const customers = useSealed<StoreCustomer>(storeId, 'customers')
  const stats = useSealed<StatusCount>(storeId, 'stats')
  if (!orders.sealed || !customers.sealed || !stats.sealed) return <p>Loading the board</p>
  return <Live storeId={storeId} orders={orders} customers={customers} stats={stats} />
}

type Sealed<Row> = { sealed?: SealedLiveQuery<Row>; refresh: () => Promise<SealedLiveQuery<Row>> }

function Live({ storeId, orders, customers, stats }: { storeId: number; orders: Sealed<OpenOrder>; customers: Sealed<StoreCustomer>; stats: Sealed<StatusCount> }) {
  const o = useLiveQuery(orders.sealed!, { refreshQuery: orders.refresh })
  const c = useLiveQuery(customers.sealed!, { refreshQuery: customers.refresh })
  const s = useLiveQuery(stats.sealed!, { refreshQuery: stats.refresh })
  const [, startTransition] = useTransition()

  // Two incremental live queries, joined here instead of in one re-sent SQL join.
  const names = useMemo(() => new Map((c.data ?? []).map((x) => [x.id, x.name])), [c.data])
  const [shown, move] = useOptimistic(o.data ?? [], (rows: readonly OpenOrder[], change: { id: number; status: string }) =>
    rows.flatMap((r) => (r.id !== change.id ? [r] : change.status === 'done' ? [] : [{ ...r, status: change.status as OpenOrder['status'] }])))

  const advance = (order: OpenOrder) => startTransition(async () => {
    move({ id: order.id, status: NEXT[order.status] })
    const res = await setStatus(order.id, NEXT[order.status])
    // Hold the optimistic state until this transaction comes back on the live stream.
    if (res.kind === 'committed') await o.utils.awaitTxId(res.txid)
  })

  const newOrder = async () => {
    const ids = [...names.keys()]
    if (!ids.length) return
    await addOrder(storeId, ids[Math.floor(Math.random() * ids.length)], 500 + Math.floor(Math.random() * 4500))
  }

  return (
    <main>
      <header>
        <h1>Orders</h1>
        <div className="stats">
          {(s.data ?? []).map((x) => <div className="stat" key={x.status}>{x.status}: {x.orders} ({money(x.revenue_cents)})</div>)}
        </div>
        <button onClick={newOrder}>New order</button>
        <span className="status">orders {o.status} · customers {c.status} · totals {s.status}</span>
      </header>
      <section className="columns">
        {COLUMNS.map((col) => (
          <div className="column" key={col}>
            <h2>{col}</h2>
            {shown.filter((x) => x.status === col).sort((a, b) => a.id - b.id).map((x) => (
              <div className="card" key={x.id}>
                <strong>#{x.id}</strong> {names.get(x.customer_id) ?? '…'} · {money(x.total_cents)}
                <div><button onClick={() => advance(x)}>{NEXT[x.status]} →</button></div>
              </div>
            ))}
          </div>
        ))}
      </section>
    </main>
  )
}
