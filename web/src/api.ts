import type { SealedLiveQuery } from '@neon/realtime/client'

let token: string | undefined
export const setToken = (t: string) => { token = t }

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${method} ${path}: HTTP ${res.status}`)
  return res.json() as Promise<T>
}

export const signIn = (email: string, password: string) => call<{ token: string; userId: string }>('POST', '/api/session', { email, password })
export const listStores = () => call<{ id: number; name: string }[]>('GET', '/api/stores')
export const sealBoard = <Row>(storeId: number, part: 'orders' | 'customers' | 'stats') =>
  call<SealedLiveQuery<Row>>('POST', '/api/board/live', { storeId, part })
export const addOrder = (storeId: number, customerId: number, totalCents: number) =>
  call<{ kind: 'committed'; id: number; txid: string }>('POST', '/api/orders', { storeId, customerId, totalCents })
export const setStatus = (id: number, status: string) =>
  call<{ kind: 'committed'; txid: string } | { kind: 'unchanged' }>('PATCH', `/api/orders/${id}`, { status })
