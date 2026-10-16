import type pg from 'pg'

// mulberry32: a small seeded generator that stays exact in JavaScript (Math.imul).
export function mulberry32(seed: number) {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export const CUSTOMERS_PER_STORE = 20
export const STATUSES = ['new', 'preparing', 'ready', 'done'] as const

// A fresh store with its customers and members. Customer ids are store * 1000 + n.
export async function createStore(c: pg.ClientBase, storeId: number, name: string, members: string[]) {
  await c.query('insert into stores (id, name) values ($1, $2) on conflict (id) do nothing', [storeId, name])
  for (let n = 1; n <= CUSTOMERS_PER_STORE; n++) {
    await c.query('insert into customers (id, store_id, name) values ($1, $2, $3) on conflict (id) do nothing', [
      storeId * 1000 + n,
      storeId,
      `Customer ${n}`,
    ])
  }
  for (const user of members) {
    await c.query('insert into store_members (store_id, user_id) values ($1, $2) on conflict do nothing', [storeId, user])
  }
}

export async function prefillOrders(c: pg.ClientBase, storeId: number, count: number, seed: number) {
  if (count <= 0) return
  const rand = mulberry32(seed)
  const values: string[] = []
  const params: unknown[] = []
  for (let i = 0; i < count; i++) {
    const p = params.length
    values.push(`($${p + 1}, $${p + 2}, $${p + 3}, $${p + 4})`)
    params.push(storeId, storeId * 1000 + 1 + Math.floor(rand() * CUSTOMERS_PER_STORE),
      STATUSES[Math.floor(rand() * 3)], 500 + Math.floor(rand() * 9500))
    if (values.length === 500 || i === count - 1) {
      await c.query(`insert into orders (store_id, customer_id, status, total_cents) values ${values.join(',')}`, params)
      values.length = 0
      params.length = 0
    }
  }
}
