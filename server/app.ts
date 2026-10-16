// The board's API (Hono). It verifies the Neon Auth JWT, takes the user id from it,
// and seals live queries that join through store_members on that id. The browser
// never sees the Realtime secret, the SQL, or anyone else's parameters.
import { Hono, type Context } from 'hono'
import { HTTPException } from 'hono/http-exception'
import { and, eq, sql } from 'drizzle-orm'
import type { Db } from '../lib/drizzle.ts'
import { signIn, verifyToken } from '../lib/auth.ts'
import { openOrdersQuery, statusCountsQuery, storeCustomersQuery } from '../lib/board-queries.ts'
import { storeMembers, stores } from '../db/schema.ts'
import type { createSealer } from '../lib/realtime.ts'

type Sealer = ReturnType<typeof createSealer>
const STATUSES = ['new', 'preparing', 'ready', 'done'] as const
const txid = sql<string>`pg_current_xact_id()::text`

async function requireUser(c: Context): Promise<string> {
  const header = c.req.header('Authorization')
  const token = header?.startsWith('Bearer ') ? header.slice(7) : undefined
  if (!token) throw new HTTPException(401, { message: 'missing bearer token' })
  try {
    return (await verifyToken(token)).sub
  } catch {
    throw new HTTPException(401, { message: 'invalid token' })
  }
}

const int = (v: unknown) => (typeof v === 'number' && Number.isInteger(v) && v > 0 ? v : undefined)

export function createApp(db: Db, sealer: Sealer) {
  async function isMember(userId: string, storeId: number) {
    const rows = await db.select({ s: storeMembers.store_id }).from(storeMembers)
      .where(and(eq(storeMembers.user_id, userId), eq(storeMembers.store_id, storeId))).limit(1)
    return rows.length > 0
  }

  return new Hono()
    // Demo shortcut: sign in server-side and hand the browser its JWT. A production
    // app would use the @neondatabase/auth client in the browser instead.
    .post('/api/session', async (c) => {
      const { email, password } = await c.req.json<{ email?: string; password?: string }>()
      if (!email || !password) return c.json({ error: 'email and password required' }, 400)
      try {
        return c.json(await signIn(email, password))
      } catch {
        return c.json({ error: 'sign-in failed' }, 401)
      }
    })
    .get('/api/stores', async (c) => {
      const userId = await requireUser(c)
      const rows = await db.select({ id: stores.id, name: stores.name }).from(stores)
        .innerJoin(storeMembers, eq(storeMembers.store_id, stores.id)).where(eq(storeMembers.user_id, userId))
      return c.json(rows)
    })
    .post('/api/board/live', async (c) => {
      const userId = await requireUser(c)
      const body = await c.req.json<{ storeId?: unknown; part?: unknown }>()
      const storeId = int(body.storeId)
      if (!storeId) return c.json({ error: 'storeId must be a positive integer' }, 400)
      // A clear refusal for non-members. The join inside each query is what keeps
      // an already-sealed query scoped after a member is removed.
      if (!(await isMember(userId, storeId))) return c.json({ error: 'not a member of this store' }, 403)
      if (body.part === 'orders') return c.json(await sealer.seal({ query: openOrdersQuery(db, userId, storeId) }))
      if (body.part === 'customers') return c.json(await sealer.seal({ query: storeCustomersQuery(db, userId, storeId) }))
      if (body.part === 'stats') return c.json(await sealer.seal({ query: statusCountsQuery(db, userId, storeId) }))
      return c.json({ error: 'part must be orders, customers or stats' }, 400)
    })
    .post('/api/orders', async (c) => {
      const userId = await requireUser(c)
      const body = await c.req.json<{ storeId?: unknown; customerId?: unknown; totalCents?: unknown }>()
      const storeId = int(body.storeId), customerId = int(body.customerId), total = int(body.totalCents)
      if (!storeId || !customerId || !total) return c.json({ error: 'storeId, customerId, totalCents required' }, 400)
      // One statement: the membership check and the insert commit together, and the
      // transaction id comes back for the client to wait on.
      const { rows } = await db.execute<{ id: number; txid: string }>(sql`
        insert into orders (store_id, customer_id, total_cents)
        select ${storeId}, c.id, ${total} from customers c
        where c.id = ${customerId} and c.store_id = ${storeId}
          and exists (select 1 from store_members m where m.store_id = ${storeId} and m.user_id = ${userId})
        returning id, ${txid} as txid`)
      if (!rows.length) return c.json({ error: 'not allowed' }, 403)
      return c.json({ kind: 'committed', id: rows[0].id, txid: rows[0].txid })
    })
    .patch('/api/orders/:id', async (c) => {
      const userId = await requireUser(c)
      const id = Number(c.req.param('id'))
      const { status } = await c.req.json<{ status?: string }>()
      if (!Number.isInteger(id) || !STATUSES.includes(status as any)) return c.json({ error: 'bad request' }, 400)
      const { rows } = await db.execute<{ txid: string }>(sql`
        update orders o set status = ${status}
        where o.id = ${id} and o.status <> ${status}
          and exists (select 1 from store_members m where m.store_id = o.store_id and m.user_id = ${userId})
        returning ${txid} as txid`)
      return c.json(rows.length ? { kind: 'committed', txid: rows[0].txid } : { kind: 'unchanged' })
    })
}

export type AppType = ReturnType<typeof createApp>
