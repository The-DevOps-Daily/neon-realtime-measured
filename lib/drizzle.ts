import { drizzle } from 'drizzle-orm/node-postgres'
import { directPool } from './db.ts'
import * as schema from '../db/schema.ts'

// Live queries are built with this client but never run through it: the
// Realtime Drizzle adapter only compiles them to SQL. Writes do run through it.
export function createDb(poolSize = 4) {
  const pool = directPool(poolSize)
  return { db: drizzle(pool, { schema }), pool }
}
export type Db = ReturnType<typeof createDb>['db']
