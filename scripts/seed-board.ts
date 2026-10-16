// Seeds the board: two stores, alice in one, bob in both, with customers and orders.
// Users sign up through Neon Auth on first run; credentials go to data/users.json.
//   node scripts/seed-board.ts
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { randomBytes } from 'node:crypto'
import { directClient } from '../lib/db.ts'
import { signIn } from '../lib/auth.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'

mkdirSync('data', { recursive: true })
const file = 'data/users.json'
const creds = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8'))
  : Object.fromEntries(['alice', 'bob', 'eve'].map((n) => [n, { email: `${n}.realtime@example.com`, password: randomBytes(12).toString('base64url') }]))
writeFileSync(file, JSON.stringify(creds, null, 2))
const alice = await signIn(creds.alice.email, creds.alice.password)
const bob = await signIn(creds.bob.email, creds.bob.password)

const c = directClient()
await c.connect()
await createStore(c, 10, 'Main St', [alice.userId, bob.userId])
await createStore(c, 11, 'Riverside', [bob.userId])
for (const id of [10, 11]) {
  const { rows } = await c.query('select count(*)::int as n from orders where store_id = $1', [id])
  if (rows[0].n === 0) await prefillOrders(c, id, 8, id)
}
await c.end()
console.log(`board seeded. Sign in as ${creds.alice.email} or ${creds.bob.email} (passwords in ${file})`)
