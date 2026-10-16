// Applies db/*.sql in order over the direct connection.
import { readdirSync, readFileSync } from 'node:fs'
import { directClient } from '../lib/db.ts'

const client = directClient()
await client.connect()
for (const file of readdirSync('db').filter((f) => f.endsWith('.sql')).sort()) {
  await client.query(readFileSync(`db/${file}`, 'utf8'))
  console.log(`applied db/${file}`)
}
const { rows } = await client.query(
  `select table_name from information_schema.tables where table_schema = 'public' order by 1`,
)
console.log('tables:', rows.map((r) => r.table_name).join(', '))
await client.end()
