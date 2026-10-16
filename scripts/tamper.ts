// Tries to get rows the caller was not sealed for. The attacker holds a valid sealed
// query for their own store (what any signed-in user of the board has) and wants
// the victim store's orders. Every attack has one exact expected outcome; an error,
// a timeout or any other outcome is a failure, and so is a single leaked victim row.
//   node scripts/tamper.ts [--preview-env .env.preview] [--self-test]
//
// --preview-env: a child branch's NEON_REALTIME_URL/SECRET (see README) for the
//   cross-branch attacks. --self-test: swaps one attack's payload for the victim's
//   real sealed query, to show the checks report a leak when there is one.
import { randomBytes } from 'node:crypto'
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { createRealtime } from '@neon/realtime/server'
import { drizzleAdapter } from '@neon/realtime-drizzle'
import { createDb } from '../lib/drizzle.ts'
import { createSealer, realtimeUrl } from '../lib/realtime.ts'
import { plainQuery } from '../lib/queries.ts'
import { createStore, prefillOrders } from '../lib/seed.ts'
import { rawSubscribe, type RawOutcome } from '../lib/raw-client.ts'
import { codeHash } from '../lib/run.ts'

const { values } = parseArgs({ options: { 'preview-env': { type: 'string' }, 'self-test': { type: 'boolean', default: false } } })
const VICTIM = 50, ATTACKER = 51
const url = realtimeUrl()
const { db, pool } = createDb(2)

const c = await pool.connect()
for (const [id, user, n] of [[VICTIM, 'victim-user', 30], [ATTACKER, 'attacker-user', 5]] as const) {
  await createStore(c, id, `tamper ${id}`, [user])
  await c.query('delete from orders where store_id = $1', [id])
  await prefillOrders(c, id, n, id)
}
const victimIds = new Set((await c.query('select id::text from orders where store_id = $1', [VICTIM])).rows.map((r) => r.id))
const attackerIds = new Set((await c.query('select id::text from orders where store_id = $1', [ATTACKER])).rows.map((r) => r.id))
c.release()
// Writes made during an attempt, so incremental changes are part of what is checked.
// Every new victim order id joins the victim set, so a leak of it would be counted.
const addOrder = async (store: number) => {
  const { rows } = await pool.query('insert into orders (store_id, customer_id, total_cents) values ($1, $2, 777) returning id::text as id', [store, store * 1000 + 1])
  ;(store === VICTIM ? victimIds : attackerIds).add(rows[0].id)
  return rows[0].id as string
}
if (victimIds.size === 0 || attackerIds.size === 0) throw new Error('victim or attacker store has no rows: refusing to run')

const sealer = createSealer()
const own = await sealer.seal({ query: plainQuery(db, ATTACKER) })
const victim = await sealer.seal({ query: plainQuery(db, VICTIM) })
const sealedAt = Date.now()
const [h, , iv, ct, tag] = own.capability.split('.')
const [vh, , viv, vct, vtag] = victim.capability.split('.')
const flip = (s: string, i: number) => s.slice(0, i) + (s[i] === 'A' ? 'B' : 'A') + s.slice(i + 1)
const forgedSealer = createRealtime({ secret: 'nrt_live_1' + randomBytes(32).toString('base64url'), db: 'neondb', adapter: drizzleAdapter() })
const forged = await forgedSealer.seal({ query: plainQuery(db, VICTIM) })

type Attack = { id: string; what: string; run: () => Promise<RawOutcome>; expect: (o: RawOutcome) => boolean; expected: string; documented?: boolean }
const rejected = (code: string) => (o: RawOutcome) => o.result === 'subscribe_rejected' && o.code === code
const attacks: Attack[] = [
  { id: 'C0', what: 'control: the attacker subscribes to their own sealed query, then their store gets an order',
    run: () => rawSubscribe(url, own.capability, { afterBaseline: async () => { await addOrder(ATTACKER) } }),
    expect: (o) => o.result === 'delivered' && o.rows.length === 5 && o.rows.every((r) => attackerIds.has(r.id!)) && o.changeRows.length === 1 && attackerIds.has(o.changeRows[0].id!),
    expected: 'delivered: the 5 attacker rows, then the new order as a change' },
  { id: 'D1', what: 'a stolen sealed query (the victim\'s) used by someone else, then the victim store gets an order', documented: true,
    run: () => rawSubscribe(url, victim.capability, { afterBaseline: async () => { await addOrder(VICTIM) } }),
    expect: (o) => o.result === 'delivered' && o.rows.length === 30 && o.changeRows.some((r) => victimIds.has(r.id!)),
    expected: 'delivered: the 30 victim rows, then the new order as a change (documented: a sealed query is a bearer token)' },
  { id: 'T1', what: 'one character changed in the ciphertext', run: () => rawSubscribe(url, [h, '', iv, flip(ct, 20), tag].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T2', what: 'one character changed in the GCM tag', run: () => rawSubscribe(url, [h, '', iv, ct, flip(tag, 3)].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T3', what: 'one character changed in the IV', run: () => rawSubscribe(url, [h, '', flip(iv, 2), ct, tag].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T4', what: 'header and IV from the attacker, ciphertext and tag from the victim', run: () => rawSubscribe(url, [h, '', iv, vct, vtag].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T5', what: 'ciphertext truncated by 8 characters', run: () => rawSubscribe(url, [h, '', iv, ct.slice(0, -8), tag].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T6', what: 'the victim query sealed with a guessed secret in the right format', run: () => rawSubscribe(url, forged.capability),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T7', what: 'header rewritten to alg "none"', run: () => rawSubscribe(url, [Buffer.from(JSON.stringify({ alg: 'none', enc: 'A256GCM', v: 1 })).toString('base64url'), '', iv, ct, tag].join('.')),
    expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  { id: 'T8', what: 'subscribe message with an extra "sql" field', run: () => rawSubscribe(url, own.capability, { extra: { sql: 'select id from orders' } }),
    expect: (o) => o.result === 'connection_error' && o.code === 'invalid_payload' && o.rows.length === 0, expected: 'connection_error invalid_payload' },
  { id: 'T9', what: 'renew the attacker\'s live subscription with the forged victim capability, then the victim store gets an order',
    run: () => rawSubscribe(url, own.capability, { renewWith: forged.capability, afterRenew: async () => { await addOrder(VICTIM) } }),
    expect: (o) => o.result === 'delivered' && o.renew?.result === 'subscription_error' && o.renew.code === 'renewal_failed' && o.renew.rows.length === 0,
    expected: 'own rows delivered; renewal: subscription_error renewal_failed; no victim rows or changes' },
]

// Cross-branch: a child branch inherits Realtime. Is its secret its own?
let preview: Record<string, string> | undefined
if (values['preview-env'] && existsSync(values['preview-env'])) {
  preview = Object.fromEntries(readFileSync(values['preview-env'], 'utf8').split('\n').filter((l) => l.includes('='))
    .map((l) => { const i = l.indexOf('='); return [l.slice(0, i), l.slice(i + 1).replace(/^"|"$/g, '')] }))
  if (!preview.NEON_REALTIME_SECRET || !preview.NEON_REALTIME_URL) preview = undefined
}
if (values['preview-env'] && !preview) console.log(`no Realtime URL and secret in ${values['preview-env']}: cross-branch attacks B1 and B2 skipped`)
if (preview) {
  console.log(`child branch secret is ${preview.NEON_REALTIME_SECRET === process.env.NEON_REALTIME_SECRET ? 'THE SAME AS' : 'different from'} the parent's; URL is ${preview.NEON_REALTIME_URL === url ? 'the same' : 'different'}`)
  const previewSealer = createRealtime({ secret: preview.NEON_REALTIME_SECRET, db: 'neondb', adapter: drizzleAdapter() })
  const previewSealed = await previewSealer.seal({ query: plainQuery(db, VICTIM) })
  attacks.push(
    { id: 'B1', what: 'a query sealed with the child branch secret, sent to the parent branch', run: () => rawSubscribe(url, previewSealed.capability),
      expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
    { id: 'B2', what: 'a query sealed with the parent secret, sent to the child branch', run: () => rawSubscribe(preview!.NEON_REALTIME_URL, victim.capability),
      expect: rejected('authorization_failed'), expected: 'subscribe_rejected authorization_failed' },
  )
}

if (values['self-test']) {
  // Make T1 send the victim's untampered capability. The checks must now report T1 as a leak.
  const t1 = attacks.find((a) => a.id === 'T1')!
  t1.run = () => rawSubscribe(url, victim.capability)
}

const results: any[] = []
for (const a of attacks) {
  const o = await a.run()
  const leaked = o.rows.filter((r) => victimIds.has(r.id!)).length + o.changeRows.filter((r) => victimIds.has(r.id!)).length
    + (o.renew?.rows.filter((r) => victimIds.has(r.id!)).length ?? 0)
  const ok = a.expect(o) && (a.documented || leaked === 0)
  results.push({ id: a.id, attack: a.what, expected: a.expected, got: `${o.result}${o.code ? ' ' + o.code : ''}${o.renew ? `; renew: ${o.renew.result}${o.renew.code ? ' ' + o.renew.code : ''}` : ''}`,
    victim_rows: leaked, ok })
}

// Expiry: wait until the attacker's own sealed query is over 60 s old, then use it.
const wait = 65_000 - (Date.now() - sealedAt)
if (wait > 0) await new Promise((r) => setTimeout(r, wait))
const expired = await rawSubscribe(url, own.capability)
results.push({ id: 'E1', attack: `the attacker's own sealed query, ${Math.round((Date.now() - sealedAt) / 1000)} s after sealing`,
  expected: 'subscribe_rejected authorization_expired', got: `${expired.result}${expired.code ? ' ' + expired.code : ''}`,
  victim_rows: 0, ok: expired.result === 'subscribe_rejected' && expired.code === 'authorization_expired' })

console.table(results.map(({ id, attack, expected, got, victim_rows, ok }) => ({ id, attack, expected, got, victim_rows, ok })))
const failed = results.filter((r) => !r.ok)
console.log(`${results.length - failed.length} of ${results.length} as expected${values['self-test'] ? ' (self-test: T1 must fail)' : ''}`)
const out = values['self-test'] ? 'results/tamper/self-test.json' : 'results/tamper/tamper.json'
writeFileSync(out.replace('.json', `-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`), JSON.stringify({ at: new Date().toISOString(), code_hash: codeHash(['scripts/tamper.ts']), victim_rows: victimIds.size, attacker_rows: attackerIds.size, results }, null, 2), { flag: 'wx' })
await pool.end()
if (values['self-test'] ? !(failed.length === 1 && failed[0].id === 'T1') : failed.length) process.exit(1)
