// Prints what Realtime changes on the branch: WAL level, slots, publications,
// extensions, schemas and the computes the Neon API reports.
import { directClient } from '../lib/db.ts'
import { endpoints, neonApi, projectId } from '../lib/neon-api.ts'

const client = directClient()
await client.connect()
const q = async (label: string, sql: string) => {
  const { rows } = await client.query(sql)
  console.log(`-- ${label}`)
  console.table(rows)
}
await q('server', `select version() as version, current_setting('wal_level') as wal_level,
  current_setting('max_replication_slots') as max_replication_slots, current_setting('max_wal_senders') as max_wal_senders`)
await q('replication slots', `select slot_name, plugin, slot_type, database, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as behind from pg_replication_slots`)
await q('publications', `select pubname, puballtables, pubinsert, pubupdate, pubdelete from pg_publication`)
await q('extensions', `select extname, extversion from pg_extension order by 1`)
await q('schemas', `select nspname from pg_namespace where nspname not like 'pg_%' and nspname <> 'information_schema' order by 1`)
await q('wal senders', `select pid, application_name, state, sync_state, write_lag, flush_lag, replay_lag from pg_stat_replication`)
await q('backends other than this one', `select usename, application_name, backend_type, state from pg_stat_activity
  where pid <> pg_backend_pid() order by backend_type, usename`)
await client.end()

console.log('-- computes (Neon API)')
console.table((await endpoints()).map((e) => ({
  id: e.id, type: e.type, state: e.current_state, min_cu: e.autoscaling_limit_min_cu,
  max_cu: e.autoscaling_limit_max_cu, suspend_s: e.suspend_timeout_seconds,
})))
const { project } = await neonApi(`/projects/${projectId()}`)
console.log('-- project usage counters (Neon API)')
console.table([{ compute_time_seconds: project.compute_time_seconds, active_time_seconds: project.active_time_seconds,
  cpu_used_sec: project.cpu_used_sec, written_data_bytes: project.written_data_bytes, data_transfer_bytes: project.data_transfer_bytes }])
