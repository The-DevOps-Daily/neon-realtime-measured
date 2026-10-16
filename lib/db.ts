import pg from 'pg'
import { required } from './env.ts'

// Neon URLs say sslmode=require; pg treats that as verify-full and warns, so say it.
const strict = (url: string) => url.replace('sslmode=require', 'sslmode=verify-full')

// Direct (unpooled) connection: migrations, inspection, and the measured writers,
// so a write's latency is Postgres plus the network, with no PgBouncer hop.
export const directUrl = () => strict(required('DATABASE_URL_UNPOOLED'))
// Pooled connection: what the app server uses for ordinary request traffic.
export const pooledUrl = () => strict(required('DATABASE_URL'))

export function directClient() {
  return new pg.Client({ connectionString: directUrl(), application_name: 'realtime-measured' })
}

export function directPool(max: number) {
  const pool = new pg.Pool({ connectionString: directUrl(), max, application_name: 'realtime-measured' })
  pool.on('error', (err) => console.error('pool error:', err.message))
  return pool
}
