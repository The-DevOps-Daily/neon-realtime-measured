import { createRealtime } from '@neon/realtime/server'
import { drizzleAdapter } from '@neon/realtime-drizzle'
import { required } from './env.ts'

// The backend half: seals queries locally with the branch secret. No network call.
export function createSealer() {
  return createRealtime({
    secret: required('NEON_REALTIME_SECRET'),
    db: 'neondb',
    adapter: drizzleAdapter(),
  })
}

export const realtimeUrl = () => required('NEON_REALTIME_URL')
