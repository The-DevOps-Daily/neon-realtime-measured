// Serves the board API on :8787. The Vite dev server proxies /api here.
import { serve } from '@hono/node-server'
import { createDb } from '../lib/drizzle.ts'
import { createSealer } from '../lib/realtime.ts'
import { createApp } from './app.ts'

const { db } = createDb(4)
const app = createApp(db, createSealer())
serve({ fetch: app.fetch, port: 8787 }, (info) => console.log(`board API on http://localhost:${info.port}`))
