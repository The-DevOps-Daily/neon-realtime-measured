import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The Realtime URL is public (the secret is not); read it from the repo's .env.
for (const file of ['../.env', '../.env.local']) {
  try { process.loadEnvFile(new URL(file, import.meta.url).pathname) } catch {}
}

export default defineConfig({
  root: new URL('.', import.meta.url).pathname,
  plugins: [react()],
  define: { 'import.meta.env.VITE_NEON_REALTIME_URL': JSON.stringify(process.env.NEON_REALTIME_URL ?? '') },
  server: { port: 5173, proxy: { '/api': 'http://localhost:8787' } },
})
