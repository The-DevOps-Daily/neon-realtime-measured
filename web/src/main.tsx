import { StrictMode, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { createRealtimeClient } from '@neon/realtime/client'
import { drizzleParsers } from '@neon/realtime-drizzle/client'
import { RealtimeProvider } from '@neon/realtime-react'
import { listStores, setToken, signIn } from './api.ts'
import { Board } from './Board.tsx'

// One client for the whole app: every live query shares its WebSocket.
const client = createRealtimeClient({ url: import.meta.env.VITE_NEON_REALTIME_URL, parsers: { ...drizzleParsers } })

function App() {
  const [stores, setStores] = useState<{ id: number; name: string }[]>()
  const [storeId, setStoreId] = useState<number>()
  const [error, setError] = useState<string>()

  if (!stores) {
    return (
      <form onSubmit={async (e) => {
        e.preventDefault()
        const form = new FormData(e.currentTarget)
        try {
          const { token } = await signIn(String(form.get('email')), String(form.get('password')))
          setToken(token)
          const mine = await listStores()
          setStores(mine)
          setStoreId(mine[0]?.id)
        } catch (err) { setError(String(err)) }
      }}>
        <h1>Live order board</h1>
        <input name="email" type="email" placeholder="email" required />
        <input name="password" type="password" placeholder="password" required />
        <button type="submit">Sign in with Neon Auth</button>
        {error && <p role="alert">{error}</p>}
      </form>
    )
  }
  if (!storeId) return <p>You are not a member of any store.</p>
  return (
    <>
      <select value={storeId} onChange={(e) => setStoreId(Number(e.target.value))}>
        {stores.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
      </select>
      <Board key={storeId} storeId={storeId} />
    </>
  )
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RealtimeProvider client={client}>
      <App />
    </RealtimeProvider>
  </StrictMode>,
)
