// Neon Auth (Managed Better Auth) from Node: sign a user up or in, get their JWT,
// and verify a JWT against the branch's JWKS. The board API and the scripts use it.
import { createRemoteJWKSet, jwtVerify } from 'jose'
import { required } from './env.ts'

const base = () => required('NEON_AUTH_BASE_URL').replace(/\/$/, '')
const origin = () => new URL(base()).origin
let jwks: ReturnType<typeof createRemoteJWKSet> | undefined

async function post(path: string, body: unknown) {
  return fetch(`${base()}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: origin() },
    body: JSON.stringify(body),
  })
}

function sessionCookie(res: Response): string | undefined {
  const cookies = res.headers.getSetCookie()
  return cookies.map((c) => c.split(';')[0]).find((c) => c.includes('session_token'))
}

// Returns a short-lived JWT for the user, creating the account on first use.
export async function signIn(email: string, password: string, name = email.split('@')[0]) {
  let res = await post('/sign-in/email', { email, password })
  if (res.status === 401 || res.status === 400) {
    res = await post('/sign-up/email', { email, password, name })
  }
  if (!res.ok) throw new Error(`Neon Auth sign-in for ${email}: HTTP ${res.status} ${await res.text()}`)
  const cookie = sessionCookie(res)
  if (!cookie) throw new Error(`Neon Auth sign-in for ${email}: no session cookie`)
  const tokenRes = await fetch(`${base()}/token`, { headers: { Cookie: cookie, Origin: origin() } })
  if (!tokenRes.ok) throw new Error(`Neon Auth /token for ${email}: HTTP ${tokenRes.status}`)
  const { token } = (await tokenRes.json()) as { token: string }
  const { sub } = await verifyToken(token)
  return { token, userId: sub }
}

export async function verifyToken(token: string): Promise<{ sub: string }> {
  jwks ??= createRemoteJWKSet(new URL(`${base()}/.well-known/jwks.json`))
  const { payload } = await jwtVerify(token, jwks, { issuer: origin(), audience: origin() })
  if (!payload.sub) throw new Error('token has no sub')
  return { sub: payload.sub }
}
