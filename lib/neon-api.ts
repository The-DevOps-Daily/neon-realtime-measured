import { required } from './env.ts'

const API = 'https://console.neon.tech/api/v2'

export async function neonApi<T = any>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${required('NEON_API_KEY')}`,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...(init.headers ?? {}),
    },
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${init.method ?? 'GET'} ${path}: HTTP ${res.status} ${text}`)
  return (text ? JSON.parse(text) : {}) as T
}

export const projectId = () => required('NEON_PROJECT_ID')
export const branchId = () => required('NEON_BRANCH_ID')

export async function endpoints() {
  const { endpoints } = await neonApi<{ endpoints: any[] }>(`/projects/${projectId()}/endpoints`)
  return endpoints
}
