// Loads .env (connection strings) and .env.local (written by `neon env pull`).
for (const file of ['.env', '.env.local']) {
  try {
    process.loadEnvFile(file)
  } catch {
    // a missing file is fine; required() below names what is missing
  }
}

export function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is not set. See .env.example.`)
  return value
}
