// Replaces globalThis.WebSocket with the `ws` package plus a tap, so a Node script
// using the browser Realtime client (createRealtimeClient) can record every frame
// it receives, count payload bytes, ping the server, and cut the connection.
//
// The Realtime SDK's own server-side client uses `ws` the same way, so this is the
// transport the SDK already supports in Node. The tap listener is added in the
// constructor, before the SDK's listener, so its timestamp is taken first.
import WS from 'ws'

export type WireFrame = {
  t: number // epoch ms, high resolution
  bytes: number
  msg: any // parsed JSON message
}

type Tap = {
  onFrame?: (frame: WireFrame) => void
  onSend?: (frame: WireFrame) => void
  // while true, new sockets fail to connect, as if the network were down
  blackhole: boolean
}

export const tap: Tap = { blackhole: false }
export const sockets = new Set<TapWebSocket>()
export const now = () => performance.timeOrigin + performance.now()

export class TapWebSocket extends WS {
  constructor(url: string | URL, protocols?: string | string[]) {
    // A blackholed connect goes to a TEST-NET-1 address (RFC 5737), which never answers.
    super(tap.blackhole ? 'wss://192.0.2.1:443/' : url, protocols, { handshakeTimeout: 5000 })
    sockets.add(this)
    this.on('close', () => sockets.delete(this))
    this.addEventListener('message', (event: any) => {
      const t = now()
      const data = event.data
      const text = typeof data === 'string' ? data : Buffer.from(data).toString('utf8')
      tap.onFrame?.({ t, bytes: Buffer.byteLength(text), msg: peek(text) })
    })
  }

  // Outgoing frames, so a run shows whether the client had to ask for anything.
  send(data: any, ...rest: any[]) {
    if (tap.onSend) {
      const text = typeof data === 'string' ? data : Buffer.from(data).toString('utf8')
      let msg: any
      try {
        msg = JSON.parse(text)
      } catch {
        msg = { type: '<non-json>' }
      }
      tap.onSend({ t: now(), bytes: Buffer.byteLength(text), msg })
    }
    return (super.send as any)(data, ...rest)
  }
}

// Frames over 4 KB (initial results, large change batches) are not parsed a second
// time here: the SDK already parses them, and a second full parse doubles the
// client's work. For those the tap keeps only the type.
function peek(text: string): any {
  if (text.length > 4096) return { type: /"type"\s*:\s*"([a-z_]+)"/.exec(text.slice(0, 256))?.[1] ?? '<unknown>', large: true }
  try {
    return JSON.parse(text)
  } catch {
    return { type: '<non-json>' }
  }
}

export function installWireTap() {
  ;(globalThis as any).WebSocket = TapWebSocket
}

// Abruptly destroy every open socket, with no close frame: what a dropped network looks like.
export function cutAll() {
  for (const s of sockets) s.terminate()
}

// WebSocket-level ping/pong round trip on the open socket.
export async function wsPing(samples = 20, gapMs = 100): Promise<number[]> {
  const s = [...sockets].find((x) => x.readyState === WS.OPEN)
  if (!s) return []
  const out: number[] = []
  for (let i = 0; i < samples; i++) {
    const t0 = now()
    await new Promise<void>((resolve) => {
      s.once('pong', () => resolve())
      s.ping()
    })
    out.push(now() - t0)
    await new Promise((r) => setTimeout(r, gapMs))
  }
  return out
}
