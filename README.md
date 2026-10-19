# Neon Realtime, measured

A live order board on [Neon Realtime](https://neon.com/docs/realtime/overview) (public beta), and the scripts that measured it on a real Neon project: update latency for a plain query, a join and an aggregate at 1, 10 and 100 writes per second; which query forms are re-sent in full on every write; what a reconnect delivers; whether an open subscription keeps the compute awake; whether live queries slow writes on the primary; and what it takes to forge, tamper with or reuse a sealed query.

Everything here ran against Lakebase Postgres 18 on Neon in `aws-eu-central-1`, with the Realtime SDK `@neon/realtime@0.1.0`. **Realtime is in beta and Neon publishes no benchmarks for it yet. Every number below is a beta number from one project and one client machine.**

Write-up: [Neon Realtime, Measured](https://devops-daily.com/posts/neon-realtime-live-queries-measured)

## Run it

You need Node.js 22.18 or later (the scripts run as TypeScript with Node's type stripping) and a Neon project in a [Realtime region](https://neon.com/docs/introduction/regions#product-availability).

```bash
npm install
npx neon link                 # pick or create the project
npx neon deploy               # applies neon.ts: Neon Auth + Realtime on the branch, writes .env
npm run migrate               # db/001_schema.sql
npm run bench:one -- --shape join --rate 10 --duration 60   # one latency run
npm run bench:score           # score everything in results/latency
```

`neon.ts` is the whole platform setup:

```ts
export default defineConfig({
  auth: true,
  preview: { realtime: { allowedOrigins: ["http://localhost:5173"] } },
});
```

The scripts that call the Neon API (run metadata, `scale-to-zero.ts`) also need `NEON_API_KEY`, `NEON_PROJECT_ID` and `NEON_BRANCH_ID` in `.env`; see `.env.example`.

### The board

```bash
npm run board:seed            # two stores, alice and bob as Neon Auth users (data/users.json)
npm run board:api             # Hono API on :8787: verifies the Neon Auth JWT, seals live queries
npm run board:web             # Vite on :5173, proxies /api
```

The API seals without querying Postgres (the membership join inside each live query is what scopes the data), so a renewal never touches the database; see `results/board-idle/` for why that matters. The board shows open orders per status column, a header with counts and revenue per status, and moves orders along with optimistic updates confirmed by `awaitTxId`. It subscribes to three live queries, all scoped by a join through `store_members` on the user id from the verified token (`lib/board-queries.ts`).

The board's sign-in goes through its own API (`POST /api/session`), which signs in to Neon Auth server-side and hands the browser its JWT. That keeps the demo's dependencies small. A production app would use the [`@neondatabase/auth`](https://neon.com/docs/auth/overview) client in the browser.

## The scripts

| Script | What it does | Output |
|---|---|---|
| `scripts/explore-incremental.ts`, `explore-variants.ts` | Subscribes to many query forms at once and records, per write, whether each got an incremental batch or a full reset, and how many bytes | `results/explore/` |
| `scripts/bench-run.ts` | One latency run: a fresh store with a fixed window of orders, one subscriber process, one open-loop writer process | `results/latency/<run>/` |
| `scripts/bench-matrix.sh` | Two rounds of 3 shapes x 1, 10, 100 writes/s, plus one raw-recorder run of the join at 100 writes/s per round | `results/latency/`, `results/diagnostic/` |
| `scripts/bench-subscriber.ts`, `bench-writer.ts` | The two recorders a run spawns: the SDK client with a wire tap, and the open-loop writer. Both log their own event-loop stalls | per run |
| `scripts/raw-subscriber.ts` | Diagnostic recorder that speaks the wire protocol without the SDK and never decodes rows (`--subscriber raw`) | `results/diagnostic/` |
| `scripts/bench-score.ts` | Joins each write to the first update that made it visible, checks every update against the writes, sets apart writes that overlap a client stall | `summary.json`, `latencies.json` per run |
| `scripts/latency-report.ts`, `post-figures.ts` | Pools both rounds per shape and rate; builds the write-up's charts from the recorded files | `report.json`, `results/figures.json` |
| `scripts/bench-selftest.ts` | Corrupts copies of recorded runs to show the scorer reports each kind of error | `results/bench-selftest.txt` |
| `scripts/sdk-cost.ts` | CPU time the SDK client spends per update, per shape | `results/sdk-cost/` |
| `scripts/path-throughput.ts` | 20,000 rows as a live query's initial result and over a direct connection, to size the network path | `results/throughput/` |
| `scripts/reconnect.ts` | Cuts the network for 5 s and for 75 s and records what comes back | `results/reconnect/` |
| `scripts/scale-to-zero.ts` | Shortens the suspend timeout to 60 s and polls the compute state with a subscription open | `results/scale-to-zero/` |
| `scripts/primary-impact.ts` | Write latency with 100 live queries switched on and off in alternating blocks | `results/primary-impact/` |
| `scripts/tamper.ts` | Forged, edited, spliced, expired, cross-branch and stolen sealed queries, over the raw protocol | `results/tamper/` |
| `scripts/scoping.ts` | Per-user scoping without RLS, through the board API with real Neon Auth users | `results/scoping/` |
| `scripts/board-check.ts` | The board end to end through its API and the SDK, without a browser | `results/board/` |
| `scripts/board-idle.ts` | The board's three queries renewed through its API, with and without a database lookup on every seal, while the compute's state is polled | `results/board-idle/` |
| `scripts/run-after-review.sh` | The reruns made after the write-up's first review: board check, stricter tamper run, board idling | `results/run-after-review.log` |
| `scripts/run-experiments.sh` | Everything after the matrix, one at a time, scale-to-zero last | `results/run-experiments.log` |
| `scripts/session-events.ts`, `egress-meter.ts` | Server-side errors the recorders saw; their received bytes per hour against Neon's consumption API | `results/` |

### How latency is measured

The writer and the subscriber run as separate processes on the same machine and stamp events with the same clock (`performance.timeOrigin + performance.now()`). Each write is one transaction that inserts an order and deletes the store's oldest one, so the live result stays the same size for the whole run, and it returns its own `pg_current_xact_id()`.

- An incremental update carries the transaction ids it covers.
- A full result carries none, but the wire protocol starts every new baseline with the snapshot it was read at (`xmin`, `xmax`, `xip`). The scorer applies the Postgres visibility rule to that snapshot to decide which writes the new result includes.

Latency is from the moment the writer issued the statement to the moment the SDK delivered the update that first included it. Every update is also checked against the writes: an incremental batch must upsert exactly the rows its transactions wrote, a full result must contain exactly the rows the visible writes imply, and an aggregate's counts must add up. `bench-selftest.ts` corrupts recorded runs to show that each of these checks fails when it should.

Every run directory has a `meta.json` stamped with a hash of the code that recorded it, every line carries the run id, and the scorer refuses files from another run or a mix of code versions.

`lib/wiretap.ts` swaps the global `WebSocket` for the `ws` package with a listener that records every frame. The SDK's own server-side client uses `ws` the same way. The tap is what lets the scripts count bytes, read the snapshot of each reset, and cut the connection for the reconnect test.

## What the scripts found

All on Lakebase Postgres 18 in `aws-eu-central-1`, from one client with a 32 ms round trip (`SELECT 1`, median of per-run p50s). Numbers are from the files named; the write-up has the detail.

**Latency** (`results/latency/report.json`, 18 runs, 48,600 writes, none missing, no consistency mismatches; the checks cover the stamped rows each update adds, removal counts, full-result sizes and the aggregate's total order count, not deleted-row or seed-row identities, per-status counts, revenue or other column values). Write issued to update applied, p50 / p99 in ms:

| Shape | 1 write/s | 10 writes/s | 100 writes/s |
|---|---|---|---|
| plain | 41.9 / 52.6 | 42.8 / 58.6 | 34.7 / 85.0 |
| join (orders, customers, store_members) | 56.2 / 90.3 | 53.6 / 94.4 | 60.4 / 2,083 |
| aggregate | 47.2 / 63.0 | 42.4 / 119.7 | 40.7 / 647.9 |

For the join at 100 writes/s, the SDK on this client is the main limit: a separate benchmark at 20 updates/s measured 9.4 ms of CPU per 200-row result (`results/sdk-cost/`), about 94% of a core if that held at 100/s. Two complete raw-protocol runs on the same workload held p50 44 to 49 ms and p99 73 to 114 ms; a third had 220 writes delayed by up to 957 ms in its first five seconds, with no recorded client stall overlapping them, before a session loss ended it (`results/diagnostic/`). Eleven `backend_session_lost` errors in 59 minutes of subscriptions account for most of the one-second maximums; the SDK recovered from each in 0.95 to 1.51 s.

**Full results on every write** (`results/explore/`), in the forms we tested. A single-table filter and a join to `store_members` pinned to one row by the `WHERE` clause got incremental changes. Joins to `customers`, `EXISTS`, `IN (SELECT ...)`, `GROUP BY`, `count(*)`, `DISTINCT`, `ORDER BY` with or without `LIMIT`, and `LIMIT` alone were re-sent in full on every write. Bytes per write grow at about 134 per row for the join's columns (`results/size-sweep/`).

**Reconnect** (`results/reconnect/`). After a 5 s and a 75 s network cut, the first update was a complete result and the live rows matched the database exactly. Live again 2.8 s after the short cut, and 33 s and 48 s after the long one (SDK backoff).

**Scale to zero** (`results/scale-to-zero/`). With one subscription held open and renewed, the compute suspended after 75 s and stayed suspended; a write then woke it and reached the open subscription 1.12 s after it was issued. Starting a live query from a suspended compute woke it. In that file, `first_result_while_active_ms` is misnamed: the compute had just suspended when that subscription started, so it was a cold start too.

**The board idling** (`results/board-idle/`). Alice's three board queries renewed through the API, suspend timeout 60 s, no writes: with a `store_members` lookup on every seal the compute never suspended in 300 s (18 renewals); with sealing that does not touch Postgres, as the board ships, it suspended 75 s after the board opened.

**Primary impact** (`results/primary-impact/2026-10-16T19-07-34/`). 100 live queries on and off in alternating blocks, 20 writes/s: the write's execution time on the primary stayed at 0.15 to 0.17 ms in every block. On the client, writes were about 2.5 ms faster in the blocks with the queries on, which we put down to the client's Wi-Fi power saving (untested). Realtime stayed enabled throughout, so this does not measure the cost of `wal_level = logical`.

**Session losses** (`results/session-events.json`, `scripts/session-events.ts`). 11 server-side `backend_session_lost` errors in 59 subscription minutes of the matrix, and 9 more (plus one heartbeat timeout while the client was swapping) in the superseded first recording.

**The egress meter** (`results/egress-meter-*.json`, `scripts/egress-meter.ts`). In the three full hours from 15:00 to 18:00 UTC, the bench recorders received at least 862 MB of Realtime message payload, while the consumption API reported 16.2 MB of public network transfer for the project.

**Sealed queries** (`results/tamper/`). 14 of 14 outcomes as expected, in the first run and in a stricter rerun (exact error codes for every attempt, and writes during the control, the stolen-query case and the renewal attack so incremental changes are checked for leaks too). No tampered, forged or cross-branch attempt disclosed a victim row, and the expired capability was refused. A copied valid one works until it expires (60 s), as documented. The self-test swaps one attack for the real victim capability and must fail.

**Scoping** (`results/scoping/`, recorded with the API's membership lookup on seal turned on, as `scripts/scoping.ts` still sets it). With the membership join in the query, a removed member's result was empty 8 ms after the removal was acknowledged (one test, 5 ms polling). With the check only at seal time, new orders kept reaching them for 56 s.

## Notes on the recordings

- **Superseded runs are kept, not deleted.** `results/latency-v1/` is the first latency matrix, recorded while other jobs pushed the client into swap; its `README.md` says why it was replaced. `results/primary-impact/2026-10-16T18-53-06/` is the first impact run, which let the CPU clock follow the load and tagged some switching time to the wrong block; the second run fixes both.
- **Code hashes.** Each run is stamped with a hash of `lib/`, `db/`, `package-lock.json` and the scripts that recorded it, as they were at the time. The board's front-end dependencies were added to the lockfile after the latency matrix, and `readJsonl` learned to read gzipped files afterwards, so a fresh checkout hashes differently from the recorded stamps.
- **Raw files are gzipped.** `*.jsonl.gz` in each run directory; the scorers read either form.
- **The client.** A Raspberry Pi on Wi-Fi with power saving on and the `ondemand` CPU governor. Both make the client a few milliseconds faster when it is busy.
- **Not in the repo:** `.env` (connection strings and the Realtime secret) and `data/users.json` (the test users' generated passwords). The test users are on `example.com`.

## License

MIT
