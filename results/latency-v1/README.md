# Latency matrix, first recording (superseded)

These 18 runs are complete and their raw files are untouched. They were superseded,
not deleted, for two reasons found while checking the method:

1. **The client machine froze.** Other jobs on the same machine pushed it into
   swap during several runs, and the recorders' own event loops stalled for up to
   11 s (see `eld_max_ms` in each `end` line). Those stalls land in the timestamps,
   so the tails here mix Neon's behaviour with the client's. The second recording
   adds a stall monitor to both recorder processes, and its scorer reports the
   writes whose window overlaps a client stall separately.
2. **Mixed code versions.** `plain-1ps-w200-A-2026-10-16T15-01-49` carries code
   hash `a47a9076e14a57a6` and every other run `9bf5cd42f22ec314`: library files
   that the bench never imports were added to `lib/` while it ran. The scorer
   refuses mixed hashes, so this set is scored without that run.

`report.json` and the per-run `summary.json` files were produced by the scorer of
that time, without the stall split.
