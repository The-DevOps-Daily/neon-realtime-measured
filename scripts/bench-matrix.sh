#!/usr/bin/env bash
# The full latency matrix: two rounds (A, B) of 3 rates x 3 shapes, plus one
# diagnostic run per round of the join at 100 writes/s with the raw recorder.
# Shape order rotates per round so no shape always runs first or last.
#   bash scripts/bench-matrix.sh            (about 75 minutes)
set -euo pipefail
cd "$(dirname "$0")/.."
duration() { case $1 in 1) echo 300;; 10) echo 180;; 100) echo 60;; esac; }
for label in A B; do
  if [ "$label" = A ]; then shapes="plain join aggregate"; else shapes="aggregate plain join"; fi
  for rate in 1 10 100; do
    for shape in $shapes; do
      node scripts/bench-run.ts --shape "$shape" --rate "$rate" --duration "$(duration "$rate")" --label "$label"
    done
  done
  node scripts/bench-run.ts --shape join --rate 100 --duration 60 --label "$label" --subscriber raw --root results/diagnostic
done
node scripts/bench-score.ts results/latency
node scripts/bench-score.ts results/diagnostic
node scripts/latency-report.ts results/latency
