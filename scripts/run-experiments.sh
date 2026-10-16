#!/usr/bin/env bash
# Everything after the latency matrix, one at a time, so no experiment disturbs
# another's timing. scale-to-zero.ts goes last: it needs the database quiet.
#   bash scripts/run-experiments.sh
set -uo pipefail
cd "$(dirname "$0")/.."
stamp() { date -u +%Y-%m-%dT%H-%M-%S; }
mkdir -p results/explore results/size-sweep results/tamper results/scoping results/reconnect results/primary-impact results/scale-to-zero results/throughput

echo "== exploration $(stamp)"
node scripts/explore-incremental.ts --rows 300 --store 904 2>&1 | tee "results/explore/explore-incremental-300-rows-$(stamp).txt"
node scripts/explore-variants.ts --rows 300 --store 905 2>&1 | tee "results/explore/explore-variants-300-rows-$(stamp).txt"

echo "== result size sweep $(stamp)"
for w in 10 100 1000 5000; do
  node scripts/bench-run.ts --shape join --rate 1 --duration 60 --window "$w" --label S --root results/size-sweep
done
node scripts/bench-run.ts --shape plain --rate 1 --duration 60 --window 5000 --label S --root results/size-sweep
node scripts/bench-score.ts results/size-sweep

echo "== SDK cost per update $(stamp)"
mkdir -p results/sdk-cost
node scripts/sdk-cost.ts 2>&1 | tee "results/sdk-cost/sdk-cost-$(stamp).txt"

echo "== path throughput $(stamp)"
node scripts/path-throughput.ts --rows 20000 --store 95 2>&1 | tee "results/throughput/throughput-$(stamp).txt"

echo "== tamper $(stamp)"
npx neon branches create --name tamper-preview --no-secrets 2>&1 | tee results/tamper/preview-branch-create.txt
sleep 20
npx neon env pull --branch tamper-preview --service realtime --file .env.preview 2>&1 | tee results/tamper/preview-env-pull.txt
node scripts/tamper.ts --preview-env .env.preview 2>&1 | tee "results/tamper/tamper-$(stamp).txt"
node scripts/tamper.ts --preview-env .env.preview --self-test 2>&1 | tee "results/tamper/self-test-$(stamp).txt"
npx neon branches delete tamper-preview 2>&1 | tee results/tamper/preview-branch-delete.txt
rm -f .env.preview

echo "== board $(stamp)"
mkdir -p results/board
node scripts/seed-board.ts 2>&1 | tee "results/board/seed-$(stamp).txt"
node scripts/board-check.ts 2>&1 | tee "results/board/board-check-$(stamp).txt"

echo "== scoping $(stamp)"
node scripts/scoping.ts 2>&1 | tee "results/scoping/scoping-$(stamp).txt"

echo "== reconnect $(stamp)"
node scripts/reconnect.ts 2>&1 | tee "results/reconnect/reconnect-$(stamp).txt"

echo "== primary impact $(stamp)"
node scripts/primary-impact.ts --rate 20 --users 100 2>&1 | tee "results/primary-impact/primary-impact-$(stamp).txt"

echo "== scale to zero $(stamp)"
node scripts/scale-to-zero.ts 2>&1 | tee "results/scale-to-zero/scale-to-zero-$(stamp).txt"
echo "== done $(stamp)"
