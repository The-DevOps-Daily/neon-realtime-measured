#!/usr/bin/env bash
# Re-runs made after the first review of the write-up: the board check (the API
# no longer queries Postgres when it seals), the stricter tamper harness, and the
# board idling with the compute's suspend timeout at 60 s (last: it needs quiet).
#   bash scripts/run-after-review.sh
set -uo pipefail
cd "$(dirname "$0")/.."
stamp() { date -u +%Y-%m-%dT%H-%M-%S; }
mkdir -p results/board results/tamper results/board-idle

echo "== board $(stamp)"
node scripts/board-check.ts 2>&1 | tee "results/board/board-check-$(stamp).txt"

echo "== tamper $(stamp)"
npx neon branches create --name tamper-preview-2 --no-secrets 2>&1 | tee results/tamper/preview-branch-2-create.txt
sleep 20
npx neon env pull --branch tamper-preview-2 --service realtime --file .env.preview 2>&1 | tee results/tamper/preview-env-2-pull.txt
node scripts/tamper.ts --preview-env .env.preview 2>&1 | tee "results/tamper/tamper-$(stamp).txt"
node scripts/tamper.ts --preview-env .env.preview --self-test 2>&1 | tee "results/tamper/self-test-$(stamp).txt"
npx neon branches delete tamper-preview-2 2>&1 | tee results/tamper/preview-branch-2-delete.txt
rm -f .env.preview

echo "== board idle $(stamp)"
node scripts/board-idle.ts 2>&1 | tee "results/board-idle/board-idle-$(stamp).txt"
echo "== done $(stamp)"
