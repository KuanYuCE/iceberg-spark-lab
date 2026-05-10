#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-B}   # A=low-freq, B=high-freq no partial, C=high-freq with partial
MODE=${2:-local}

echo "=== Exp07 ConcurrentConflicts — Scenario $SCENARIO, Mode $MODE ==="

mkdir -p results

rm -f results/exp07-ingestion-ready.flag
rm -f results/exp07-stop.flag
rm -f results/exp07-ingestion-metrics.json
rm -f results/exp07-compaction-metrics.json

case $SCENARIO in
  A)
    INTERVAL=2000
    PARTIAL=false
    MAX_COMMITS=5
    ;;
  B)
    INTERVAL=100
    PARTIAL=false
    MAX_COMMITS=5
    ;;
  C)
    INTERVAL=100
    PARTIAL=true
    MAX_COMMITS=5
    ;;
  *)
    echo "Unknown scenario: $SCENARIO. Use A, B, or C."
    exit 1
    ;;
esac

echo "Scenario $SCENARIO: intervalMs=$INTERVAL partialProgress=$PARTIAL maxCommits=$MAX_COMMITS"

# Start IngestionWorker in background
./gradlew run \
  -PmainClass=experiments.Exp07a_IngestionWorker \
  -PrunMode=$MODE \
  -PscenarioIntervalMs=$INTERVAL \
  > results/exp07-ingestion.log 2>&1 &
INGESTION_PID=$!
echo "IngestionWorker PID=$INGESTION_PID"

# Wait for ready flag (max 3 minutes)
echo "Waiting for ingestion ready signal..."
WAITED=0
until [ -f results/exp07-ingestion-ready.flag ] || [ $WAITED -ge 180 ]; do
  sleep 2
  WAITED=$((WAITED + 2))
done

if [ ! -f results/exp07-ingestion-ready.flag ]; then
  echo "ERROR: IngestionWorker did not signal ready within 3 minutes."
  kill $INGESTION_PID 2>/dev/null || true
  exit 1
fi
echo "IngestionWorker ready. Launching CompactionWorker."

# Run CompactionWorker (foreground — wait for it to complete)
./gradlew run \
  -PmainClass=experiments.Exp07b_CompactionWorker \
  -PrunMode=$MODE \
  -PpartialProgress=$PARTIAL \
  -PmaxCommits=$MAX_COMMITS \
  > results/exp07-compaction.log 2>&1

echo "CompactionWorker done. Writing stop flag."
touch results/exp07-stop.flag

# Wait for IngestionWorker to exit
wait $INGESTION_PID || true
echo "IngestionWorker exited."

# Run ResultAnalyzer
./gradlew run \
  -PmainClass=experiments.Exp07_ResultAnalyzer \
  -PrunMode=$MODE \
  > results/exp07-analyzer.log 2>&1

echo ""
echo "=== Exp07 Scenario $SCENARIO complete ==="
ls -lh results/Exp07_ConcurrentConflicts-*.html 2>/dev/null || echo "No HTML report found — check logs."
