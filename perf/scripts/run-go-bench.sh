#!/usr/bin/env bash
#
# Phase 6 — Go benchmark runner. Regenerates the Go baselines under
# perf/baselines/go/.
#
# Defaults are SMOKE settings (count=1, benchtime=10x) so the script
# completes in ~10s. Pass `--full` to use the defensible settings
# (count=5, benchtime=2s) — those take ~10 min and produce
# benchstat-quality numbers.
#
# Output files:
#   perf/baselines/go/core-<UTC>.txt            timestamped copy
#   perf/baselines/go/core-smoke.txt            latest run (also overwrites the README source)
#   perf/baselines/go/orchestrator-<UTC>.txt    same for serializer/ + deserializer/
#
# All cells are runnable without Docker or AWS — Glue is mocked.
#
# Usage:
#   perf/scripts/run-go-bench.sh           # smoke
#   perf/scripts/run-go-bench.sh --full    # full
#
set -euo pipefail

MODE=${1:-smoke}
case "$MODE" in
    smoke) COUNT=1; BENCHTIME=10x ;;
    --full|full) COUNT=5; BENCHTIME=2s ;;
    *) echo "usage: $0 [smoke|--full]" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GO_ROOT="$REPO_ROOT/native-schema-registry/golang"
OUT_DIR="$REPO_ROOT/perf/baselines/go"
TS=$(date -u +%Y%m%dT%H%M%SZ)

GO_BIN=${GO:-/usr/local/go/bin/go}

mkdir -p "$OUT_DIR"

# Inner module (core) — its own go.mod, run from its dir.
echo "==> core benches ($MODE, count=$COUNT, benchtime=$BENCHTIME)"
(
    cd "$GO_ROOT/pkg/gsrserde-go/core"
    GOPROXY=direct GOSUMDB=off "$GO_BIN" test \
        -run='^$' \
        -bench=. \
        -benchmem \
        -benchtime="$BENCHTIME" \
        -count="$COUNT" \
        -timeout=30m \
        ./...
) | tee "$OUT_DIR/core-$TS.txt"
cp "$OUT_DIR/core-$TS.txt" "$OUT_DIR/core-$MODE.txt"

# Outer module (serializer/, deserializer/, etc).
echo "==> orchestrator benches ($MODE, count=$COUNT, benchtime=$BENCHTIME)"
(
    cd "$GO_ROOT"
    GOPROXY=direct GOSUMDB=off "$GO_BIN" test \
        -run='^$' \
        -bench=. \
        -benchmem \
        -benchtime="$BENCHTIME" \
        -count="$COUNT" \
        -timeout=30m \
        ./pkg/gsrserde-go/serializer ./pkg/gsrserde-go/deserializer
) | tee "$OUT_DIR/orchestrator-$TS.txt"
cp "$OUT_DIR/orchestrator-$TS.txt" "$OUT_DIR/orchestrator-$MODE.txt"

echo
echo "Wrote:"
echo "  $OUT_DIR/core-$TS.txt"
echo "  $OUT_DIR/orchestrator-$TS.txt"
echo "Updated:"
echo "  $OUT_DIR/core-$MODE.txt"
echo "  $OUT_DIR/orchestrator-$MODE.txt"
