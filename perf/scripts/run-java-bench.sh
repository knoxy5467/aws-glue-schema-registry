#!/usr/bin/env bash
#
# Phase 6 — Java JMH runner. Builds and runs the JMH benchmarks under
# native-schema-registry/perf/java/.
#
# Defaults are SMOKE settings (-i 1 -wi 1 -f 1 -r 1s) so the script
# completes in <2 min. Smoke uses ONE warmup iteration (-wi 1, not -wi 0)
# so the single measurement iteration runs against a JIT-warmed code path;
# Phase 6.1 review finding 5 flagged the original -wi 0 as producing
# baselines dominated by interpreted/C1 cost and not comparable to Go's
# auto-tuned b.N. -wi 1 is still smoke-grade — the JIT may compile but
# not fully optimize in one second — but the numbers are now in the same
# order of magnitude as a full run.
# Pass `--full` to drop the overrides and use the JMH defaults from
# @Warmup/@Measurement/@Fork on EncodeDecodeBench (3 warmup × 1s,
# 5 measurement × 1s, fork 2) — that takes ~10 min.
#
# Output files:
#   perf/baselines/java/wireformat-<UTC>.txt    timestamped copy
#   perf/baselines/java/wireformat-smoke.txt    latest smoke run
#   perf/baselines/java/wireformat-full.txt     latest full run
#
# Usage:
#   perf/scripts/run-java-bench.sh           # smoke
#   perf/scripts/run-java-bench.sh --full    # full (~10 min)
#
set -euo pipefail

MODE=${1:-smoke}
case "$MODE" in
    smoke)  JMH_FLAGS="-i 1 -wi 1 -f 1 -r 1s" ;;
    --full|full) JMH_FLAGS="" ;;
    *) echo "usage: $0 [smoke|--full]" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PERF_JAVA="$REPO_ROOT/native-schema-registry/perf/java"
OUT_DIR="$REPO_ROOT/perf/baselines/java"
TS=$(date -u +%Y%m%dT%H%M%SZ)

JAR="$PERF_JAVA/target/benchmarks.jar"

mkdir -p "$OUT_DIR"

# Build the JMH uber-jar only if it doesn't exist or the sources are newer.
NEEDS_BUILD=0
if [[ ! -f "$JAR" ]]; then
    NEEDS_BUILD=1
elif [[ -n "$(find "$PERF_JAVA/src" -newer "$JAR" 2>/dev/null | head -1)" ]]; then
    NEEDS_BUILD=1
fi

if [[ "$NEEDS_BUILD" -eq 1 ]]; then
    echo "==> building benchmarks.jar"
    (cd "$PERF_JAVA" && mvn -q -DskipTests package)
fi

case "$MODE" in
    smoke|"") MODE_LABEL=smoke ;;
    --full|full) MODE_LABEL=full ;;
esac

echo "==> JMH benchmark ($MODE_LABEL): java -jar benchmarks.jar $JMH_FLAGS"
java -jar "$JAR" $JMH_FLAGS 2>&1 | tee "$OUT_DIR/wireformat-$TS.txt"
cp "$OUT_DIR/wireformat-$TS.txt" "$OUT_DIR/wireformat-$MODE_LABEL.txt"

echo
echo "Wrote:"
echo "  $OUT_DIR/wireformat-$TS.txt"
echo "Updated:"
echo "  $OUT_DIR/wireformat-$MODE_LABEL.txt"
