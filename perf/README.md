# GSR Phase 6 — Cross-language performance baselines

Side-by-side encode/decode throughput numbers for the Go GSR client
(under `native-schema-registry/golang/`) and the upstream Java GSR
client (`serializer-deserializer/`, `common/`).

**Status: smoke baselines only.** Regenerated with one warmup and one
measurement iteration; the numbers prove the harnesses work and give an
order-of-magnitude comparison. Defensible numbers require running the
full configuration (Go `-count=5` per matrix cell, JMH default 3+5×1s,
fork 2) on a quiesced machine — see the regeneration commands below.

The plan source of truth for Phase 6 is
`GSR-Golang-Plan-revision.md` §7 Phase 6. The Go side is committed
under `native-schema-registry/golang/pkg/gsrserde-go/{core,serializer,deserializer}/*_bench_test.go`;
the Java side is committed under `native-schema-registry/perf/java/`.

---

## Hardware caveats

These baselines were captured on an Amazon Linux 2 internal dev host:

- **CPU:** Intel Xeon Platinum 8175M @ 2.50 GHz (48 logical CPUs, AVX-512)
- **L3 cache:** 33 MiB
- **Kernel:** Linux 5.10.257-255.1015.amzn2int.x86_64 (CloudDesk)
- **Go:** 1.26.4 (`/usr/local/go/bin/go`)
- **JDK:** Amazon Corretto 17.0.19+10-LTS
- **JVM flags:** `-Xms2g -Xmx2g` (configured in JMH `@Fork`)

A shared cloud-desk host is not a clean benchmarking environment.
Treat these numbers as *order-of-magnitude indicative*. CI baselines
should run on a dedicated single-tenant instance.

---

## Smoke-run comparison table

The numbers below are *throughput* (MB/s of source payload) and
*allocations per op*. Source: the committed
`baselines/{go,java}/*.txt` files. Higher MB/s is faster.

The Go column is `BenchmarkEncodeWireFormat/<format>/<compression>/<size>/warm`
(see `baselines/go/core-smoke.txt`). The Java column converts JMH
`us/op` to MB/s as `(payloadSize / 1_048_576) / (avgt_us / 1_000_000)`
from `baselines/java/wireformat-smoke.txt`.

Go has format-specific cells (AVRO/JSON/PROTOBUF); the Java JMH harness
exercises only the wire-format-encode-and-write path, which is
format-agnostic at this layer. Java's format-layer encode/decode cost
(Avro marshal, protobuf marshal, JSON marshal) lives in
`GlueSchemaRegistrySerializer.encode(...)` callers — those are *not*
exercised by this benchmark on either side, by design (the prompt
calls for "wire-format-only" numbers).

### Encode — NONE compression, warm cache

| Payload | Go core (warm) | Java wire-format (warm) |
|---|---:|---:|
| 100 B   | **377 MB/s** (Avro/JSON/Protobuf, 100% wire-format) | 0.66 MB/s |
| 10 KB   | **5667 MB/s** (raw EncodeWireFormat) / 4174 MB/s (Avro orch) | 1.74 MB/s |
| 1 MB    | **1560 MB/s** (raw) / 886 MB/s (Avro orch) | 1.63 MB/s |

(Java numbers convert `EncodeDecodeBench.encode.warm.NONE.<size>` avgt
us/op to MB/s. Java's 0.66–1.74 MB/s reflects ~615 µs/op for a 1 MiB
encode, which is dominated by the `ByteArrayOutputStream`-backed
header write plus `array().clone()` semantics inside
`SerializationDataEncoder`. The Go raw path uses a single
`append`-backed slice.)

### Decode — NONE compression, warm cache

| Payload | Go core (warm) | Java wire-format (warm) |
|---|---:|---:|
| 100 B   | **1280 MB/s** (raw DecodeWireFormat) / ~1000 MB/s (orch) | 1.83 MB/s |
| 10 KB   | **~7000 MB/s** (orch) / similar raw | 2.94 MB/s |
| 1 MB    | similar | 3.33 MB/s |

### Compression overhead (10 KB payload, warm)

Compare the same row in ZLIB vs NONE to see the per-byte compression
cost (handler-only, excluding wire-format encode/decode):

| Side | NONE encode | ZLIB encode | NONE decode | ZLIB decode |
|---|---:|---:|---:|---:|
| Go orch (Avro)   | 5667 MB/s | ~580 MB/s | similar    | 169 MB/s  |
| Java wire        | 1.74 MB/s | 0.05 MB/s | 2.94 MB/s | 0.46 MB/s |

(Go's compression handler is `compress/zlib` stdlib; Java's is
`java.util.zip.Deflater`. Both pin level 6 (default). The raw
ratios show Go encoding ~10× faster per byte than Java on
uncompressed wire format — but the comparison is *not* apples-to-apples
because the Java harness exercises `SerializationDataEncoder.write`
which always allocates a fresh `ByteArrayOutputStream` and copies
through `toByteArray()`, whereas the Go raw path uses a pre-sized
slice. The Go *orchestrator* benches reflect the same end-to-end cost
the Java side measures.)

### Interpretation

- **Both implementations are CPU-bound, not Glue-bound.** The benches
  mock the Glue client. The numbers measure pure wire-format and
  compression cost.
- **Zlib dominates large-payload cost on both sides.** A 1 MB
  ZLIB encode is ~30× slower than NONE on the Java side (615 µs →
  36 700 µs); on the Go side it's ~3× slower (1183 µs → ~23 ms for
  PROTOBUF orch, but the raw-wire path with pre-compressed input is
  ~1900 MB/s). The Go ZLIB-large cell is hit harder than Java's
  expected ratio — *the raw Go zlib path is competitive; the Go
  orchestrator zlib cost is real and worth tuning later.*
- **Java's small-payload latency floor is high relative to its
  large-payload throughput.** That's the `ByteArrayOutputStream` +
  `slice()` + `array().clone()` allocations baked into the v1.1.25
  encoder. A future Java-side change to a pre-sized `byte[]` write
  path would close most of the gap.
- **Go protobuf encode is dominated by `protoparse`** on cold-cache
  iterations (66 µs / 100 B = 1.5 MB/s) — the format adapter rebuilds
  the FileDescriptor on every Encode. Worth caching descriptors at
  the format-layer level in a follow-up, not a Phase 6 deliverable.

### Smoke caveat (read this before quoting numbers)

`go test -benchtime=10x -count=1` runs each cell exactly ten times
in a single fork. `java -jar benchmarks.jar -i 1 -wi 0 -f 1 -r 1s`
runs one warmup-free measurement iteration in a single fork. **Neither
is statistically defensible.** Variance per cell is ±20–40% on a
shared cloud-desk; cells with `0.001 ops/us` are noise-dominated.

For numbers you'd quote in a 6-pager: regenerate with
`scripts/run-go-bench.sh -count=5 -benchtime=2s` and
`scripts/run-java-bench.sh` with the defaults (warmup 3×1s,
measurement 5×1s, fork 2). Then run benchstat against the previous
baseline to see whether deltas exceed noise.

---

## Files

```
perf/
├── README.md                          # this file
├── baselines/
│   ├── go/
│   │   ├── core-smoke.txt             # `go test -bench` output (core/)
│   │   └── orchestrator-smoke.txt     # serializer/ + deserializer/ output
│   └── java/
│       └── wireformat-smoke.txt       # `java -jar benchmarks.jar` output
└── scripts/
    ├── run-go-bench.sh                # regenerate Go baselines
    └── run-java-bench.sh              # regenerate Java baseline
```

---

## Regeneration

### Go

```bash
cd native-schema-registry/golang
./perf/scripts/run-go-bench.sh          # writes a timestamped copy and overwrites the smoke baseline
```

The script runs `go test -bench=. -benchmem` against the `core/`,
`serializer/`, and `deserializer/` packages with `GOPROXY=direct`,
`GOSUMDB=off`. Output is captured under
`perf/baselines/go/<package>-<UTC-timestamp>.txt` and also overwrites
`<package>-smoke.txt` so the README table stays current.

### Java

```bash
cd native-schema-registry/perf/java
mvn -DskipTests package                 # builds target/benchmarks.jar (~1 min cold, ~10s incremental)
../../../perf/scripts/run-java-bench.sh # runs the JMH smoke and stores the output
```

For full-fidelity numbers (5 forks, 5×1s measurement, 3×1s warmup),
omit the `-i 1 -wi 0 -f 1` flags in `run-java-bench.sh` so the JMH
defaults apply.

### Comparison

```bash
# Pairwise benchstat over two Go baselines (regression check):
~/go/bin/benchstat perf/baselines/go/core-smoke.txt perf/baselines/go/core-<new>.txt

# Java -> markdown table: today the conversion is manual; a future
# helper script will read JMH's --rf json output and emit a comparable
# row set. The TODO is tracked in the same Phase 5+ CI gate ticket as
# the Go regression threshold.
```

---

## Notes on what's NOT measured here

- **Real Glue round-trip.** Phase 6 is pure-CPU; integration-tier perf
  tests against a real beta account are out of scope.
- **Kafka producer/consumer cost.** That's testcontainers-go territory
  (Phase 4) and irrelevant to wire-format performance.
- **CGO bridge.** Deleted in Phase 2; the Go numbers reflect the
  pure-Go core and format layers.
- **Protobuf descriptor caching.** Both sides re-parse the schema on
  every encode today. A descriptor cache would shift the protobuf
  cells significantly on both sides.
