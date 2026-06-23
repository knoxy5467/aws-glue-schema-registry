# GSR Phase 6 — Cross-language performance baselines

Side-by-side encode/decode throughput numbers for the Go GSR client
(`native-schema-registry/golang/`) and the upstream Java GSR client
(`serializer-deserializer/`, `common/`).

**Status: full defensible baselines, Phase 6.2 generator.** Numbers come
from `make bench-go-full` (count=3, benchtime=1s) plus `make bench-java-full`
(JMH defaults: warmup 3×1s, measurement 5×1s, fork 2). The Phase 6.2
fix made Go and Java consume **byte-identical** payloads — the xorshift64 +
64-char ASCII alphabet generator produces the same bytes from the same
seed in both languages (verified by spot-comparing the first 16 bytes:
`zuQNF7o0WCoXn7ad` on both sides). The ZLIB columns are now
apples-to-apples; the Phase 6.1 numbers were biased by a payload-shape
asymmetry (Java used SecureRandom = incompressible; Go used a 63-byte
repeating cycle = trivially compressible).

The plan source of truth for Phase 6 is `GSR-Golang-Plan-revision.md`
§7 Phase 6. The Go side is committed under
`native-schema-registry/golang/pkg/gsrserde-go/{core,serializer,deserializer}/*_bench_test.go`
plus the shared `test_helpers/perf_fixtures.go`; the Java side is
committed under `native-schema-registry/perf/java/`.

---

## Hardware caveats

These baselines were captured on an Amazon Linux 2 internal dev host:

- **CPU:** Intel Xeon Platinum 8175M @ 2.50 GHz (48 logical CPUs, AVX-512)
- **L3 cache:** 33 MiB
- **Kernel:** Linux 5.10.257-255.1015.amzn2int.x86_64 (CloudDesk)
- **Go:** 1.26.4 (`/usr/local/go/bin/go`)
- **JDK:** Amazon Corretto 17.0.19+10-LTS
- **JVM flags:** `-Xms2g -Xmx2g` (set by JMH `@Fork`)
- **Maven:** Apache 3.9.9

A shared cloud-desk host is not a clean benchmarking environment.
Treat these numbers as *order-of-magnitude indicative*. CI baselines
should run on a dedicated single-tenant instance.

---

## Payload generator

Phase 6.2 introduced a deterministic xorshift64-based payload generator
that produces byte-identical output on the Go and Java sides:

- **Seed:** `0x9E3779B97F4A7C15`
- **Algorithm:** xorshift64 (Marsaglia 2003) — `s ^= s<<13; s ^= s>>7; s ^= s<<17`
- **Output mapping:** `alphabet[(state >> 16) & 63]` where alphabet is
  `"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ."`
- **zlib ratio:** ~75% at 10 KB and 1 MB (realistic for compressed text;
  the old 63-byte cycle gave a deceptive ~1%, and crypto/rand gave
  ~100% i.e. uncompressible).

The same generator lives in three places (kept in lock-step):
`test_helpers/perf_fixtures.go::PerfPayload`,
`core/encoder_bench_test.go::benchPayload`,
`perf/java/.../EncodeDecodeBench.java::generatePerfPayload`.

---

## Side-by-side comparison

All numbers are MB/s of *source payload*. Higher is faster. Java column
converts JMH `us/op` to MB/s as `payloadSize / 1_048_576 / (avgt_us / 1_000_000)`.

Java has two `format` axes — `WIRE_ONLY` (header construction + optional
ZLIB) and `PROTOBUF_INDEX` (also concatenates the message-index varint
over a pre-marshaled `perf.Payload` message). The Go core column splits
AVRO/JSON/PROTOBUF — AVRO and JSON go through the same `EncodeWireFormat`
path so they match Java's WIRE_ONLY; PROTOBUF additionally pays the
varint-prefix cost, matching Java's PROTOBUF_INDEX. After the Phase 6.2
fix, both sides do the message-marshal work once at setup, not per
iteration.

### Encode wire format, NONE compression, warm cache

| Payload | Go AVRO/JSON | Go PROTOBUF | Java WIRE_ONLY | Java PROTOBUF_INDEX |
|---|---:|---:|---:|---:|
| 100 B   |  202 MB/s | 165 MB/s |  1.80 MB/s | 1.25 MB/s |
| 10 KB   | 2989 MB/s | 119 MB/s |  3.57 MB/s | 2.26 MB/s |
| 1 MB    | 2446 MB/s | 826 MB/s |  2.66 MB/s | 1.62 MB/s |

(Java numbers come from `EncodeDecodeBench.encode` — e.g. WIRE_ONLY @ 1 MB =
375.6 µs/op, PROTOBUF_INDEX @ 1 MB = 615.9 µs/op. The Java WIRE_ONLY → PROTOBUF_INDEX
delta at 100 B is now ~25 ns vs ~870 ns before Phase 6.2 — the difference is
the message-index varint copy cost, not DynamicMessage construction.)

### Encode wire format, ZLIB compression, warm cache

| Payload | Go AVRO/JSON | Go PROTOBUF | Java WIRE_ONLY | Java PROTOBUF_INDEX |
|---|---:|---:|---:|---:|
| 100 B   |  0.39 MB/s |   0.22 MB/s |   0.0107 MB/s | 0.0112 MB/s |
| 10 KB   | 24.8 MB/s  | 16.1  MB/s  |   0.0410 MB/s | 0.0396 MB/s |
| 1 MB    | 43.6 MB/s  | 42.2  MB/s  |   0.0188 MB/s | 0.0188 MB/s |

ZLIB dominates encode cost at large payloads. The Phase 6.2 fix made
this column *truer* — Java ZLIB encode @ 1 MB went from 35 ms (incompressible
SecureRandom input → zlib bails fast) to 53 ms (compressible xorshift input →
zlib actually compresses). Go's matching path is ~24 ms; both sides run
stdlib zlib at default level 6. Go is ~2× faster per source byte; the
gap is dominated by Java's `ByteArrayOutputStream + toByteArray()` overhead
plus the `Deflater`-per-call surface (each `SerializationDataEncoder.write`
constructs a new stream).

### Decode wire format, NONE compression, warm cache

The Go core `BenchmarkDecodeWireFormat` warm path returns a zero-copy
slice over `data[18:]` (no payload copy), so the reported MB/s is the
"caller can read this many bytes per second" rather than "decode work
scales with N bytes". The Java side allocates a fresh `byte[]` via
`array().clone()` semantics in `getPlainData`, so its column tracks
the real per-byte decode cost.

| Payload | Go (zero-copy) | Java WIRE_ONLY (copy) |
|---|---:|---:|
| 100 B   | 444 MB/s | 4.54 MB/s |
| 10 KB   | 43 GB/s  | 6.02 MB/s |
| 1 MB    | 4.4 TB/s | 4.30 MB/s |

The 4.4 TB/s figure is honest but misleading: it's fixed-overhead-only
(~220 ns/op regardless of payload size). For an apples-to-apples decode
comparison see the orchestrator-level rows below where both sides
actually marshal payload bytes into a user-visible value.

### Decode wire format, ZLIB compression, warm cache

| Payload | Go core (zero-copy) | Java WIRE_ONLY |
|---|---:|---:|
| 100 B   |  N/A (returns slice) |  0.077 MB/s |
| 10 KB   |  ~8 MB/s             |  0.158 MB/s |
| 1 MB    |  ~617 MB/s           |  0.149 MB/s |

ZLIB decode @ 1 MB now actually runs the inflate loop on real
compressed bytes (~6.7 ms in Java, ~1.7 ms in Go). Pre-Phase-6.2 the
Java number was 1.2 ms because the input compressed to nearly nothing
and `Inflater.inflate` had almost no work to do.

### Orchestrator-level (Serializer/Deserializer end-to-end, Go side)

| Format | 100 B encode | 10 KB encode | 1 MB encode |
|---|---:|---:|---:|
| AVRO       |  2.5 MB/s  |  207 MB/s |  962 MB/s |
| JSON       | ~9 MB/s    |  ~9 MB/s  |  ~9 MB/s  |
| PROTOBUF   | ~58 MB/s   | ~58 MB/s  |  ~191 MB/s |

| Format | 100 B decode | 10 KB decode | 1 MB decode |
|---|---:|---:|---:|
| AVRO       |   4.5 MB/s  |  379 MB/s | 2365 MB/s |
| JSON       |   7.4 MB/s  |  ~12 MB/s | ~10 MB/s  |
| PROTOBUF   | ~28 MB/s    | ~972 MB/s | ~1893 MB/s |

These exercise the full format-layer adapter + wire-format encode
(Avro/JSON/Protobuf marshal then GSR header + compression). The matching
Java path lives in `GlueSchemaRegistrySerializer.encode` and is
intentionally NOT exercised by the JMH module — the prompt calls for
wire-format-only on the Java side. The orchestrator numbers are
Go-only, useful for tracking the Go library's own regressions but not
directly comparable to the Java column above.

### Interpretation

- **Both implementations are CPU-bound, not Glue-bound.** Glue is
  mocked on both sides; the numbers measure pure wire-format,
  compression, and (for PROTOBUF) message-index varint cost.
- **Java's `SerializationDataEncoder` is ~660× slower than Go's
  `EncodeWireFormat`** on uncompressed 10 KB (3.57 MB/s vs 2989 MB/s).
  The cost is dominated by `ByteArrayOutputStream + toByteArray()`
  allocations in `SerializationDataEncoder.write` — every call
  allocates a fresh stream and copies through `toByteArray()`. A
  pre-sized `byte[]` write path would close most of the gap.
- **ZLIB compression dominates large-payload cost on both sides.**
  Java pays ~53 ms / Go pays ~24 ms at 1 MB encode — Go's ~2× lead is
  smaller than the uncompressed gap because both sides spend most of
  their CPU inside stdlib zlib at level 6. The Phase 6.2 generator
  fix made these numbers comparable (pre-fix the same cell was 35 ms
  on Java and ~24 ms on Go, but the Java number was deceptively low
  because SecureRandom input is incompressible).
- **PROTOBUF_INDEX adds ~25 ns at 100 B and amortizes to ~0% at 1 MB**
  on the Java side after Phase 6.2 hoisted DynamicMessage construction
  into `@Setup`. Pre-fix the same cell showed 0.94 µs/op (12× worse)
  because the message build ran inside the encode loop.
- **Go protobuf encode is dominated by `protoparse`** on every Encode
  call (~66 µs at 10 KB) — the format adapter rebuilds the FileDescriptor
  on every call. A descriptor cache would shift this column
  significantly. Phase 7+ optimization candidate.

---

## Files

```
perf/
├── README.md                          # this file
├── baselines/
│   ├── go/
│   │   ├── core-smoke.txt             # count=1 benchtime=10x (fast loop)
│   │   ├── core-full.txt              # count=3 benchtime=1s (committed, defensible)
│   │   ├── orchestrator-smoke.txt
│   │   └── orchestrator-full.txt
│   └── java/
│       ├── wireformat-smoke.txt       # -i 1 -wi 1 -f 1 -r 1s
│       └── wireformat-full.txt        # JMH defaults (warmup 3×1s, meas 5×1s, fork 2)
└── scripts/
    ├── run-go-bench.sh                # [smoke|--full]
    └── run-java-bench.sh              # [smoke|--full]
```

The timestamped `*-<UTC>.txt` files produced by the scripts are
intentionally NOT committed — they accumulate per run. `bench-compare`
in the Makefile picks the two most recent timestamped Go files and
runs `benchstat` over them.

---

## Regeneration

### Go

```bash
cd native-schema-registry/golang
make bench-go           # smoke: count=1 benchtime=10x, ~30 s
make bench-go-full      # full:  count=3 benchtime=1s, ~12 min on this CPU
```

The full-run wall-clock split: ~10 min for the core module
(`pkg/gsrserde-go/core`) and ~7 min for the orchestrator
(`pkg/gsrserde-go/serializer` + `deserializer`). The script
sequences them; you can run them individually:

```bash
cd pkg/gsrserde-go/core && go test -run='^$' -bench=. -benchmem -count=3 -benchtime=1s ./...
cd ../.. && go test -run='^$' -bench=. -benchmem -count=3 -benchtime=1s ./pkg/gsrserde-go/serializer ./pkg/gsrserde-go/deserializer
```

### Java

```bash
cd native-schema-registry/perf/java
mvn -DskipTests package                 # builds target/benchmarks.jar
cd ../../..
make bench-java          # smoke: -i 1 -wi 1 -f 1 -r 1s, ~2 min
make bench-java-full     # full:  JMH defaults from @Warmup/@Measurement/@Fork, ~13 min
```

The full run consumes ~7 min for `EncodeDecodeBench.encode` and ~6
min for `EncodeDecodeBench.decode`, with the `format × compression ×
payloadSize` matrix (2×2×3 = 12 cells per benchmark) at ~16 s/cell.

### Comparison

```bash
make bench-compare                # benchstat over the two most recent Go baselines
~/go/bin/benchstat <old.txt> <new.txt>
```

JMH does not have a benchstat equivalent. To detect Java regressions
re-run `make bench-java-full` and diff the `±` error bands in
`wireformat-full.txt`.

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
  cells significantly — a Phase 7+ optimization.
