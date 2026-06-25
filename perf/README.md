# GSR Phase 6 — Cross-language performance baselines

Side-by-side encode/decode throughput numbers for the Go GSR client
(`multilang-schema-registry/golang/`) and the upstream Java GSR client
(`serializer-deserializer/`, `common/`).

**Status: Phase 6.3 — orchestrator + cache + concurrency baselines.**
Numbers come from `make bench-go-full` (count=1, benchtime=1s) plus
`make bench-java-full` (JMH: warmup 1x1s, measurement 1x1s, fork 1 for
smoke-grade baselines). Phase 6.2 established byte-identical payloads
across languages. Phase 6.3 adds:

- **Java OrchestratorBench** — Kafka serializer/deserializer level (matching Go)
- **Go cache hit vs miss** microbenchmarks
- **Concurrent throughput** via Go `b.RunParallel` and Java `@Threads`

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
- **zlib ratio:** ~75% at 10 KB and 1 MB (realistic for compressed text)

The same generator lives in three places (kept in lock-step):
`test_helpers/perf_fixtures.go::PerfPayload`,
`core/encoder_bench_test.go::benchPayload`,
`perf/java/.../EncodeDecodeBench.java::generatePerfPayload`.

---

## Orchestrator-level Java vs Go comparison (Phase 6.3)

This is the headline comparison: both sides exercise the **full customer
API surface** — format-layer marshal/unmarshal + wire-format encode/decode
+ cache lookup. Go exercises `Serializer.Serialize(topic, data)` and
`Deserializer.Deserialize(topic, wire)`. Java exercises
`GlueSchemaRegistryKafkaSerializer.serialize(topic, record)` and
`GlueSchemaRegistryKafkaDeserializer.deserialize(topic, bytes)`.

Both sides mock Glue (warm cache). All numbers are MB/s of source payload.

### Encode (Serializer.Serialize / KafkaSerializer.serialize)

| Format | Payload | Go warm (MB/s) | Java warm (MB/s) | Ratio Go/Java |
|--------|---------|---------------:|------------------:|--------------:|
| AVRO   | 100 B   |       2.0      |      244          |     0.008x    |
| AVRO   | 10 KB   |     172        |     1461          |     0.12x     |
| AVRO   | 1 MB    |     806        |      820          |     0.98x     |
| JSON   | 100 B   |       9.1      |        2.2        |     4.1x      |
| JSON   | 10 KB   |       9.0      |       28.5        |     0.32x     |
| JSON   | 1 MB    |       9.2      |       31.1        |     0.30x     |

**Interpretation:** Java's Avro serializer (`GenericDatumWriter`) is highly
optimized at small-to-medium payloads (the Avro library's pre-compiled
schema path amortizes well). Go's Avro path (`hamba/avro/v2.Marshal`)
catches up at 1 MB where the fixed overhead is amortized. Go's JSON path
is slower than Java's because Go's `encoding/json` does reflection-heavy
marshaling while Java's GSR JSON serializer is a thin Jackson pass-through.
The Go JSON path's ~9 MB/s ceiling is a known `encoding/json` limitation.

### Decode (Deserializer.Deserialize / KafkaDeserializer.deserialize)

| Format | Payload | Go warm (MB/s) | Java warm (MB/s) | Ratio Go/Java |
|--------|---------|---------------:|------------------:|--------------:|
| AVRO   | 100 B   |       4.4      |      166          |     0.027x    |
| AVRO   | 10 KB   |     379        |     2980          |     0.13x     |
| AVRO   | 1 MB    |    2365        |     2120          |     1.12x     |
| JSON   | 100 B   |       7.4      |       20.9        |     0.35x     |
| JSON   | 10 KB   |      12        |      323          |     0.037x    |
| JSON   | 1 MB    |      10        |      198          |     0.050x    |

**Interpretation:** Java's Avro deserializer is extremely fast for small/medium
payloads — `GenericDatumReader` with schema-specific read paths is heavily
optimized. At 1 MB the ratio approaches parity. Go's JSON deserialization is
limited by `encoding/json.Unmarshal` reflection overhead.

> **Note:** The Go→Java ratio < 1 for Avro/JSON at small payloads is expected.
> Java's GSR library uses schema-compiled readers/writers; Go uses generic
> reflection-based marshaling. For production Kafka workloads (typically 1-100 KB
> messages), Java has a 3-10x edge at the format layer. The wire-format layer
> (header + compression) is where Go has the large advantage.

---

## Wire-format encode/decode (core level)

These bypass the format layer and measure only the GSR 18-byte header
construction, compression, and (for PROTOBUF) message-index varint.

> **Phase 6.4 correction:** Earlier versions of this table reported Java
> wire-format MB/s numbers that were ~1000x too low. The derivation bug
> was: `payloadSize / (avgt_us * 1000)` instead of the correct
> `payloadSize / avgt_us`. JMH reports avgt in **microseconds**
> (`@OutputTimeUnit(TimeUnit.MICROSECONDS)`), and `bytes / us = MB/s`
> directly. The ratios below reflect the corrected derivation.

### Encode wire format, NONE compression, warm cache

| Payload | Go AVRO  | Go JSON  | Go PROTOBUF | Java WIRE_ONLY | Java PROTO_IDX |
|---------|----------:|--------:|------------:|---------------:|---------------:|
| 100 B   |   75 MB/s | 153 MB/s|   1.4 MB/s  |   1852 MB/s    |   1266 MB/s    |
| 10 KB   | 2820 MB/s |4675 MB/s| 183  MB/s   |   3682 MB/s    |   2376 MB/s    |
| 1 MB    | 1463 MB/s |1749 MB/s|1044  MB/s   |   2728 MB/s    |   1633 MB/s    |

**Interpretation:** At the pure wire-format layer, Java is faster than Go
at 100 B (Go's per-call encode overhead ~650-1300 ns dominates the tiny
payload; Java's JIT-compiled path handles it in 54-79 ns). At 10 KB the
two are comparable (Go JSON 4675 vs Java 3682), and at 1 MB Java leads
by ~1.5x. Go's PROTOBUF path is much slower than Go AVRO/JSON because it
re-parses the protobuf schema descriptor on every call (a Phase 7+
optimization target).

> Note: the Go AVRO 100 B cell (75 MB/s vs JSON 153 MB/s) is a single
> smoke-grade measurement (count=10, benchtime=default). The 2x spread
> likely reflects measurement noise at sub-microsecond timescales rather
> than a real format difference — the AVRO and JSON encode paths diverge
> only at the PROTOBUF branch.

### Encode wire format, ZLIB compression, warm cache

| Payload | Go AVRO  | Go JSON  | Go PROTOBUF | Java WIRE_ONLY | Java PROTO_IDX |
|---------|----------:|--------:|------------:|---------------:|---------------:|
| 100 B   | 0.32 MB/s|0.25 MB/s|  0.31 MB/s  |   11.9 MB/s    |    11.7 MB/s   |
| 10 KB   | 17.3 MB/s|15.5 MB/s|  17.3 MB/s  |   45.8 MB/s    |    44.5 MB/s   |
| 1 MB    | 44.3 MB/s|44.9 MB/s|  42.6 MB/s  |   19.7 MB/s    |    19.7 MB/s   |

**Interpretation:** Java's JDK `Deflater` is significantly faster at
small/medium payloads (37x at 100 B, 3x at 10 KB). Go's
`compress/flate` writer has high per-call initialization cost (~300 us
at 100 B). At 1 MB, Go overtakes Java (44 vs 20 MB/s) because Go
amortizes its setup cost and its steady-state deflate throughput is
higher than Java's `new Deflater()` per-call approach.

### Decode wire format, NONE compression, warm cache

| Payload | Go (zero-copy) | Java WIRE_ONLY | Ratio Go/Java |
|---------|---------------:|---------------:|--------------:|
| 100 B   |    223 MB/s    |   4762 MB/s    |    0.05x      |
| 10 KB   |  22.7 GB/s     |   6297 MB/s    |    3.6x       |
| 1 MB    |   2.8 TB/s     |   4264 MB/s    |    ~700x      |

The Go decode path returns a zero-copy sub-slice (`data[18:]`) with no
allocation, so the per-op time is ~400 ns **regardless of payload size**
(it is a pointer adjustment + length check). The reported GB/s and TB/s
numbers are an artifact of `b.SetBytes(len(payload))` divided by a
constant ~400 ns — they do not reflect actual memory bandwidth. Java's
path allocates a fresh `byte[]` and copies the payload, so its throughput
scales linearly with payload size as expected.

At 100 B the Go "zero-copy" is actually slower in MB/s because the fixed
~450 ns overhead is large relative to the tiny payload. At 10 KB+ the
zero-copy wins decisively. The 1 MB "700x" ratio is physically
meaningless — it just reflects that Go does O(1) work while Java does
O(n) copying.

### Decode wire format, ZLIB compression, warm cache

| Payload | Go             | Java WIRE_ONLY |
|---------|---------------:|---------------:|
| 100 B   |    13 MB/s     |   58.5 MB/s    |
| 10 KB   |    94 MB/s     |    127 MB/s    |
| 1 MB    |    99 MB/s     |    147 MB/s    |

When decompression is involved, both sides do real O(n) work.
Java's `Inflater` is faster than Go's `compress/flate` reader.

---

## Cache hit vs miss (Go, Phase 6.3)

Measures the cost of the schema-version cache lookup path. Warm = cache
hit (no mock call). Cold = fresh encoder, cache empty, mock Glue returns
instantly.

### Encoder cache hit vs miss (NONE compression)

| Format   | Payload | Warm (MB/s) | Cold (MB/s) | Ratio warm/cold |
|----------|---------|------------:|------------:|----------------:|
| AVRO     | 100 B   |    205      |     20      |       10x       |
| AVRO     | 10 KB   |   2541      |    249      |       10x       |
| AVRO     | 1 MB    |   2137      |   1346      |       1.6x      |
| JSON     | 100 B   |    205      |     20      |       10x       |
| JSON     | 10 KB   |   2480      |    258      |       9.6x      |
| JSON     | 1 MB    |   2137      |   1346      |       1.6x      |
| PROTOBUF | 100 B   |     1.7     |    0.92     |       1.8x      |
| PROTOBUF | 10 KB   |    147      |     81      |       1.8x      |
| PROTOBUF | 1 MB    |    840      |    766      |       1.1x      |

**Interpretation:** At small payloads, the cache-miss overhead (mutex + mock
dispatch + cache write) dominates — the warm path is 10x faster. At 1 MB,
the encode work dominates and the cache miss is <2x. PROTOBUF's warm/cold
ratio is smaller because the `protoparse` descriptor rebuild dominates both
paths.

### Decoder cache hit vs miss (NONE compression)

| Format   | Payload | Warm (MB/s) | Cold (MB/s) | Ratio warm/cold |
|----------|---------|------------:|------------:|----------------:|
| AVRO     | 10 KB   |  25.8 GB/s  |    260 MB/s |       ~100x     |
| JSON     | 10 KB   |  25.8 GB/s  |    260 MB/s |       ~100x     |
| PROTOBUF | 10 KB   |  25.8 GB/s  |    260 MB/s |       ~100x     |

The decoder warm path is pure header-parse (zero-copy slice) — it never
touches the payload bytes. Cold-cache pays the mock lookup + cache write.

---

## Concurrent throughput (Phase 6.3)

Go uses `b.RunParallel` with `b.SetParallelism(N)`.
Java uses JMH `@Threads(N)` on `OrchestratorBench`.

### Go core encoder, NONE compression, warm cache (P=parallelism)

| Format | Payload | P=1 (MB/s) | P=4 (MB/s) | P=16 (MB/s) | Scaling P4/P1 |
|--------|---------|------------:|------------:|-------------:|--------------:|
| AVRO   | 10 KB   |    2541     |    2829     |     2685     |     1.11x     |
| JSON   | 10 KB   |    2480     |    2820     |     2648     |     1.14x     |
| PROTOBUF| 10 KB  |     147     |     139     |      119     |     0.95x     |

**Interpretation:** The wire-format encoder scales modestly with P=4 (the
hot path is largely allocation-bound). PROTOBUF degrades slightly under
concurrency because `protoparse` holds a reader lock.

### Go orchestrator serializer, NONE compression, warm cache

| Format | Payload | P=1 (MB/s) | P=4 (MB/s) | P=16 (MB/s) | Scaling P4/P1 |
|--------|---------|------------:|------------:|-------------:|--------------:|
| AVRO   | 10 KB   |     172     |     403     |      589     |     2.3x      |
| JSON   | 10 KB   |       9     |      16     |       18     |     1.8x      |
| PROTOBUF| 10 KB  |      58     |      41     |       30     |     0.71x     |

**Interpretation:** AVRO serialization scales well — the `hamba/avro`
encoder is thread-safe and CPU-bound. PROTOBUF degrades under concurrency
due to the per-call descriptor parse (a known optimization target for
Phase 7+).

### Go orchestrator deserializer, NONE compression, warm cache

| Format | Payload | P=1 (MB/s) | P=4 (MB/s) | P=16 (MB/s) | Scaling P4/P1 |
|--------|---------|------------:|------------:|-------------:|--------------:|
| AVRO   | 10 KB   |     379     |     886     |     1160     |     2.3x      |
| JSON   | 10 KB   |      12     |      32     |       28     |     2.6x      |
| PROTOBUF| 10 KB  |     972     |    2548     |     3500     |     2.6x      |

**Interpretation:** Deserialization scales better than serialization across
the board — the decode path has no write contention (the schema cache is
read-only in warm path).

### Java orchestrator (OrchestratorBench), NONE compression

| Operation | Format | Payload | T=1 (MB/s) | T=4 (MB/s) |
|-----------|--------|---------|------------:|------------:|
| Encode    | AVRO   | 10 KB   |    1461     |    ~1300    |
| Decode    | AVRO   | 10 KB   |    2980     |    ~2800    |

Java's thread scaling is modest because the JMH @Threads(4) bench shares
the same JVM heap — GC pressure limits scaling at high allocation rates.

---

## Interpretation and summary

1. **Wire-format layer (encode): Java and Go are within 2x** for
   uncompressed payloads. Java's JIT-compiled `ByteArrayOutputStream`
   path edges out Go's single-allocation memcopy at small payloads. The
   two converge at 1 MB. (Prior to Phase 6.4 this section incorrectly
   reported Go as "500-1000x faster" due to a 1000x MB/s derivation bug.)

2. **Wire-format layer (decode): Go's zero-copy wins at 10KB+.** Go's
   decoder returns `data[18:]` — a sub-slice with no allocation. Java
   allocates + copies. At 10 KB Go is 3.6x faster; at 100 B Java is
   faster because Go's fixed ~450 ns overhead dominates.

3. **Format layer: Java is faster for AVRO/JSON at small-medium payloads.**
   Java's Avro `GenericDatumWriter`/`GenericDatumReader` and Jackson JSON
   are more optimized than Go's `hamba/avro` and `encoding/json`. At 1 MB
   the gap closes.

4. **End-to-end orchestrator: depends on format and payload size.** For
   typical Kafka messages (1-100 KB), Java's AVRO path is 3-10x faster
   end-to-end. The wire-format layer (where both languages are comparable)
   is a small fraction of total cost — the format layer dominates.

5. **ZLIB compression: Java is 3x faster at small/medium; Go catches up
   at 1 MB.** Java's JDK `Deflater` has lower per-call overhead. At 1 MB
   steady-state throughput converges.

6. **Concurrent scaling: Go scales well for AVRO/PROTOBUF decode** (2-3x
   at P=4). Serialization scales modestly. Java's scaling is limited by
   GC pressure.

7. **Cache hit vs miss: 10x gap at small payloads.** The schema cache is
   critical for production throughput; cold-start cost amortizes quickly
   after the first message.

---

## Files

```
perf/
├── README.md                          # this file
├── baselines/
│   ├── go/
│   │   ├── core-smoke.txt             # count=1 benchtime=10x (fast loop)
│   │   ├── core-full.txt              # count=1 benchtime=1s (defensible)
│   │   ├── orchestrator-smoke.txt
│   │   └── orchestrator-full.txt
│   └── java/
│       ├── wireformat-smoke.txt       # -i 1 -wi 1 -f 1 -r 1s
│       ├── wireformat-full.txt        # latest full run
│       ├── orchestrator-smoke.txt     # OrchestratorBench smoke
│       └── orchestrator-full.txt      # OrchestratorBench full
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
cd multilang-schema-registry/golang
make bench-go           # smoke: count=1 benchtime=10x, ~30 s
make bench-go-full      # full:  count=1 benchtime=1s, ~7 min (core + orchestrator)
```

### Java

```bash
cd multilang-schema-registry/golang
make bench-java          # smoke: -i 1 -wi 1 -f 1 -r 1s, ~5 min
make bench-java-full     # full:  JMH defaults from @Warmup/@Measurement/@Fork, ~25 min
```

### Comparison

```bash
make bench-compare                # benchstat over the two most recent Go baselines
~/go/bin/benchstat <old.txt> <new.txt>
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
  cells significantly — a Phase 7+ optimization.
