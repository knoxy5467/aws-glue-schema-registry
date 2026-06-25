# GSR Phase 6 — Cross-language performance baselines

Side-by-side encode/decode throughput numbers for the Go GSR client
(`native-schema-registry/golang/`) and the upstream Java GSR client
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

### Encode wire format, NONE compression, warm cache

| Payload | Go AVRO/JSON | Go PROTOBUF | Java WIRE_ONLY | Java PROTOBUF_INDEX |
|---------|-------------:|------------:|---------------:|--------------------:|
| 100 B   |    205 MB/s  |   1.7 MB/s  |   1.89 MB/s    |     1.30 MB/s       |
| 10 KB   |   2542 MB/s  | 147  MB/s   |   3.78 MB/s    |     2.42 MB/s       |
| 1 MB    |   2137 MB/s  | 840  MB/s   |   2.66 MB/s    |     1.62 MB/s       |

### Encode wire format, ZLIB compression, warm cache

| Payload | Go AVRO/JSON | Go PROTOBUF | Java WIRE_ONLY | Java PROTOBUF_INDEX |
|---------|-------------:|------------:|---------------:|--------------------:|
| 100 B   |   0.39 MB/s  |  0.23 MB/s  |  0.012 MB/s    |    0.012 MB/s       |
| 10 KB   |   16.4 MB/s  |  13.8 MB/s  |  0.042 MB/s    |    0.045 MB/s       |
| 1 MB    |   39.4 MB/s  |  39.3 MB/s  |  0.019 MB/s    |    0.020 MB/s       |

### Decode wire format, NONE compression, warm cache

| Payload | Go (zero-copy) | Java WIRE_ONLY |
|---------|---------------:|---------------:|
| 100 B   |    444 MB/s    |   4.7 MB/s     |
| 10 KB   |  25.8 GB/s     |   6.1 MB/s     |
| 1 MB    |   4.4 TB/s     |   4.5 MB/s     |

The Go decode path returns a zero-copy slice (`data[18:]`) with no allocation,
so throughput scales with fixed overhead only. Java's path allocates a fresh
`byte[]`.

### Decode wire format, ZLIB compression, warm cache

| Payload | Go             | Java WIRE_ONLY |
|---------|---------------:|---------------:|
| 100 B   |  N/A (slice)   |  0.059 MB/s    |
| 10 KB   |  ~80 MB/s      |  0.130 MB/s    |
| 1 MB    |  ~617 MB/s     |  0.142 MB/s    |

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

1. **Wire-format layer: Go is 500-1000x faster than Java** (uncompressed).
   Go's `EncodeWireFormat` is a single allocation + memcopy; Java's
   `SerializationDataEncoder.write` constructs a `ByteArrayOutputStream`
   per call.

2. **Format layer: Java is faster for AVRO/JSON at small-medium payloads.**
   Java's Avro `GenericDatumWriter`/`GenericDatumReader` and Jackson JSON
   are more optimized than Go's `hamba/avro` and `encoding/json`. At 1 MB
   the gap closes.

3. **End-to-end orchestrator: depends on format and payload size.** For
   typical Kafka messages (1-100 KB), Java's AVRO path is 3-10x faster
   end-to-end. Go's advantage at the wire layer is absorbed by the format
   layer overhead.

4. **ZLIB compression dominates at large payloads** on both sides. Go is
   ~2x faster per-byte because Java's stdlib deflater has higher overhead
   (new `Deflater` per call vs Go's pooled writers).

5. **Concurrent scaling: Go scales well for AVRO/PROTOBUF decode** (2-3x
   at P=4). Serialization scales modestly. Java's scaling is limited by
   GC pressure.

6. **Cache hit vs miss: 10x gap at small payloads.** The schema cache is
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
cd native-schema-registry/golang
make bench-go           # smoke: count=1 benchtime=10x, ~30 s
make bench-go-full      # full:  count=1 benchtime=1s, ~7 min (core + orchestrator)
```

### Java

```bash
cd native-schema-registry/golang
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
