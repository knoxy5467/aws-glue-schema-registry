# Phase 4.15 — Demo Binary: Narrated Java-Go Cross-Language Cross-Version Interop

## 1. Overview

The Go GSR client has achieved full cross-language cross-version interop (Phase 4.13) and Java parity (Phase 4.14). What it lacks is a way to showcase this to a human observer in real time. This phase delivers a runnable demo binary that exercises the existing interop machinery (Java sidecar, testcontainers Kafka, real AWS Glue) and emits a tagged narration stream so a screen-share viewer can watch Java and Go take turns producing and consuming through the same Kafka topic and the same Glue registry, across schema versions.

The demo binary is a composition layer only. It reuses every existing package without modification to the production surface.

## 2. Architecture

The demo binary lives at `/workplace/mrknox/phase-4.15/native-schema-registry/golang/cmd/demo/main.go` within the `integration-tests` module (it carries the `//go:build integration` tag and uses the module path `github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests`).

Internal packages under `cmd/demo/internal/` provide the narration engine, cell runner, and CLI flag handling.

```
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ cmd/demo/main.go                                                        │
 │   parses CLI flags, gates on env vars, wires signal handlers            │
 │   defers realglue.Cleanup                                               │
 └──────────────────────────┬──────────────────────────────────────────────┘
                            │
 ┌──────────────────────────▼──────────────────────────────────────────────┐
 │ cmd/demo/internal/runner                                                │
 │   CellRunner: iterates the scenario matrix, emits [demo]/[verdict]      │
 │   owns: scenario filtering (--cell, --format, --compression, etc.)      │
 │         per-cell lifecycle: setup → run → teardown                       │
 └──────┬──────────┬──────────┬──────────┬──────────┬──────────────────────┘
        │          │          │          │          │
        ▼          ▼          ▼          ▼          ▼
 ┌──────────┐ ┌──────────┐ ┌────────┐ ┌────────┐ ┌─────────────────────┐
 │javasidecar│ │realglue  │ │kafka-  │ │gsrserde│ │cmd/demo/internal/   │
 │(boot +   │ │(client + │ │harness │ │-go ser │ │narrator             │
 │ RPC)     │ │ cleanup) │ │(broker)│ │+ deser │ │(tag-based log emit) │
 └──────────┘ └────┬─────┘ └────────┘ └────────┘ └─────────────────────┘
                   │
       ┌───────────▼──────────────────────────────────────────┐
       │ AWS SDK v2 Glue client                                │
       │  └─ APIOptions middleware → emits [glue] log lines    │
       └───────────────────────────────────────────────────────┘
```

### Module placement

The demo binary is part of the `integration-tests` module because it imports the test-infrastructure packages (`pkg/javasidecar`, `pkg/kafkaharness`, `pkg/realglue`) that live in that module. The binary's main package path is:

```
github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/cmd/demo
```

Build command: `go build -tags integration ./cmd/demo` (run from within `integration-tests/`).

### File layout

```
/workplace/mrknox/phase-4.15/native-schema-registry/golang/
├── cmd/demo/
│   └── main.go                        # entry point (//go:build integration)
├── integration-tests/
│   ├── cmd/demo/
│   │   ├── main.go                    # entry point
│   │   └── internal/
│   │       ├── narrator/
│   │       │   ├── narrator.go        # Narrator struct + Emit(tag, msg)
│   │       │   └── narrator_test.go
│   │       ├── runner/
│   │       │   ├── runner.go          # CellRunner + scenario matrix
│   │       │   ├── cache_scenario.go  # cache hit/miss/evict demo
│   │       │   ├── autoregister.go    # auto-register fall-through demo
│   │       │   ├── baseline.go        # same-version baseline demo
│   │       │   └── runner_test.go
│   │       ├── wireformat/
│   │       │   ├── inspect.go         # parse + format wire bytes
│   │       │   └── inspect_test.go
│   │       └── flags/
│   │           ├── flags.go           # CLI flag definitions + parsing
│   │           └── flags_test.go
│   └── ...existing packages...
```

**Correction:** Given the module boundary, the actual file tree is rooted in `integration-tests/cmd/demo/`. The brief's stated path `native-schema-registry/golang/cmd/demo/main.go` means a symlink or redirect from the outer module. The spec uses the integration-tests module path for all imports.

**Resolution:** Place `main.go` at `/workplace/mrknox/phase-4.15/native-schema-registry/golang/integration-tests/cmd/demo/main.go` and add a one-line wrapper at `/workplace/mrknox/phase-4.15/native-schema-registry/golang/cmd/demo/main.go` that simply documents "build from integration-tests/cmd/demo". OR, per the brief's stated AC-1, place the binary directly at the outer-module path and import integration-tests packages via the replace directive that already exists in the integration-tests module. Since the integration-tests module has `replace github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang => ../` already, the inverse does NOT exist. The binary MUST live in the integration-tests module. The brief's path `native-schema-registry/golang/cmd/demo/main.go` is interpreted as a convenience symlink. The actual compilable source is at `integration-tests/cmd/demo/main.go`.

## 3. Observability Strategy

Each of the 11 tags is sourced from a wrapper, middleware, or observer OUTSIDE the production `pkg/gsrserde-go/` surface. No production code changes required.

### `[java-producer]` and `[java-consumer]`

**Source:** Wrap the `*javasidecar.Sidecar` in a thin `NarratedSidecar` struct that delegates to `KafkaProduce` and `KafkaConsume`, logging before and after each call.

```go
// cmd/demo/internal/runner/narrated_sidecar.go
type NarratedSidecar struct {
    sc       *javasidecar.Sidecar
    narrator *narrator.Narrator
}

func (n *NarratedSidecar) KafkaProduce(ctx context.Context, req javasidecar.KafkaProduceRequest) (*javasidecar.KafkaProduceResponse, error) {
    n.narrator.Emit("java-producer", "Serialize(topic=%s, format=%s, schema=%s)", req.Topic, req.Format, req.SchemaName)
    resp, err := n.sc.KafkaProduce(ctx, req)
    if err == nil {
        n.narrator.Emit("java-producer", "produced offset=%d partition=%d versionId=%s", resp.Offset, resp.Partition, resp.SchemaVersionID)
    }
    return resp, err
}

func (n *NarratedSidecar) KafkaConsume(ctx context.Context, req javasidecar.KafkaConsumeRequest) (*javasidecar.KafkaConsumeResponse, error) {
    n.narrator.Emit("java-consumer", "Deserialize(topic=%s, format=%s)", req.Topic, req.Format)
    resp, err := n.sc.KafkaConsume(ctx, req)
    if err == nil {
        n.narrator.Emit("java-consumer", "consumed versionId=%s dataFormat=%s", resp.SchemaVersionID, resp.DataFormat)
    }
    return resp, err
}
```

**Justification:** The sidecar is accessed purely through its HTTP client methods. Wrapping those methods requires zero production code changes.

### `[go-producer]` and `[go-consumer]`

**Source:** Wrap the `*serializer.Serializer` and `*deserializer.Deserializer` in thin narrating wrappers that log before/after each `Serialize`/`Deserialize` call.

```go
// cmd/demo/internal/runner/narrated_go.go
type NarratedSerializer struct {
    ser      *serializer.Serializer
    enc      *gsrcore.GsrEncoder  // retained for cache observation
    narrator *narrator.Narrator
}

func (n *NarratedSerializer) Serialize(topic string, data interface{}) ([]byte, error) {
    n.narrator.Emit("go-producer", "Serialize(topic=%s, data=%T)", topic, data)
    framed, err := n.ser.Serialize(topic, data)
    if err == nil {
        n.narrator.Emit("go-producer", "produced %d bytes", len(framed))
    }
    return framed, err
}
```

**Justification:** Uses `NewSerializerWithEncoder` / `NewDeserializerWithDecoder` to construct the Serializer/Deserializer while retaining a handle to the `*GsrEncoder` / `*GsrDecoder` for cache observation. No production code changes. This pattern is identical to what `protobuf_pojo_test.go` and `avro_specific_record_test.go` already use.

### `[glue]`

**Source:** AWS SDK v2 middleware on the `realglue.Real` client's underlying `aws.Config`. The middleware is added via `APIOptions` at construction time, before passing the config to `glue.NewFromConfig`.

The demo constructs its own `aws.Config` (using `config.LoadDefaultConfig`) and appends an `InitializeMiddleware` that logs every Glue API call:

```go
cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-2"))
cfg.APIOptions = append(cfg.APIOptions, func(stack *smithymiddleware.Stack) error {
    return stack.Initialize.Add(
        smithymiddleware.InitializeMiddlewareFunc("DemoGlueNarrator",
            func(ctx context.Context, in smithymiddleware.InitializeInput, next smithymiddleware.InitializeHandler) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
                opName := smithymiddleware.GetOperationName(ctx)
                nar.Emit("glue", "%s request", opName)
                out, meta, err := next.HandleInitialize(ctx, in)
                if err != nil {
                    nar.Emit("glue", "%s error: %v", opName, err)
                } else {
                    nar.Emit("glue", "%s success", opName)
                }
                return out, meta, err
            }),
        smithymiddleware.Before,
    )
})
```

The demo then passes this config to `realglue.New(ctx, realglue.WithAWSConfig(cfg))`. Because `realglue.Real` embeds `*glue.Client` and the config carries the middleware, every Glue call made by the core encoder/decoder (which internally uses `glue.NewFromConfig`) will trigger the log.

**Wait:** The core encoder creates its own `glue.Client` from the config map via `NewGsrEncoder(configMap)`. That client does NOT carry our middleware. We need the middleware on the client that the encoder uses, not just on the realglue cleanup client.

**Revised approach:** The demo uses `NewGsrEncoderForTest` (test seam) to construct the encoder with a wrapped `GlueClient`. The `GlueClient` interface in `pkg/gsrserde-go/core` defines the Glue API calls the encoder makes. We wrap the real SDK client in a logging adapter:

```go
type narratedGlueClient struct {
    inner    gsrcore.GlueClient
    narrator *narrator.Narrator
}

func (n *narratedGlueClient) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, ...) (*glue.GetSchemaByDefinitionOutput, error) {
    n.narrator.Emit("glue", "GetSchemaByDefinition schema=%s", aws.ToString(params.SchemaId.SchemaName))
    out, err := n.inner.GetSchemaByDefinition(ctx, params, optFns...)
    // log result
    return out, err
}
// ... same pattern for CreateSchema, RegisterSchemaVersion, GetSchemaVersion, GetSchema, DeleteSchema
```

This approach uses the `gsrcore.GlueClient` interface, which the encoder already accepts via `NewGsrEncoderForTest`. The decoder also accepts a `GlueClient` via `NewGsrDecoderForTest`. The production surface is FROZEN; we use only the test seam constructors, which already exist for exactly this purpose.

**Justification:** `NewGsrEncoderForTest` and `NewGsrDecoderForTest` accept a `GlueClient` interface (defined in `pkg/gsrserde-go/core`). Wrapping that interface is standard Go composition with zero production changes.

### `[cache]`

**Source:** The existing `EncoderCacheHas(enc *GsrEncoder, schemaName, dataFormat string) bool` and `DecoderCacheHas(dec *GsrDecoder, schemaVersionID string) bool` test seams in `pkg/gsrserde-go/core/test_seam.go`.

The demo's NarratedSerializer/NarratedDeserializer hold the raw `*GsrEncoder` / `*GsrDecoder` references (constructed via `NewGsrEncoderForTest` / `NewGsrDecoderForTest`). Before and after each Serialize/Deserialize call, the wrapper polls these seams:

```go
// Before Serialize:
schemaName := topic // DefaultSchemaNameStrategy returns topic
hadBefore := gsrcore.EncoderCacheHas(n.enc, schemaName, dataFormat)

// After Serialize:
hasAfter := gsrcore.EncoderCacheHas(n.enc, schemaName, dataFormat)

if !hadBefore && hasAfter {
    n.narrator.Emit("cache", "miss -> fetched from Glue (schema=%s)", schemaName)
} else if hadBefore && hasAfter {
    n.narrator.Emit("cache", "hit (schema=%s)", schemaName)
} else if hadBefore && !hasAfter {
    n.narrator.Emit("cache", "evicted, refetching from Glue (schema=%s)", schemaName)
}
```

**Justification:** `EncoderCacheHas` / `DecoderCacheHas` are the designated observation seams from Phase 4.x. No production code changes.

### `[kafka]`

**Source:** Wrap the sarama `SyncProducer` and `Consumer` (same libraries used by `consumeOne` / `produceOne` in the integration tests) in narrating wrappers.

```go
type NarratedKafkaProducer struct { ... }
func (n *NarratedKafkaProducer) Produce(topic string, value []byte) (partition int32, offset int64, err error) {
    n.narrator.Emit("kafka", "produce topic=%s size=%d", topic, len(value))
    // send via sarama SyncProducer
    n.narrator.Emit("kafka", "produced partition=%d offset=%d", partition, offset)
}

type NarratedKafkaConsumer struct { ... }
func (n *NarratedKafkaConsumer) ConsumeOne(topic string) ([]byte, error) {
    n.narrator.Emit("kafka", "consume topic=%s partition=0 offset=oldest", topic)
    // consume via sarama
    n.narrator.Emit("kafka", "consumed topic=%s offset=%d size=%d", topic, msg.Offset, len(msg.Value))
}
```

**Justification:** The demo uses sarama directly (same as the integration tests). The narrating wrapper is entirely in demo-internal code.

### `[schema-evolution]`

**Source:** Emitted by the cell runner at the point where it registers v2 after v1. The runner knows it is about to perform a schema evolution step because it holds both `v1Schema` and `v2Schema` and calls the Java sidecar to register v2.

```go
n.narrator.Emit("schema-evolution", "v1 registered")
// ... after v2 registration call ...
n.narrator.Emit("schema-evolution", "v2 registered (BACKWARD-evolution)")
```

**Justification:** This is domain knowledge the cell runner inherently has. No production code involved.

### `[wire-format]`

**Source:** After each Go serialize (framed bytes available) or before each Go deserialize (framed bytes received from Kafka), the wrapper parses the 18-byte header using the constants from `pkg/gsrserde-go/core/wire_format.go`:

- Byte 0: `WireFormatVersionByte` (0x03)
- Byte 1: compression byte (0x00 = NONE, 0x05 = ZLIB)
- Bytes 2-17: schema-version UUID (16 bytes, formatted as 8-4-4-4-12)
- Bytes 18+: payload

```go
// cmd/demo/internal/wireformat/inspect.go
func Inspect(framed []byte) string {
    if len(framed) < gsrcore.WireFormatHeaderSize {
        return "<too short>"
    }
    headerByte := framed[0]
    compressionByte := framed[1]
    uuidBytes := framed[2:18]
    uuid := formatUUID(uuidBytes) // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    payload := framed[18:]
    hexPrefix := hex.EncodeToString(payload[:min(32, len(payload))])
    return fmt.Sprintf("header=0x%02x compression=0x%02x uuid=%s payload[:%d]=%s total=%d",
        headerByte, compressionByte, uuid, min(32, len(payload)), hexPrefix, len(framed))
}
```

The wrapper calls this after every serialization and before every deserialization:
```go
n.narrator.Emit("wire-format", wireformat.Inspect(framed))
```

**Justification:** Reads only the constant definitions (`WireFormatVersionByte`, `CompressionByteNone`, `CompressionByteZlib`, `WireFormatHeaderSize`) from `core/wire_format.go`. These are public constants, not internal logic. No production code changes.

### `[demo]` and `[verdict]`

**Source:** Emitted by the cell runner at scenario boundaries and after each cell completes.

```go
n.narrator.Emit("demo", "=== Cell A.1: Java->Go AVRO_NONE BACKWARD v1->v2 ===")
// ... after cell ...
n.narrator.Emit("verdict", "PASS: produced={id:xver-id name:xver-name age:99} decoded={id:xver-id name:xver-name age:99}")
```

**Justification:** Top-level orchestration code in the cell runner. No production code involved.

## 4. Cell-Runner Core

### Interface

```go
// cmd/demo/internal/runner/runner.go
type CellRunner struct {
    narrator     *narrator.Narrator
    sidecar      *NarratedSidecar
    glueClient   *narratedGlueClient
    broker       *kafkaharness.Broker
    cleanup      *realglue.Cleanup
    region       string
    flags        *flags.DemoFlags
}

func New(opts RunnerOpts) *CellRunner { ... }
func (r *CellRunner) RunAll(ctx context.Context) error { ... }
func (r *CellRunner) RunScenario(ctx context.Context, scenario string) error { ... }
```

### Scenario Matrix

The interop scenario exercises the same 12 cells as `interop_crossversion_kafka_test.go`:

| Cell ID | Direction | Format | Compression |
|---------|-----------|--------|-------------|
| java-go-avro-none | Java->Go | AVRO | NONE |
| java-go-avro-zlib | Java->Go | AVRO | ZLIB |
| java-go-json-none | Java->Go | JSON | NONE |
| java-go-json-zlib | Java->Go | JSON | ZLIB |
| java-go-protobuf-none | Java->Go | PROTOBUF | NONE |
| java-go-protobuf-zlib | Java->Go | PROTOBUF | ZLIB |
| go-java-avro-none | Go->Java | AVRO | NONE |
| go-java-avro-zlib | Go->Java | AVRO | ZLIB |
| go-java-json-none | Go->Java | JSON | NONE |
| go-java-json-zlib | Go->Java | JSON | ZLIB |
| go-java-protobuf-none | Go->Java | PROTOBUF | NONE |
| go-java-protobuf-zlib | Go->Java | PROTOBUF | ZLIB |

### Filtering

Filtering is applied before execution. Each flag acts as a predicate on the matrix:

- `--cell=java-go-avro-zlib` matches only that cell.
- `--format=avro` matches all cells where Format == AVRO.
- `--compression=NONE` matches all cells where Compression == NONE.
- `--direction=java-go` matches all Cell A variants.
- `--scenario=interop|cache|auto-register|all` selects which scenario sets to run.

Predicates are AND-ed: `--format=avro --compression=NONE` yields only `java-go-avro-none` and `go-java-avro-none`.

### Cleanup

Cleanup is deferred at the top of main via `defer cleanup.Run(ctx)`. Signal handlers for SIGINT and SIGTERM also trigger cleanup:

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-sigCh
    nar.Emit("demo", "signal received, cleaning up...")
    cleanup.Run(context.Background())
    os.Exit(1)
}()
```

All schemas use the prefix `gsr-go-demo-` so `cleanup.TrackSchemaPrefix("default-registry", "gsr-go-demo-")` catches everything.

### Execution Order

Cells run sequentially (not parallel) so narration output is readable. Each cell:
1. Emits `[demo]` boundary.
2. Registers schemas, emitting `[glue]` + `[schema-evolution]` lines.
3. Produces, emitting `[java-producer]` or `[go-producer]` + `[kafka]` + `[wire-format]` + `[cache]`.
4. Consumes, emitting `[java-consumer]` or `[go-consumer]` + `[kafka]` + `[wire-format]` + `[cache]`.
5. Validates, emitting `[verdict]`.

## 5. Cache Scenario

The cache scenario runs AFTER the 12-cell interop matrix (or independently via `--scenario=cache`). It uses a single format (AVRO, NONE compression) to keep output focused on cache behavior.

### Sequence

1. Construct a Go serializer with `cacheSize=2` (via `NewGsrEncoderForTest` with `GsrEncoderOptions{CacheSize: 2}`).
2. **First produce (schema-A):** `EncoderCacheHas` returns false before, true after.
   - Emits: `[cache] miss -> fetched from Glue (schema=gsr-go-demo-cache-A)`
   - Emits: `[glue] GetSchemaByDefinition ...`
3. **Second produce (same schema-A):** `EncoderCacheHas` returns true before and after.
   - Emits: `[cache] hit (schema=gsr-go-demo-cache-A)`
   - No `[glue]` line (cache served it).
4. **Produce schema-B and schema-C** (filling the 2-entry cache, evicting schema-A):
   - After producing schema-C: `EncoderCacheHas(enc, "gsr-go-demo-cache-A", "AVRO")` returns false.
   - Emits: `[cache] evicted, refetching from Glue (schema=gsr-go-demo-cache-A)`
5. **Third produce (schema-A again):** cache miss, triggers Glue refetch.
   - Emits: `[cache] miss -> fetched from Glue (schema=gsr-go-demo-cache-A)`
   - Emits: `[glue] GetSchemaByDefinition ...`

### How eviction is triggered

By setting `CacheSize: 2` on the encoder options and producing 3 distinct schemas (A, B, C). The LRU cache evicts schema-A (oldest) when schema-C arrives. This approach uses only the existing `CacheSize` configuration option and the `EncoderCacheHas` observation seam. No production code changes.

## 6. Auto-Register Fall-Through Scenario

The auto-register scenario demonstrates the Phase 4.14 `EntityNotFoundException -> CreateSchema` fall-through path. It runs independently via `--scenario=auto-register` or as part of `--scenario=all`.

### Sequence

1. Choose a schema name that has never been registered: `gsr-go-demo-autoregister-<uuid>`.
2. Construct Go serializer with `schemaAutoRegistrationEnabled: true`.
3. Serialize the record. The encoder calls:
   - `GetSchemaByDefinition` -> 404 `EntityNotFoundException`
   - Falls through to `CreateSchema` (auto-register path)
   - `RegisterSchemaVersion`
4. The narrated GlueClient wrapper logs each call:
   - `[glue] GetSchemaByDefinition schema=gsr-go-demo-autoregister-xxx -> EntityNotFoundException`
   - `[glue] CreateSchema schema=gsr-go-demo-autoregister-xxx`
   - `[glue] RegisterSchemaVersion schema=gsr-go-demo-autoregister-xxx`
5. Emit verdict: `[verdict] PASS: auto-register fall-through exercised`

### Cleanup

The auto-registered schema is tracked via `cleanup.TrackSchema("default-registry", schemaName)`.

## 7. Same-Version Baseline Scenario

Demonstrates the Phase 4.6.5 same-version interop still works (no evolution, single schema version).

### Sequence

1. Register one schema version via Java sidecar (AVRO, NONE compression).
2. **Java produces, Go consumes:** `[java-producer]` -> `[kafka]` -> `[go-consumer]` -> `[wire-format]` -> `[verdict]`.
3. **Go produces, Java consumes:** `[go-producer]` -> `[kafka]` -> `[java-consumer]` -> `[wire-format]` -> `[verdict]`.
4. No v2 registration, no schema evolution.

Emit: `[verdict] PASS: same-version baseline (Java->Go + Go->Java)`

## 8. CLI Flag Parsing

Uses the `flag` standard library package. All flags are optional.

| Flag | Type | Default | Behavior |
|------|------|---------|----------|
| `--cell` | string | `""` (all) | Filter to a specific cell ID (e.g., `java-go-avro-zlib`). Case-insensitive. If the value does not match any known cell, exit 1 with the list of valid cell IDs. |
| `--format` | string | `""` (all) | Filter by format: `avro`, `json`, or `protobuf`. Case-insensitive. |
| `--compression` | string | `""` (all) | Filter by compression: `NONE` or `ZLIB`. Case-insensitive. |
| `--direction` | string | `""` (all) | Filter by direction: `java-go` or `go-java`. Case-insensitive. |
| `--scenario` | string | `"all"` | Scenario set: `interop` (12 cells only), `cache` (cache demo only), `auto-register` (auto-register only), `all` (interop + cache + auto-register + baseline). |
| `--pretty` | bool | `true` | When true, record values are pretty-printed (multiline JSON). When false, compact one-liners. |

Parsing happens in `cmd/demo/internal/flags/flags.go`:

```go
func Parse() *DemoFlags {
    f := &DemoFlags{}
    flag.StringVar(&f.Cell, "cell", "", "filter to a specific cell ID")
    flag.StringVar(&f.Format, "format", "", "filter by format (avro|json|protobuf)")
    flag.StringVar(&f.Compression, "compression", "", "filter by compression (NONE|ZLIB)")
    flag.StringVar(&f.Direction, "direction", "", "filter by direction (java-go|go-java)")
    flag.StringVar(&f.Scenario, "scenario", "all", "scenario set (interop|cache|auto-register|all)")
    flag.BoolVar(&f.Pretty, "pretty", true, "pretty-print payload values")
    flag.Parse()
    return f
}
```

## 9. Gating Logic

At the top of `main()`, before any resource allocation:

```go
if os.Getenv("AWS_INTEGRATION") != "1" || strings.ToLower(os.Getenv("GSR_GLUE")) != "real" {
    fmt.Fprintln(os.Stderr, "demo requires real AWS credentials and Docker; see README")
    os.Exit(1)
}
```

Exit message is verbatim: `demo requires real AWS credentials and Docker; see README`

Exit code: 1.

## 10. Cleanup

`realglue.Cleanup` handles all Glue resource teardown. The demo wires it as follows:

```go
real, err := realglue.New(ctx)
// ... error handling ...
cleanup := real.NewCleanup()
cleanup.TrackSchemaPrefix("default-registry", "gsr-go-demo-")

// Deferred cleanup (normal exit)
defer func() {
    cleanCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    if err := cleanup.Run(cleanCtx); err != nil {
        nar.Emit("demo", "cleanup error: %v", err)
    }
}()

// Signal handler (Ctrl-C / SIGTERM)
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
go func() {
    sig := <-sigCh
    nar.Emit("demo", "received %v, running cleanup...", sig)
    cleanCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    _ = cleanup.Run(cleanCtx)
    os.Exit(1)
}()
```

The Kafka broker container is torn down via `defer stop()` from `kafkaharness.StartShared`. The Java sidecar is torn down via `defer sc.Stop(ctx)`.

## 11. Makefile Target

Add to `/workplace/mrknox/phase-4.15/native-schema-registry/golang/Makefile`:

```makefile
# Demo — narrated cross-language interop showcase.
# Requires: Go, Maven (for sidecar JAR), Docker, AWS creds for 850995546034.
JAVA_SIDECAR_JAR := integration-tests/java-interop/target/java-interop-sidecar.jar

.PHONY: java-sidecar-jar
java-sidecar-jar:
	@if [ ! -f $(JAVA_SIDECAR_JAR) ]; then \
		echo "Building Java sidecar JAR..."; \
		cd integration-tests/java-interop && mvn -q package -DskipTests; \
	fi

.PHONY: demo
demo: java-sidecar-jar generate-protos
	@echo "Running GSR Go client demo (real AWS, Docker required)..."
	cd integration-tests && AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local \
		$(GO) run -tags integration ./cmd/demo $(DEMO_ARGS)

.PHONY: demo-cell
demo-cell: java-sidecar-jar generate-protos
	@if [ -z "$(CELL)" ]; then echo "Usage: make demo-cell CELL=java-go-avro-none"; exit 1; fi
	cd integration-tests && AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local \
		$(GO) run -tags integration ./cmd/demo --cell=$(CELL)
```

### Steps in `make demo`

1. Check if `integration-tests/java-interop/target/java-interop-sidecar.jar` exists. If not, run `mvn -q package -DskipTests` in that directory.
2. Run `generate-protos` (existing target, needed for protobuf test schemas).
3. `go run -tags integration ./cmd/demo` from within the `integration-tests/` directory, with `AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local`.
4. Optional `DEMO_ARGS` pass-through for extra flags (e.g., `make demo DEMO_ARGS="--scenario=cache"`).

## 12. README Section

Add a section to `/workplace/mrknox/phase-4.15/native-schema-registry/golang/README.md` titled "Live demo":

```markdown
## Live demo

The demo binary exercises the full Go GSR client against a real AWS Glue registry
and a Kafka broker (via testcontainers-go), narrating every operation in real time.
Java and Go take turns producing and consuming through the same topic and registry,
across schema versions.

**Prerequisites:** Go 1.25+, Maven 3.x (for the Java sidecar JAR), Docker,
AWS credentials for account 850995546034 with Glue permissions.

```bash
make demo
```

To run a single cell:
```bash
make demo-cell CELL=java-go-avro-none
```

Sample output (first 20 lines):
```
[demo] === Cell A.1: Java->Go AVRO_NONE BACKWARD v1->v2 ===
[glue] GetSchemaByDefinition schema=gsr-go-demo-xver-avro-... -> EntityNotFoundException
[glue] CreateSchema schema=gsr-go-demo-xver-avro-...
[schema-evolution] v1 registered
[java-producer] Serialize(topic=gsr-go-demo-xver-..., format=AVRO, schema=gsr-go-demo-xver-avro-...)
[kafka] produce topic=gsr-go-demo-xver-... size=82
[java-producer] produced offset=0 partition=0 versionId=abc12345-...
[schema-evolution] v2 registered (BACKWARD-evolution)
[kafka] consume topic=gsr-go-demo-xver-... partition=0 offset=oldest
[kafka] consumed topic=gsr-go-demo-xver-... offset=0 size=82
[wire-format] header=0x03 compression=0x00 uuid=abc12345-1234-5678-9abc-def012345678 payload[:32]=0a0e... total=82
[go-consumer] Deserialize(topic=gsr-go-demo-xver-...)
[cache] miss -> fetched from Glue (schema=gsr-go-demo-xver-...)
[glue] GetSchemaVersion versionId=abc12345-...
[go-consumer] deserialized map[id:xver-id name:xver-name age:99]
[verdict] PASS: produced={id:xver-id name:xver-name age:99} decoded={id:xver-id name:xver-name age:99}
```

See `test-artifacts/4.15-demo-output.log` for a full captured run.
```

The sample output snippet is a placeholder; the final README will contain the REAL captured output (first 20 lines from the captured log).

## 13. Test Artifacts

The captured demo output is committed at:

```
/workplace/mrknox/phase-4.15/native-schema-registry/golang/test-artifacts/4.15-demo-output.log
```

Format: raw terminal output from a full `make demo` run (all 12 interop cells + cache scenario + auto-register scenario + baseline scenario). Each line starts with a `[tag]` prefix. The file is captured via:

```bash
make demo 2>&1 | tee test-artifacts/4.15-demo-output.log
```

The README links to this file for the full output reference.

## 14. Acceptance Criteria

- [ ] **AC-1:** Binary at `native-schema-registry/golang/cmd/demo/main.go` builds via `go build -tags integration` from the integration-tests module. Reuses (does not reimplement): `pkg/javasidecar/`, `pkg/kafkaharness/`, `pkg/realglue/`, `pkg/gsrserde-go/serializer`, `pkg/gsrserde-go/deserializer`. NO new production code paths; NO duplication of test logic.
- [ ] **AC-2:** Live narrated log output with all 11 required tags (`[java-producer]`, `[java-consumer]`, `[go-producer]`, `[go-consumer]`, `[glue]`, `[cache]`, `[kafka]`, `[schema-evolution]`, `[wire-format]`, `[demo]`, `[verdict]`). Tags are lowercase, square-bracketed, verbatim.
- [ ] **AC-3:** Exercises all 12 cross-language cross-version cells by default. Also exercises: cache behavior (hit + eviction + refetch), auto-register fall-through (EntityNotFoundException -> CreateSchema), same-version baseline (Phase 4.6.5 path).
- [ ] **AC-4:** CLI flags: `--cell`, `--format`, `--compression`, `--direction`, `--scenario`, `--pretty`. All optional with documented defaults.
- [ ] **AC-5:** Gated by `AWS_INTEGRATION=1 GSR_GLUE=real`. Without them, prints "demo requires real AWS credentials and Docker; see README" and exits 1. With them, runs end-to-end. Cleanup via `realglue.Cleanup` on exit (deferred + signal handlers for Ctrl-C).
- [ ] **AC-6:** Makefile target `make demo` at `native-schema-registry/golang/Makefile`. Builds Java sidecar JAR if missing. Optional `make demo-cell CELL=...`.
- [ ] **AC-7:** README section at `native-schema-registry/golang/README.md` titled "Live demo" with prerequisites, command, and sample output snippet (~20 lines, REAL captured output).
- [ ] **AC-8:** All test suites GREEN with `-race` post-merge: inner core, outer module, integration-tests (no tag + with-integration-tag build). Demo binary itself has no test suite, but any new packages under `cmd/demo/internal/` get Tier-1 tests.
- [ ] **AC-9:** Demo run captured in `native-schema-registry/golang/test-artifacts/4.15-demo-output.log` showing all 12 cells PASS + cache + auto-register + baseline scenarios, real Glue in 850995546034. Committed on `phase-4.15`.

## 15. Production-Code Changes (REQUIRES PM APPROVAL)

None.

All observability is achieved through:
- Wrapping the `GlueClient` interface (already a public interface in `pkg/gsrserde-go/core`)
- Using `NewGsrEncoderForTest` / `NewGsrDecoderForTest` test-seam constructors
- Using `EncoderCacheHas` / `DecoderCacheHas` public test seams
- Wrapping the `*javasidecar.Sidecar` methods
- Wrapping sarama Kafka producer/consumer
- Reading public wire-format constants (`WireFormatVersionByte`, `CompressionByteNone`, `CompressionByteZlib`, `WireFormatHeaderSize`)

No hooks, loggers, or middleware need to be added inside `pkg/gsrserde-go/`.

## Constraints

- MUST NOT modify any file under `pkg/gsrserde-go/` (production surface FROZEN).
- MUST NOT introduce new Go modules or break the existing module structure.
- MUST NOT duplicate logic from the integration test files; MUST reuse the existing patterns (schema constants, record builders, config builders).
- MUST emit narration starting with the first actor operation (no preamble narration).
- Tag names MUST be verbatim per AC-2 (lowercase, square-bracketed, no aliases).
- MUST NOT create a PR, push to remote, or create a CR. Local commits only on `phase-4.15`.
- The binary MUST be buildable ONLY with the `integration` build tag (it depends on testcontainers-go and real-AWS infrastructure).
- Cells MUST run sequentially for readable narration output.

## Definition of Done

1. `cd integration-tests && go build -tags integration ./cmd/demo` succeeds without errors.
2. `make demo` runs end-to-end on the dev host with Docker and AWS creds, printing all 11 tag types.
3. `make demo-cell CELL=java-go-avro-none` runs only that one cell.
4. `make demo DEMO_ARGS="--scenario=cache"` runs only the cache scenario.
5. `test-artifacts/4.15-demo-output.log` exists, committed, contains 12 PASS verdicts + cache + auto-register + baseline.
6. `go test -race ./...` passes for all three module scopes (core, outer, integration-tests).
7. No files under `pkg/gsrserde-go/` are modified on the `phase-4.15` branch relative to `golang-mrknox-4x-integrated`.

## Regression Guardrails

- **INV-FROZEN-SURFACE:** `git diff golang-mrknox-4x-integrated -- pkg/gsrserde-go/` MUST produce empty output on the `phase-4.15` branch.
- **INV-EXISTING-TESTS:** `cd integration-tests && go test -tags integration -race -count=1 ./...` MUST pass with `AWS_INTEGRATION=1 GSR_GLUE=real` (existing Phase 4.13/4.14 tests still green).
- **INV-UNIT-TESTS:** `go test -race -count=1 ./...` (from the outer module root, excluding integration-tests) MUST pass.
- **INV-NO-NEW-DEPS:** The integration-tests `go.mod` MUST NOT gain new direct dependencies beyond what already exists (sarama, testcontainers-go, AWS SDK, etc.). The demo uses only existing imports.

## Scenarios

```gherkin
Feature: Demo binary narrated output

  Scenario: Full default run
    Given AWS_INTEGRATION=1 and GSR_GLUE=real are set
    And Docker is running
    And the Java sidecar JAR exists
    When the user runs "make demo"
    Then the binary starts without preamble narration
    And 12 interop cells execute sequentially
    And each cell emits [demo], [glue], [java-producer] or [go-producer], [kafka], [wire-format], [cache], [java-consumer] or [go-consumer], [verdict] tags
    And the cache scenario emits at least one [cache] hit and one [cache] evicted line
    And the auto-register scenario emits [glue] GetSchemaByDefinition -> EntityNotFoundException followed by [glue] CreateSchema
    And the baseline scenario emits [verdict] PASS for both directions
    And all 12 cells emit [verdict] PASS
    And cleanup deletes all gsr-go-demo-* schemas from Glue

  Scenario: Missing env vars
    Given AWS_INTEGRATION is unset or GSR_GLUE != "real"
    When the user runs the demo binary
    Then it prints "demo requires real AWS credentials and Docker; see README" to stderr
    And exits with code 1

  Scenario: Cell filter
    Given the environment is configured correctly
    When the user runs "make demo-cell CELL=go-java-json-zlib"
    Then only the go-java-json-zlib cell executes
    And only one [verdict] line appears in output

  Scenario: Ctrl-C during execution
    Given the demo is running
    When the user sends SIGINT
    Then the demo emits [demo] signal received
    And runs realglue.Cleanup
    And exits with code 1
```
