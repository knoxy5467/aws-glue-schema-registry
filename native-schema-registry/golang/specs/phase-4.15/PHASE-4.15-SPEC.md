# Phase 4.15 — Demo Binary: Narrated Java-Go Cross-Language Cross-Version Interop

## 1. Context

Phases 4.10 through 4.14 brought the Go GSR client to wire-format and behavioral parity with the Java reference implementation. Phase 4.6.5 proved cross-language same-version interop. Phase 4.12 proved cross-version within Go. Phase 4.13 proved cross-language cross-version interop via automated tests. What is missing is a human-readable narration that a reviewer or customer can read top-to-bottom and understand exactly what happens at each step: what schemas are registered, what bytes go on the wire, what UUIDs are assigned, and what values decode on the other side.

This phase delivers a standalone demo binary (Go) that orchestrates the existing Java sidecar and the Go client library to produce a verbose, narrated transcript showing all three data formats (Avro, JSON Schema, Protobuf) in both cross-language directions, with schema evolution (v1 to v2) exercised across language boundaries.

## 2. Goals and Non-Goals

### Goals

- Produce a single Go binary (`cmd/demo-interop/main.go`) that emits a narrated transcript to stdout.
- Cover all 3 formats (Avro, JSON Schema, Protobuf) in both directions (Java-to-Go, Go-to-Java) with cross-version evolution (v1 producer, v2 registered, consumer verifies v1 fields).
- Narrate at each step: registry name, schema name, schema body, Glue version-id, Go config applied, wire-byte hex dump decomposed into header/compression/UUID/payload, deserialized value field-by-field.
- Run against real AWS Glue in account `850995546034`, region `us-east-2`.
- Exit 0 on full success, non-zero on any failure, with structured error reporting.
- Clean up all Glue schemas on exit (success or failure).
- Complete in under 5 minutes on a Cloud Desktop with warm JVM and Kafka container.

### Non-Goals

- Do NOT add new tests to the existing integration test suite (that was phases 4.10-4.14).
- Do NOT change the Go client public API surface in `pkg/gsrserde-go/`.
- Do NOT modify the Java sidecar source code (it is already sufficient as-is from Phase 4.13).
- Do NOT introduce new AWS service dependencies (no new KMS keys, no new S3 buckets).
- Do NOT build a canary, production deployment, UI, or doc-site change.
- Do NOT produce an asciinema cast automatically (the transcript is designed to be captured manually via `tee` or piped to a file).

## 3. Architecture

### 3.1 Demo Architecture Decision: Go Binary + Existing Java Sidecar

The demo reuses the existing Java sidecar (`integration-tests/java-interop/`) that phases 4.6.5 and 4.13 built and validated. The Go demo binary starts the sidecar via the `javasidecar` package (same `StartForTest`-like mechanism, adapted for a non-test `main()` context), then drives both languages through the sidecar's HTTP API and the Go client's public serializer/deserializer API.

**Justification:** The Java sidecar already exposes `/kafka-produce` and `/kafka-consume` endpoints that exercise the full `GlueSchemaRegistryKafkaSerializer.serialize(topic, record)` and `GlueSchemaRegistryKafkaDeserializer.deserialize(topic, bytes)` paths. Reusing this avoids building a separate Java binary, keeps the integration surface identical to what the tests already validate, and lets the demo inherit the same compatibility/region/format support.

**Alternative rejected:** A pair of standalone binaries communicating via shared files was considered but rejected because it would require duplicating the Java sidecar's schema-registration logic in a new entry point and would not exercise the same code path customers use.

### 3.2 File Layout

```
native-schema-registry/golang/integration-tests/
├── cmd/
│   └── demo-interop/
│       ├── main.go              # Entry point, orchestrates demo flow
│       ├── narrator.go          # Narration formatting helpers (section headers, hex dump, etc.)
│       ├── scenarios.go         # Per-format scenario definitions (schemas, records, checks)
│       └── README.md            # One-screen build/run instructions
```

The demo binary lives in the `integration-tests` module (not the outer or core module) because it depends on `javasidecar`, `kafkaharness`, and `realglue` which are all defined in that module. The entry point is `integration-tests/cmd/demo-interop/main.go`.

It imports:
- `integration-tests/pkg/javasidecar` (sidecar lifecycle and HTTP client)
- `integration-tests/pkg/kafkaharness` (testcontainers-go Kafka broker via `StartShared`)
- `integration-tests/pkg/realglue` (cleanup)
- `pkg/gsrserde-go/serializer` (Go serialization)
- `pkg/gsrserde-go/deserializer` (Go deserialization)
- `pkg/gsrserde-go/common` (configuration)
- `pkg/gsrserde-go/avro` (AvroRecord type)
- `pkg/gsrserde-go/serializer/json` (JsonDataWithSchema type)

### 3.3 Kafka and Sidecar Startup from `main()`

The integration test helpers `kafkaharness.Start()` and `javasidecar.StartForTest()` both require `testing.TB` and cannot be called from a `main` package. The demo uses their non-test counterparts instead:

- **Kafka:** `kafkaharness.StartShared(ctx)` returns `(*Broker, func() error, error)` without requiring `testing.TB`. The demo calls this directly and defers the returned stop function. Source: `pkg/kafkaharness/kafka_container.go` line 95.
- **Sidecar:** `javasidecar.New(ctx, opts)` returns `(*Sidecar, error)` without requiring `testing.TB`. The demo calls this directly and defers `sc.Stop(ctx)`. Source: `pkg/javasidecar/sidecar.go` line 165.

Both functions have the same `KAFKA_BROKER` / `GSR_INTEROP_MODE` env var short-circuit behavior as their test-oriented wrappers.

### 3.4 Kafka Dependency

The demo uses testcontainers-go for Kafka (same as integration tests). The Java sidecar produces/consumes via Kafka, which is the same path the real customer API exercises. The demo does NOT communicate with the sidecar via raw bytes or files; it uses the actual Kafka transport to prove end-to-end wire-format parity.

### 3.5 Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Demo binary (Go)                                                 │
│                                                                   │
│  ┌─────────────────────┐     ┌──────────────────────────────┐   │
│  │ Go Serializer        │     │ Go Deserializer              │   │
│  │ (narrates config,    │     │ (narrates schema-version-id, │   │
│  │  hex dump, UUID)     │     │  decoded fields, equality)   │   │
│  └──────────┬───────────┘     └──────────────┬───────────────┘   │
│             │ Go→Java path                   ▲ Java→Go path      │
│             ▼                                │                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │             Kafka (testcontainers-go)                       │  │
│  └────────────────────────────────────────────────────────────┘  │
│             ▲                                │                    │
│             │ Java→Go path                   ▼ Go→Java path      │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Java Sidecar (integration-tests/java-interop/)            │  │
│  │  /kafka-produce (register + serialize + produce)           │  │
│  │  /kafka-consume (poll + deserialize + report)              │  │
│  └────────────────────────────────────────────────────────────┘  │
│             │                                                     │
│             ▼                                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Real AWS Glue Schema Registry                             │  │
│  │  Account 850995546034, us-east-2, default-registry         │  │
│  └────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

## 4. Demo Flow Per Format

Each format runs the following sequence. The narration output is interleaved with execution.

### 4.1 Shared Prefix

Before any per-format scenario runs, the demo narrates startup:

```
================================================================================
  GSR Go Client — Java↔Go Cross-Language Cross-Version Interop Demo
  Date: <timestamp>
  Account: 850995546034
  Region: us-east-2
  Registry: default-registry
================================================================================

[STARTUP] Java sidecar starting (local mode)...
[STARTUP] Java sidecar ready at http://127.0.0.1:<port>
[STARTUP] Kafka broker starting (testcontainers-go)...
[STARTUP] Kafka broker ready at <bootstrap>
```

### 4.2 Per-Format Flow (repeated for Avro, JSON Schema, Protobuf)

Each format section has two directions. The order within a format is:

1. **Direction A: Java produces at v1, Go consumes (with v2 registered)**
2. **Direction B: Go produces at v1, Java consumes (with v2 registered)**

#### Direction A: Java→Go with Cross-Version

```
────────────────────────────────────────────────────────────────────────────────
  FORMAT: AVRO | Direction A: Java produces at v1, Go consumes
────────────────────────────────────────────────────────────────────────────────

[SCHEMA-V1] Registering schema v1 via Java sidecar...
  Schema name: demo-4.15-avro-<uuid8>
  Registry:    default-registry
  Compatibility: BACKWARD
  Schema body:
    {
      "type": "record",
      "name": "CrossVersionRecord",
      ...
    }

[SCHEMA-V1] Glue schema-version-id: <uuid>

[SCHEMA-V2] Registering schema v2 (evolution: added optional "email" field)...
  Schema name: demo-4.15-avro-<uuid8> (same schema, new version)
  Schema body:
    {
      "type": "record",
      ...
      {"name": "email", "type": ["null", "string"], "default": null}
    }

[SCHEMA-V2] Glue schema-version-id: <uuid>

[JAVA-PRODUCE] Java sidecar producing v1 record to Kafka...
  Topic:       demo-4.15-avro-a-<uuid8>
  Record:      {"id": "demo-id", "name": "demo-name", "age": 42}
  Compression: NONE

[JAVA-PRODUCE] Produced. Framed bytes (hex):
  03 00 <16-byte-uuid> <payload-bytes...>
  ├── Header byte:      0x03 (GSR v3 wire format)
  ├── Compression byte: 0x00 (NONE)
  ├── Schema version UUID: <formatted-uuid>
  └── Payload body: <remaining-hex> (<N> bytes)

[GO-CONSUME] Go deserializer consuming from Kafka topic...
  Go config:
    region:                      us-east-2
    registry.name:               default-registry
    compression:                 NONE
    schemaAutoRegistrationEnabled: true
    dataFormat:                   AVRO
    avroRecordType:              GENERIC_RECORD

[GO-CONSUME] Deserialized successfully.
  Schema version retrieved: <uuid> (v1)
  Decoded Go value:
    map[string]interface{}{
      "id":   "demo-id"    (string)
      "name": "demo-name"  (string)
      "age":  42           (int)
    }

[VERIFY] Equality check:
  id:   "demo-id"   == "demo-id"   ✓
  name: "demo-name" == "demo-name" ✓
  age:  42          == 42          ✓
  PASS — Java v1 → Go decode (v2 registered) succeeded.
```

#### Direction B: Go→Java with Cross-Version

```
────────────────────────────────────────────────────────────────────────────────
  FORMAT: AVRO | Direction B: Go produces at v1, Java consumes
────────────────────────────────────────────────────────────────────────────────

[SCHEMA-SETUP] v1 and v2 already registered from Direction A (reusing schema name).

[GO-PRODUCE] Go serializer producing v1 record...
  Go config:
    region:                      us-east-2
    registry.name:               default-registry
    compression:                 NONE
    schemaAutoRegistrationEnabled: true
    dataFormat:                   AVRO
    avroRecordType:              GENERIC_RECORD
  Record: AvroRecord{Schema: <v1>, Data: {"id": "demo-id", "name": "demo-name", "age": 42}}

[GO-PRODUCE] Serialized. Framed bytes (hex):
  03 00 <16-byte-uuid> <payload-bytes...>
  ├── Header byte:      0x03 (GSR v3 wire format)
  ├── Compression byte: 0x00 (NONE)
  ├── Schema version UUID: <formatted-uuid>
  └── Payload body: <remaining-hex> (<N> bytes)

[JAVA-CONSUME] Java sidecar consuming from Kafka topic...
  Topic: demo-4.15-avro-b-<uuid8>

[JAVA-CONSUME] Deserialized by Java.
  Java envelope: {"fields": {"id": "demo-id", "name": "demo-name", "age": 42}}
  schemaVersionId: <uuid>
  dataFormat: AVRO

[VERIFY] Equality check:
  id:   "demo-id"   == "demo-id"   ✓
  name: "demo-name" == "demo-name" ✓
  age:  42          == 42          ✓
  PASS — Go v1 → Java decode (v2 registered) succeeded.
```

### 4.3 Format-Specific Notes

**Avro:** Uses `AvroRecord{Schema, Data}` on the Go side. Java record envelope is `{"fields": {...}}`. Evolution: add nullable `email` field with default null.

**JSON Schema:** Uses `JsonDataWithSchema{Schema, Payload}` on the Go side. Java record envelope is `{"schema": "...", "payload": "..."}`. Evolution: add non-required nullable `email` property.

**Protobuf:** Uses `dynamicpb.Message` built from the v1 schema definition string (same pattern as Phase 4.13 `buildDynamicProtoMessage`). Java record envelope is `{"messageTypeFullName": "test.CrossVersionMessage", "fieldsJson": "..."}`. Evolution: add `string email = 4` field.

### 4.4 Summary Section

After all 6 scenario pairs complete:

```
================================================================================
  DEMO RESULTS
================================================================================

  Format        Direction A (Java→Go)  Direction B (Go→Java)
  ──────────    ─────────────────────  ─────────────────────
  AVRO          PASS                   PASS
  JSON Schema   PASS                   PASS
  Protobuf      PASS                   PASS

  Total: 6/6 PASS

[CLEANUP] Deleting demo schemas from Glue...
  Deleted: demo-4.15-avro-<uuid8>
  Deleted: demo-4.15-json-<uuid8>
  Deleted: demo-4.15-proto-<uuid8>
  ...

[DONE] Demo completed successfully. Exit code: 0
================================================================================
```

## 5. Schema Fixtures

The demo reuses the exact same v1/v2 schema pairs proven in Phase 4.13's integration tests. These are defined in `integration-tests/tests/interop_crossversion_kafka_test.go` at lines 73-139.

### 5.1 Avro

**v1** (source: `crossVersionAvroV1`, line 73):
```json
{
  "type": "record",
  "name": "CrossVersionRecord",
  "namespace": "test",
  "fields": [
    {"name": "id",   "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "age",  "type": "int"}
  ]
}
```

**v2** (source: `crossVersionAvroV2`, line 84):
```json
{
  "type": "record",
  "name": "CrossVersionRecord",
  "namespace": "test",
  "fields": [
    {"name": "id",    "type": "string"},
    {"name": "name",  "type": "string"},
    {"name": "age",   "type": "int"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

### 5.2 JSON Schema

**v1** (source: `crossVersionJSONV1`, line 96):
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id":   {"type": "string"},
    "name": {"type": "string"},
    "age":  {"type": "integer"}
  },
  "required": ["id", "name", "age"],
  "additionalProperties": false
}
```

**v2** (source: `crossVersionJSONV2`, line 108):
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id":    {"type": "string"},
    "name":  {"type": "string"},
    "age":   {"type": "integer"},
    "email": {"type": ["string", "null"]}
  },
  "required": ["id", "name", "age"],
  "additionalProperties": false
}
```

### 5.3 Protobuf

**v1** (source: `crossVersionProtoV1`, line 121):
```proto
syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
```

**v2** (source: `crossVersionProtoV2`, line 131):
```proto
syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
```

### 5.4 Demo Record Values

All formats use the same logical record:
- `id`: `"demo-id"`
- `name`: `"demo-name"`
- `age`: `42`

These are distinct from the Phase 4.13 test values (`xverID="xver-id"`, `xverName="xver-name"`, `xverAge=99`) so any confusion between test and demo output is immediately visible.

### 5.5 Fixture Accessibility

The schema string constants (`crossVersionAvroV1`, `crossVersionAvroV2`, `crossVersionJSONV1`, `crossVersionJSONV2`, `crossVersionProtoV1`, `crossVersionProtoV2`) and the `buildDynamicProtoMessage` helper function are defined in `integration-tests/tests/interop_crossversion_kafka_test.go`, which is a `_test.go` file in package `integration_tests`. Go does not allow importing symbols from `_test.go` files into a `main` package.

**Committed approach:** The demo package (`cmd/demo-interop/scenarios.go`) re-declares the schema string constants as package-level `const` values. The values are byte-for-byte identical to those in `interop_crossversion_kafka_test.go` lines 73-139. They are re-declared (not imported) because Go cannot import `_test.go` symbols from a `main` package. A code comment in `scenarios.go` cites the source file and line range so future maintainers know the canonical source of truth.

Similarly, the `buildDynamicProtoMessage` logic (approximately 50 lines of straightforward proto parsing using `jhump/protoreflect/desc/protoparse` and `dynamicpb`) is copied into the demo package as a plain function with signature `func buildDynamicProtoMessage(schemaDef string, fields map[string]interface{}) (*dynamicpb.Message, error)`. The copy drops the `testing.T` dependency that the test version never had anyway (the test version already uses this exact signature, see `interop_crossversion_kafka_test.go` lines 387-439). A code comment cites the source.

Extracting these into a shared non-test helper package (e.g., `pkg/testfixtures/`) is out of scope for this demo phase. The demo is a standalone demonstration tool, not production code, and duplicating a few string constants and one small utility function is acceptable for that purpose. If a future phase needs to share these fixtures across multiple consumers, that refactoring can happen then.

## 6. Configuration

### 6.1 Environment Variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `AWS_PROFILE` or `AWS_ACCESS_KEY_ID` | One of | - | AWS credentials |
| `AWS_REGION` | No | `us-east-2` | Glue API region |
| `GSR_INTEROP_JAVA` | No | `java` | Path to JDK binary |
| `DEMO_REGISTRY` | No | `default-registry` | Glue registry name |
| `DEMO_PREFIX` | No | `demo-4.15` | Schema name prefix |

### 6.2 Go Client Config Applied

For each format, the demo constructs `common.Configuration` with:

```go
gsrMap := map[string]string{
    "region":                        region,        // us-east-2
    "registry.name":                 registryName,  // default-registry
    "compression":                   "NONE",
    "schemaAutoRegistrationEnabled": "true",
}
```

Plus per-format keys:
- **Avro:** `DataFormatTypeKey=DataFormatAvro`, `AvroRecordTypeKey=AvroRecordTypeGeneric`
- **JSON:** `DataFormatTypeKey=DataFormatJSON`
- **Protobuf:** `DataFormatTypeKey=DataFormatProtobuf`, `ProtobufMessageDescriptorKey=<dynamicpb descriptor from v1 schema text>`

These config values match what the integration tests use (see `buildGoConfigCellA` and `buildGoConfigCellB` in `interop_crossversion_kafka_test.go`, lines 463-491 and 606-634).

### 6.3 Schema Naming Convention

Schema names follow the pattern: `demo-4.15-<format>-<8-hex-chars>` where:
- `format` is one of `avro`, `json`, `proto`
- `8-hex-chars` is a random suffix generated at demo start (shared across directions for a given format, so Direction B can reuse Direction A's registered schemas)

Topic names follow the pattern: `demo-4.15-<format>-<direction>-<8-hex-chars>` where:
- `format` is one of `avro`, `json`, `proto`
- `direction` is `a` (Java-to-Go) or `b` (Go-to-Java)

A separate throwaway topic for v2 registration uses: `demo-4.15-<format>-reg-<8-hex-chars>`

## 7. Java Sidecar Contract

The demo reuses the existing Java sidecar exactly as-is. No new endpoints or modifications are needed. Phase 4.13 already added the `compatibility` field to `/kafka-produce` (see `KafkaProduceHandler.java` line 79 and `sidecar.go` line 451).

### 7.1 Endpoints Used

| Endpoint | Request Shape | Purpose in Demo |
|----------|--------------|-----------------|
| `POST /kafka-produce` | `{format, schema, schemaName, record, compression, bootstrap, topic, region, compatibility}` | Register schema + produce framed record to Kafka |
| `POST /kafka-consume` | `{bootstrap, topic, format, region, timeoutMs}` | Poll Kafka + deserialize framed record |
| `GET /health` | - | Readiness check |

### 7.2 Go Client Interface

The demo uses the sidecar via the `javasidecar.Sidecar` struct:
- `javasidecar.New(ctx, opts)` to start (source: `sidecar.go` line 165)
- `sc.KafkaProduce(ctx, req)` to drive Java serialization (line 520)
- `sc.KafkaConsume(ctx, req)` to drive Java deserialization (line 568)
- `sc.Stop(ctx)` for cleanup (line 397)

The sidecar is started in local mode (JVM process, not container) because the demo runs on a developer Cloud Desktop where `java` is on PATH.

### 7.3 Narrating Java-Side Output

The Java sidecar returns:
- From `/kafka-produce`: `{schemaVersionId, bytes (base64), offset, partition}` (line 543-561 of `sidecar.go`)
- From `/kafka-consume`: `{schemaVersionId, dataFormat, schemaDefinition, schemaArn, record}` (line 590-602 of `sidecar.go`)

The demo decodes the base64 `bytes` field to hex for narration and prints the `record` envelope for the Java-consume direction.

## 8. Build and Run Instructions

### 8.1 Prerequisites

1. **AWS credentials** for account `850995546034` (the developer "customer" account). Either `AWS_PROFILE` set in `~/.aws/config` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` exported.
2. **Go 1.21+** on PATH.
3. **JDK 11+** on PATH (or `GSR_INTEROP_JAVA` pointing to a JDK binary).
4. **Docker** running (for testcontainers-go Kafka broker).
5. **Java sidecar JAR built:** `make java-sidecar-build` (from `native-schema-registry/golang/`).
6. **`default-registry` exists** in Glue in `us-east-2` (see `REAL-AWS-RUNBOOK.md`).

### 8.2 Running the Demo

From `native-schema-registry/golang/`:

```bash
# Build and run
make demo-interop

# Or manually:
cd integration-tests && go run ./cmd/demo-interop/

# Capture transcript:
cd integration-tests && go run ./cmd/demo-interop/ 2>&1 | tee demo-transcript.txt
```

### 8.3 Makefile Target

A new `demo-interop` target is added to the existing Makefile:

```makefile
.PHONY: demo-interop
demo-interop: java-sidecar-build
    @if [ -z "$$AWS_PROFILE" ] && [ -z "$$AWS_ACCESS_KEY_ID" ]; then \
        echo "ERROR: demo-interop requires AWS credentials"; \
        exit 1; \
    fi
    @echo "Running Java↔Go cross-language cross-version interop demo..."
    cd integration-tests && $(GO) run ./cmd/demo-interop/
```

## 9. Verification and Cleanup

### 9.1 What "Demo Passes" Means

- Exit code 0.
- All 6 cross-pairs succeed (3 formats x 2 directions).
- The summary table at the end shows 6/6 PASS.
- No Glue resource leaks: all `demo-4.15-*` schemas are deleted.

### 9.2 Cleanup Strategy

Cleanup runs in a `defer` block from `main()`:

1. Uses `realglue.NewCleanup()` with `TrackSchemaPrefix("default-registry", "demo-4.15-")`.
2. On normal exit or panic/fatal, the deferred cleanup sweeps all schemas matching the prefix.
3. If the demo is interrupted (SIGINT/SIGTERM), a signal handler triggers the same cleanup before exiting.
4. Narrates each deletion in the `[CLEANUP]` section.

### 9.3 Idempotent Re-runs

If a prior demo run leaked schemas (killed before cleanup):
- The current run's random suffix ensures no name collision with leftover schemas.
- The prefix sweep (`demo-4.15-`) in cleanup will also delete any leftover schemas from prior runs that share the prefix. This is by design: the demo is its own janitor.

### 9.4 Post-Run Verification

The demo prints a reminder at the end:

```
To verify no leaked schemas:
  aws glue list-schemas --registry-id RegistryName=default-registry --region us-east-2 \
    --query 'Schemas[?starts_with(SchemaName, `demo-4.15-`)].SchemaName' --output text
```

## 10. Constraints

### MUST

- MUST reuse the existing Java sidecar (`integration-tests/java-interop/`) without adding new endpoints.
- MUST reuse the existing `javasidecar`, `kafkaharness`, and `realglue` Go packages.
- MUST reuse the Phase 4.13 schema fixtures (same v1/v2 pairs).
- MUST use `buildDynamicProtoMessage` pattern for Protobuf (not compiled testpb types).
- MUST print narration to stdout in a human-readable format (section headers, indentation, separators).
- MUST decompose wire bytes into header (0x03), compression byte, 16-byte UUID, and payload body in hex.
- MUST print the Go config map applied for each serialization/deserialization.
- MUST exit 0 only when all 6 pairs pass.
- MUST clean up all schemas on exit (defer + signal handler).
- MUST run on a Cloud Desktop with `aws sts get-caller-identity` returning account `850995546034`.
- MUST complete in under 5 minutes.

### MUST NOT

- MUST NOT modify any file under `pkg/gsrserde-go/` (no public API changes).
- MUST NOT add new entries to the integration test suite (`tests/` directory).
- MUST NOT introduce dependencies on KMS, S3, or any service beyond Glue and Kafka.
- MUST NOT hardcode AWS credentials or account IDs in source (use env vars / SDK default chain).
- MUST NOT use the `ModeContainer` path for the sidecar (demo assumes local JVM).

## 11. Definition of Done

1. `go run ./cmd/demo-interop/` exits 0 when run with valid AWS credentials against account `850995546034`.
2. The stdout transcript contains narration for all 6 cross-pairs (3 formats x 2 directions).
3. Each narration section includes: schema body, Glue version-id, Go config, wire-byte hex dump with decomposition, decoded values, and equality check result.
4. `make demo-interop` succeeds (target exists, builds sidecar, runs demo).
5. `aws glue list-schemas --registry-id RegistryName=default-registry --region us-east-2 --query 'Schemas[?starts_with(SchemaName, `demo-4.15-`)].SchemaName' --output text` returns empty after the demo completes.
6. No new exported symbols in `pkg/gsrserde-go/`.
7. The transcript is redirectable to a file via `| tee demo.log` without corruption.

## 12. Regression Guardrails

- **INV-1: No public API surface change.** `go doc ./pkg/gsrserde-go/...` output remains identical before and after this phase. The demo imports the library as a consumer, not a contributor.
- **INV-2: Existing tests unaffected.** `make test` and `make test-integ-interop-real` pass identically with or without the demo directory present. The demo is a separate `cmd/` binary, not a `_test.go` file.
- **INV-3: Java sidecar backward compatibility.** The sidecar JAR built by `make java-sidecar-build` continues to serve Phase 4.6.5 and 4.13 tests. No sidecar source changes in this phase.
- **INV-4: Cleanup prefix isolation.** The demo uses prefix `demo-4.15-` which is distinct from integration test prefixes (`gsr-go-it-interop-`, `gsr-go-it-xver-`). No interference between demo and test cleanup sweeps.

## 13. Scenarios (Gherkin)

### Scenario: Avro Java-to-Go with cross-version evolution

```gherkin
Given the Java sidecar registers Avro schema v1 under "demo-4.15-avro-<suffix>" with BACKWARD compatibility
  And the Java sidecar registers Avro schema v2 under the same name
  And the Java sidecar produces a v1 record to Kafka
When the Go deserializer consumes the framed bytes from Kafka
  And the Go deserializer resolves the schema-version-id from the wire header
Then the decoded Go map contains id="demo-id", name="demo-name", age=42
  And the demo prints the full wire-byte hex dump with UUID decomposition
  And the demo prints "PASS"
```

### Scenario: Avro Go-to-Java with cross-version evolution

```gherkin
Given schemas v1 and v2 are registered in Glue (from Direction A or re-registered)
  And the Go serializer produces a v1 AvroRecord to Kafka
When the Java sidecar consumes the framed bytes via /kafka-consume
Then the Java envelope contains fields.id="demo-id", fields.name="demo-name", fields.age=42
  And the demo prints the Go-side wire-byte hex dump with UUID decomposition
  And the demo prints "PASS"
```

### Scenario: JSON Schema Java-to-Go with cross-version evolution

```gherkin
Given the Java sidecar registers JSON Schema v1 under "demo-4.15-json-<suffix>" with BACKWARD compatibility
  And the Java sidecar registers JSON Schema v2 under the same name
  And the Java sidecar produces a v1 JSON payload to Kafka
When the Go deserializer consumes and decodes
Then the decoded Go string payload contains {"id":"demo-id","name":"demo-name","age":42}
  And the demo prints "PASS"
```

### Scenario: Protobuf Go-to-Java with cross-version evolution

```gherkin
Given schemas v1 and v2 are registered in Glue
  And the Go serializer builds a dynamicpb.Message from the v1 .proto definition
  And the Go serializer serializes the message to Kafka
When the Java sidecar consumes via /kafka-consume
Then the Java envelope contains fieldsJson with id="demo-id", name="demo-name", age=42
  And the demo prints "PASS"
```

### Scenario: Demo cleanup on success

```gherkin
Given the demo has completed all 6 pairs successfully
When the deferred cleanup runs
Then all schemas matching prefix "demo-4.15-" are deleted from default-registry
  And the demo exits with code 0
```

### Scenario: Demo cleanup on failure

```gherkin
Given the demo fails on any pair (e.g., Glue timeout)
When the deferred cleanup runs (triggered by defer or signal handler)
Then all schemas matching prefix "demo-4.15-" are deleted from default-registry
  And the demo exits with non-zero code
  And the narration includes a [FAIL] marker identifying which pair failed
```

## 14. Risks and Assumptions

### Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Glue rate-limiting during schema registration (6 schemas created serially) | Low | Demo fails mid-run | Demo runs formats sequentially, not in parallel. 6 schemas is well below Glue's burst limit. Built-in retry in Go serializer handles transient throttles. |
| Kafka testcontainer slow start on cold Cloud Desktop | Medium | Demo exceeds 5-minute budget | 60s startup timeout (same as integration tests). First-time Docker pull is a one-time cost not counted against the 5-minute run. |
| Java sidecar JAR not pre-built | Medium | Demo fails at startup | `make demo-interop` depends on `java-sidecar-build` target. README instructs user to run `make java-sidecar-build` first. |
| Prior demo run leaked schemas with colliding names | Low | Registration fails with AlreadyExistsException | Random 8-hex suffix makes collisions improbable (1 in 4 billion). Prefix sweep in cleanup deletes any leftover schemas from prior runs. |
| `default-registry` does not exist in account | Low | All schema operations fail | README prereqs check; `REAL-AWS-RUNBOOK.md` documents creation. |

### Assumptions

1. The developer Cloud Desktop has Docker running and `java` on PATH.
2. AWS credentials resolve to account `850995546034` with permissions listed in `REAL-AWS-RUNBOOK.md`.
3. The Java sidecar JAR is built (via `make java-sidecar-build`) before running the demo.
4. `default-registry` exists in `us-east-2`.
5. The demo is NOT intended to run in CI. It is a manual, one-shot demonstration tool.

## 15. Out of Scope

- No canary or continuous validation (deferred until post-ship per room memory `no-canary-until-shipped`).
- No production deployment.
- No UI or web interface.
- No doc-site changes.
- No SDK behavior changes.
- No `ModeContainer` support in the demo (local JVM only).
- No ZLIB compression in the demo narration (the demo uses NONE to keep hex dumps short and readable; ZLIB is already proven in 4.13 tests).
- No multi-version evolution chains (v1-v2-v3). The demo proves the single-hop v1-to-v2 gap.
