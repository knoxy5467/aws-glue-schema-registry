# Phase 4.13 — Cross-Language Cross-Version Interop

## 1. Overview & Goal

Phase 4.6.5 proved Go and Java GSR clients produce byte-identical wire frames at a single schema version. Phase 4.12 proved the Go client handles schema evolution (v1 to v2 BACKWARD) within a single language. Phase 4.13 closes the remaining gap: a record serialized by one language at schema version N must deserialize correctly in the other language when the schema name carries a BACKWARD-evolved successor version.

The test proves that the GSR wire-format header (version UUID) is the canonical interop contract, and that neither language's serializer/deserializer embeds language-specific assumptions into the framed bytes that would break cross-language consumption under evolution.

## 2. Scope & Non-Goals

### In scope

- Two directional cells: Java produces at v1, Go consumes (Cell A); Go produces at v1, Java consumes (Cell B).
- Three data formats: AVRO, JSON, PROTOBUF.
- Two compression modes: NONE, ZLIB.
- BACKWARD compatibility mode (fixed for all cells).
- Schema evolution from v1 to v2 (one added optional/nullable field).
- Real AWS Glue schema registration (account 850995546034).
- Kafka transport via testcontainers-go.
- Teardown via `realglue.Cleanup`.

### Non-goals

- FORWARD, FULL, or BACKWARD_ALL compatibility modes (covered by Phase 4.12 within Go).
- Multi-hop evolution chains (v1 to v2 to v3).
- Avro reader-schema projection (deferred to Phase 5 canary).
- Performance or load testing.
- New core library surface area.

## 3. Architecture / Test Topology

```
 ┌─────────────────────────────────────────────────────────┐
 │ Go test process (interop_crossversion_kafka_test.go)     │
 │                                                         │
 │   ┌──────────────┐       ┌────────────────────┐        │
 │   │ Go Serializer│       │ Go Deserializer    │        │
 │   │ (v1 payload) │       │ (decodes v1 frame) │        │
 │   └──────┬───────┘       └────────┬───────────┘        │
 │          │                        ▲                     │
 │          │ Cell B: produce        │ Cell A: consume     │
 │          ▼                        │                     │
 │   ┌──────────────────────────────────────────────┐      │
 │   │        Kafka (testcontainers-go)             │      │
 │   └──────────────────────────────────────────────┘      │
 │          ▲                        │                     │
 │          │ Cell A: produce        │ Cell B: consume     │
 │          │                        ▼                     │
 │   ┌──────────────────────────────────────────────┐      │
 │   │   Java Sidecar (javasidecar.StartForTest)    │      │
 │   │                                              │      │
 │   │  /kafka-produce (registers v1+v2, produces   │      │
 │   │                   at v1)                     │      │
 │   │  /kafka-consume (deserializes framed bytes)  │      │
 │   └──────────────────────────────────────────────┘      │
 │                          │                              │
 │                          ▼                              │
 │         ┌──────────────────────────────────────┐        │
 │         │ Real AWS Glue (us-east-2)            │        │
 │         │ Account 850995546034                 │        │
 │         │ default-registry, BACKWARD compat    │        │
 │         └──────────────────────────────────────┘        │
 └─────────────────────────────────────────────────────────┘
```

### File layout

```
integration-tests/
├── tests/
│   └── interop_crossversion_kafka_test.go   (NEW — main test file)
├── java-interop/src/main/java/.../interop/
│   └── KafkaProduceHandler.java             (MODIFY — accept compatibility field)
└── pkg/javasidecar/
    └── sidecar.go                           (MODIFY — pass compatibility in request)
```

## 4. Wire Format & Schema Evolution Per Format

All three formats use the same evolution strategy: v2 adds one optional/nullable field to v1. Under BACKWARD compatibility, a consumer with awareness of v2 can still read v1-produced records because the new field has a default (null for AVRO/JSON, zero-value for PROTOBUF). The test produces at v1 and asserts the original v1 fields decode correctly on the consumer side.

### 4.1 AVRO

**v1 schema:**
```json
{
  "type": "record",
  "name": "CrossVersionRecord",
  "namespace": "test",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
```

**v2 schema (BACKWARD-compatible):**
```json
{
  "type": "record",
  "name": "CrossVersionRecord",
  "namespace": "test",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

**Justification:** Adding a union-typed field with `"default": null` is the canonical BACKWARD-compatible Avro evolution. Consumers using v2 can still read v1 data (the missing `email` field resolves to the default `null`). Consumers using v1 ignore the field entirely if reading v2 data.

### 4.2 JSON Schema

**v1 schema:**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "age": {"type": "integer"}
  },
  "required": ["id", "name", "age"],
  "additionalProperties": false
}
```

**v2 schema (BACKWARD-compatible):**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "age": {"type": "integer"},
    "email": {"type": ["string", "null"]}
  },
  "required": ["id", "name", "age"],
  "additionalProperties": false
}
```

**Justification:** Adding a non-required property with a nullable type is BACKWARD-compatible under JSON Schema: v1 documents (missing `email`) still validate against v2 schema. The `additionalProperties: false` constraint is applied on BOTH versions to prove that Glue's compatibility checker accepts this strict evolution pattern.

**Decision: `additionalProperties: false` (committed).** Both v1 and v2 use strict mode. This is a stronger test of Glue's compatibility engine and matches production usage patterns where schemas lock down their shape. Risk mitigation: if Glue rejects `additionalProperties: false` for any reason during implementation, the implementor MAY switch both schemas to `additionalProperties: true` without requiring a spec revision.

### 4.3 PROTOBUF

**Choice: Add an optional field to the existing message (option b).**

This matches the Avro and JSON patterns (same logical evolution), avoids introducing a second generated message type, and aligns with the existing `testpb.TestMessage` that already has `email` (field 4) and `tags` (field 5). The test will use a SUBSET of fields for v1 (id, name, age only) and add an additional optional field for v2 via a distinct .proto definition string.

**v1 .proto (schema definition string registered with Glue):**
```proto
syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
```

**v2 .proto (schema definition string registered with Glue):**
```proto
syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
```

**Justification:** In proto3, adding a new field with a higher field number is always wire-compatible (BACKWARD). Readers unaware of field 4 skip it; readers aware of field 4 get the zero value (`""`) when reading v1 data. Using a SEPARATE message name (`CrossVersionMessage`) avoids any confusion with the existing `TestMessage` and makes the test self-contained. No new code generation is needed because the Go serializer uses `dynamicpb` (via jhump/protoprint) when the registered schema differs from the compiled type. The test constructs the v1 payload using the JSON-to-DynamicMessage path the sidecar already supports.

**Important note:** The Go side produces Protobuf by providing a `proto.Message` and the schema definition. For cross-version testing, we use a dedicated `.proto` definition string (not the compiled `testpb.TestMessage`). The Go serializer's Protobuf path accepts a `protoreflect.MessageDescriptor` in config. For this test, we will build a `dynamicpb.Message` from the v1 schema definition at runtime, populate fields via reflection, and pass it to `Serialize`. This mirrors what the Java side does (DynamicMessage).

## 5. Java Sidecar Surface

### 5.1 Existing endpoints used (no change needed)

- `POST /kafka-consume` — already accepts all fields needed (bootstrap, topic, format, region, timeoutMs). No modification required.
- `POST /health` — health check, no changes.

### 5.2 Modified endpoint: `POST /kafka-produce`

**Current behavior:** The `KafkaProduceHandler` hardcodes `COMPATIBILITY_SETTING` to `"NONE"` (line 94 of `KafkaProduceHandler.java`).

**Required change:** Accept an optional `"compatibility"` field in the JSON request body. When present, use it as the `COMPATIBILITY_SETTING` value; when absent, default to `"NONE"` (preserving backward compatibility with Phase 4.6.5 tests).

**File:** `integration-tests/java-interop/src/main/java/com/amazonaws/services/schemaregistry/interop/KafkaProduceHandler.java`

**Change (1 line added, 1 line modified):**
```java
// After line 78 (region extraction):
String compatibility = HttpUtil.optionalString(req, "compatibility", "NONE");

// Line 94 changes FROM:
gsrConfigs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "NONE");
// TO:
gsrConfigs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, compatibility);
```

**Request shape after change:**
```json
{
  "format": "AVRO",
  "schema": "<v1 schema definition>",
  "schemaName": "gsr-go-it-xver-avro-<suffix>",
  "record": {"fields": {"id": "...", "name": "...", "age": 42}},
  "compression": "NONE",
  "bootstrap": "<kafka bootstrap>",
  "topic": "<topic>",
  "region": "us-east-2",
  "compatibility": "BACKWARD"
}
```

### 5.3 No new endpoint needed

The brief mentions a potential `/register-schema-version` endpoint. After analysis, this is NOT needed. The Java GSR library's `GlueSchemaRegistryKafkaSerializer.serialize()` already handles the full registration flow internally:
1. First call with schema name X creates the schema with the given compatibility.
2. Second call with schema name X but a different definition automatically calls `RegisterSchemaVersion` under the hood (the library catches `AlreadyExistsException` and falls through to `RegisterSchemaVersion`).

The test achieves the two-version state by calling `/kafka-produce` twice on the same `schemaName`: first with the v1 definition, then with the v2 definition. Both calls pass `"compatibility": "BACKWARD"`. The first creates the schema; the second registers v2 as a new version. After both calls, the schema name has v1 and v2 registered with BACKWARD compatibility.

### 5.4 Go sidecar client change

**File:** `integration-tests/pkg/javasidecar/sidecar.go`

**Change:** Add `Compatibility` field to `KafkaProduceRequest` struct (line ~442) and pass it in the request body map (line ~527).

```go
type KafkaProduceRequest struct {
    // ... existing fields ...
    Compatibility string // optional; defaults to "NONE" on the Java side
}
```

In the `KafkaProduce` method body map construction:
```go
if req.Compatibility != "" {
    body["compatibility"] = req.Compatibility
}
```

## 6. Go Test Surface

### 6.1 Package and file

- **Package:** `integration_tests` (same as all other integration tests in the `tests/` directory).
- **File:** `integration-tests/tests/interop_crossversion_kafka_test.go` (NEW).
- **Build tag:** `//go:build integration`

### 6.2 Test functions

Two top-level test functions mirroring the Phase 4.6.5 naming convention:

- `TestInterop_CrossVersion_JavaProduce_GoConsume(t *testing.T)` — Cell A
- `TestInterop_CrossVersion_GoProduce_JavaConsume(t *testing.T)` — Cell B

**Design note (Cell B independent registration):** The brief suggests Cell B should "reuse Cell A's registered v1+v2." This spec intentionally departs from that suggestion. Each cell independently registers its own v1 and v2 schemas via the Java sidecar using a unique suffix from `uniqueInteropSuffix(t)`. The justification is test isolation: independent registration enables `t.Parallel()` safety across all subtests, eliminates cross-cell cascading failures (Cell A schema cleanup cannot break Cell B), and gives each cell deterministic teardown via its own `cleanup.TrackSchema` call. The PBI creator and implementor should treat this as a deliberate design choice, not a deviation to fix.

### 6.3 Table-driven test shape

```go
type crossVersionCase struct {
    name        string
    format      string
    compression string
    v1Schema    string
    v2Schema    string
    // goV1Record builds the v1 payload for Go-side serialization.
    goV1Record  func() interface{}
    // javaV1Record builds the v1 JSON envelope for Java-side serialization.
    javaV1Record func() map[string]any
    // goCheck validates Go-deserialized v1 payload.
    goCheck     func(t *testing.T, got interface{})
    // javaEnvelopeCheck validates Java-deserialized v1 envelope.
    javaEnvelopeCheck func(t *testing.T, env map[string]any)
}
```

A `crossVersionMatrix()` function returns 6 cases (3 formats x 2 compressions). The schema name base distinguishes formats: `"xver-avro"`, `"xver-json"`, `"xver-proto"`.

**Exact subtest names emitted by `crossVersionMatrix()`:**

| `name` | format | compression |
|--------|--------|-------------|
| `AVRO_NONE` | AVRO | NONE |
| `AVRO_ZLIB` | AVRO | ZLIB |
| `JSON_NONE` | JSON | NONE |
| `JSON_ZLIB` | JSON | ZLIB |
| `PROTOBUF_NONE` | PROTOBUF | NONE |
| `PROTOBUF_ZLIB` | PROTOBUF | ZLIB |

Each direction function runs `t.Run(tc.name, ...)`, producing 12 total subtests:
- `TestInterop_CrossVersion_JavaProduce_GoConsume/AVRO_NONE`
- `TestInterop_CrossVersion_JavaProduce_GoConsume/AVRO_ZLIB`
- ... (6 per direction)
- `TestInterop_CrossVersion_GoProduce_JavaConsume/AVRO_NONE`
- ... (6 per direction)

These names are distinct from Phase 4.6.5's subtests (which use format-only names like `avro`, `json`, `protobuf`) and will not collide in test output.

### 6.4 Cell A flow (Java produces at v1, Go consumes)

```
1. Generate unique suffix for schema name and topic.
2. Java sidecar: /kafka-produce with v1 schema + compatibility=BACKWARD
   → creates schema in Glue with BACKWARD compat, registers v1, produces to Kafka.
3. Java sidecar: /kafka-produce with v2 schema + same schemaName + compatibility=BACKWARD
   → registers v2 as new version under same schema name (does NOT produce to Kafka).
   NOTE: This produce goes to a throwaway topic so it doesn't pollute Cell A's
   consumption. Alternatively, it can produce to the same topic but the Go consumer
   reads only the first message.
   ORDERING NOTE: Registering v2 AFTER the v1 produce (step 2) is intentional and
   semantically equivalent to registering both first. The Go consumer's read (step 4)
   happens strictly after step 3 completes, so v2 is guaranteed to exist by read time.
4. Go consumer: sarama poll from topic → gets v1-framed bytes.
5. Go deserializer: Deserialize(topic, framed) → resolves v1 UUID via Glue, decodes.
6. Assert: decoded payload matches expected v1 fields.
```

**Simplification:** Step 3 exists solely to establish that v2 IS registered (proving the schema name has evolved). The test assertion in step 6 validates that Go correctly deserializes a v1 frame even when a v2 exists. The Java GSR library's internal cache won't interfere because step 2's produce already completed and the Glue state is durable.

**Alternative simpler approach for step 3:** Instead of a second `/kafka-produce` call to register v2, the test can call `/kafka-produce` with v2 schema + a DIFFERENT throwaway topic name. This registers v2 against the same schema name (auto-registration on same schema name triggers RegisterSchemaVersion internally) without polluting the test topic with a v2 message.

### 6.5 Cell B flow (Go produces at v1, Java consumes)

```
1. Generate unique suffix for schema name and topic.
2. Java sidecar: /kafka-produce with v1 schema + compatibility=BACKWARD + throwaway topic
   → creates schema in Glue with BACKWARD compat, registers v1.
3. Java sidecar: /kafka-produce with v2 schema + same schemaName + compatibility=BACKWARD + throwaway topic
   → registers v2 as new version.
4. Go serializer: Serialize(topic, v1Record) → registers v1 against schema name
   (auto-registration detects existing, resolves to v1 UUID), encodes, produces to Kafka.
5. Java sidecar: /kafka-consume from topic → deserializes v1 bytes.
6. Assert: Java envelope matches expected v1 fields.
```

**Note on step 4:** The Go serializer with `schemaAutoRegistrationEnabled=true` will call `CreateSchema` which hits `AlreadyExistsException` (schema already exists from step 2), then falls through to `GetSchemaByDefinition` which resolves the existing v1 UUID. This is the expected happy path.

### 6.6 Helpers reused vs. added

**Reused (from Phase 4.6.5 `interop_kafka_roundtrip_test.go`):**
- `requireKafkaInterop(t)` — gating function
- `startInteropSidecar(ctx, t)` — sidecar startup
- `consumeOne(t, ctx, bootstrap, topic)` — sarama single-message consumer
- `produceOne(t, ctx, bootstrap, topic, framed)` — sarama single-message producer
- `uniqueInteropSuffix(t)` — random suffix generator
- `buildGoConfig(t, region, schemaName, format, compression)` — Go-side config builder
- `mustJSON(v)` — JSON helper

**Added (test-only, in the new file):**
- `crossVersionMatrix() []crossVersionCase` — table of 6 cases with v1/v2 schemas.
- `registerV2ViaJava(ctx, sc, req)` helper — wraps the "produce v2 to throwaway topic" call for readability. Returns only error (response discarded).
- `buildDynamicProtoMessage(schemaDef, fields)` — constructs a `dynamicpb.Message` from a schema definition string and field values. Needed because the Go serializer for PROTOBUF accepts a `proto.Message` and we cannot use the compiled `testpb.TestMessage` (which has 5 fields, not 3).

### 6.7 PROTOBUF Go-side record construction

For PROTOBUF, the Go producer needs to serialize a v1 message (3 fields: id, name, age) using the v1 schema definition. The approach:

1. Parse the v1 .proto definition string into a `protoreflect.FileDescriptor` using `protoparse` (jhump/protocompile, already a dependency).
2. Find the `CrossVersionMessage` descriptor.
3. Build a `dynamicpb.NewMessage(descriptor)` and set fields via `Set(fd, value)`.
4. Pass this `dynamicpb.Message` (which implements `proto.Message`) to `Serialize`.
5. Configure the Go serializer with this message's descriptor via `ProtobufMessageDescriptorKey`.

This is the same pattern the Java sidecar uses (DynamicMessage) and is consistent with how the GSR library works in production (customers can use any proto.Message implementation).

## 7. Test Gating & Cleanup

### 7.1 Environment variables

Tests are gated by the same guard as Phase 4.6.5:
- `AWS_INTEGRATION=1` — required (real Glue calls bill AWS).
- `GSR_GLUE=real` — required (tests register schemas with real Glue).
- `GSR_INTEROP_MODE=local` — required (Kafka roundtrip needs host-accessible broker).
- `java` on PATH (or `GSR_INTEROP_JAVA` set).

The gate function `requireKafkaInterop(t)` already enforces all of these. The new test reuses it.

### 7.2 Cleanup

The test registers a prefix sweep and explicit schema tracks:
```go
cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-xver-")
cleanup.TrackSchema("default-registry", schemaName) // per subtest
```

Schema names use the pattern `gsr-go-it-xver-{format}-{suffix}` where suffix is 8 hex characters from `uniqueInteropSuffix`. This guarantees parallel-safety and cleanup coverage.

The throwaway topics used for "register v2" produce calls use a distinct topic name (`gsr-go-it-xver-reg-{suffix}`) so they don't interfere with the consumption topic.

### 7.3 Timeouts

- Sidecar startup: 60s context (reused from Phase 4.6.5).
- Per-subtest: 120s context.
- Java sidecar /kafka-consume: `timeoutMs: 60000`.
- `t.Parallel()` enabled for all subtests within a direction.

## 8. Acceptance Criteria

1. `TestInterop_CrossVersion_JavaProduce_GoConsume` passes for all 6 matrix cells (3 formats x 2 compressions).
2. `TestInterop_CrossVersion_GoProduce_JavaConsume` passes for all 6 matrix cells.
3. Each test subtest registers both v1 and v2 under the same schema name with BACKWARD compatibility before producing the test message at v1.
4. Go-side deserialization returns the original v1 payload fields (id, name, age) without data loss.
5. Java-side deserialization (via `/kafka-consume`) returns the original v1 payload fields without data loss.
6. All schemas created during the test are cleaned up via `realglue.Cleanup` at teardown.
7. Tests are gated by `AWS_INTEGRATION=1`, `GSR_GLUE=real`, `GSR_INTEROP_MODE=local`, and pass only when all gates are satisfied.
8. The Java sidecar `KafkaProduceHandler` change is backward-compatible (existing Phase 4.6.5 tests continue to pass with no modification).
9. No new public surface area is added to the core library (`pkg/gsrserde-go/`).
10. Schema names are unique per test run (random suffix prevents cross-run collisions).
11. The PROTOBUF test uses `dynamicpb.Message` on the Go side to construct a v1-only message (3 fields), demonstrating that the wire format is language-agnostic regardless of the concrete message type used at serialization time.
12. Test total is exactly 12 subtests: 2 directions x 3 formats x 2 compressions.

## 9. Regression Guardrails

- **INV-1:** Phase 4.6.5 `interop_kafka_roundtrip_test.go` continues to pass unchanged. The `KafkaProduceHandler` compatibility field defaults to `"NONE"` when absent, preserving existing behavior.
- **INV-2:** Cleanup prefix `gsr-go-it-xver-` does not collide with Phase 4.6.5's `gsr-go-it-interop-` prefix. Both prefixes are distinct and independently sweepable.
- **INV-3:** No new exported symbols in `pkg/gsrserde-go/`. All new helpers (`crossVersionMatrix`, `registerV2ViaJava`, `buildDynamicProtoMessage`) live exclusively in `integration-tests/` and are test-only.

## 10. Risks & Design Decisions

### Risks

| Risk | Mitigation |
|------|-----------|
| Java sidecar's auto-registration of v2 under BACKWARD may fail if Glue's compatibility check rejects the evolution | Use schemas proven to be BACKWARD-compatible (same patterns as Phase 4.12 and Phase 4.6.5). The Avro nullable-union and Proto add-field patterns are canonical. |
| `dynamicpb.Message` serialization path may differ from compiled-type path in the Go serializer | The Go serializer's Protobuf encoder uses `proto.Marshal` which works identically for both `dynamicpb.Message` and compiled types. Verify in implementation. |
| Kafka testcontainer startup flakiness under parallel subtests | Reuse a single Kafka broker across all subtests (same pattern as Phase 4.6.5). Unique topics per subtest prevent cross-talk. |
| JSON Schema `additionalProperties: false` in v2 may cause Glue to reject v1 documents missing `email` | The compatibility check is on the SCHEMA level (v2 schema accepts v1 documents), not on the data level. v1 documents without `email` are valid against v2 schema because `email` is not in `required`. If this fails, fall back to `additionalProperties: true`. |

### Design Decisions (previously open questions, now resolved)

1. **JSON Schema `additionalProperties: false` (committed):** Both v1 and v2 use strict mode. Resolved in §4.2 with a risk-mitigation fallback to `true` if Glue rejects it.

2. **Throwaway topic for v2 registration (accepted):** The spec uses a throwaway topic (`gsr-go-it-xver-reg-{suffix}`) to trigger v2 registration via the existing `/kafka-produce` endpoint. This avoids adding a new sidecar endpoint and keeps the diff minimal. The unused Kafka topic is ephemeral (testcontainer-scoped) and costs nothing.

3. **`dynamicpb` dependency (accepted):** `google.golang.org/protobuf/types/dynamicpb` is already a transitive dependency of the Go serializer's Protobuf path. Using it in tests introduces no new dependency. It is the correct tool for constructing messages from schema definitions at runtime.

## 11. Out-of-Band Considerations

### Flakiness

- Kafka container startup can be slow on cold-pull. The 60s startup context and the shared-broker pattern (one broker for all subtests) mitigates this.
- Glue API throttling under parallel runs is mitigated by unique schema names and the test's tolerance for retries within the 120s subtest timeout.

### Retries

- The Go serializer has built-in retry for Glue throttling (exponential backoff). The Java sidecar relies on the Java GSR library's internal retry.
- `consumeOne` polls with 500ms intervals until ctx expires; no explicit retry loop needed.

### Timeouts

| Operation | Timeout | Rationale |
|-----------|---------|-----------|
| Sidecar startup | 60s | JVM cold start + JAR loading |
| Per-subtest | 120s | Schema registration + Kafka produce + consume + Glue lookups |
| Java /kafka-consume poll | 60s | Generous to allow for slow Kafka rebalance |

### Parallel-safe test names

Each subtest uses a unique random suffix in its schema name AND topic name:
- Schema: `gsr-go-it-xver-{format}-{8hex}`
- Topic: `gsr-go-it-xver-{8hex}` (Cell A/B main topic)
- Throwaway topic for v2 reg: `gsr-go-it-xver-reg-{8hex}`

The prefix `gsr-go-it-xver-` is distinct from Phase 4.6.5's `gsr-go-it-interop-` prefix, preventing cleanup interference between phases.
