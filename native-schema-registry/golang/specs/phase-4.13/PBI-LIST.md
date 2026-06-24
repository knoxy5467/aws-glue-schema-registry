# Phase 4.13 — PBI List (Cross-Language Cross-Version Interop)

## Summary

Five PBIs decompose the spec into: (1) sidecar compatibility extension with regression safety, (2) test scaffolding and schema helpers, (3) Cell A implementation, (4) Cell B implementation, and (5) cumulative validation. PBIs 3 and 4 are parallelizable after PBI-2 completes. PBI-5 depends on both 3 and 4.

## Summary Table

| ID | Title | Size | Dependencies | Files Touched |
|----|-------|------|--------------|---------------|
| PBI-4.13-1 | Sidecar compatibility extension | M | none | `KafkaProduceHandler.java`, `sidecar.go` |
| PBI-4.13-2 | Test scaffolding and schema helpers | L | PBI-4.13-1 | `interop_crossversion_kafka_test.go` |
| PBI-4.13-3 | Cell A (Java produces v1, Go consumes) | M | PBI-4.13-2 | `interop_crossversion_kafka_test.go` |
| PBI-4.13-4 | Cell B (Go produces v1, Java consumes) | M | PBI-4.13-2 | `interop_crossversion_kafka_test.go` |
| PBI-4.13-5 | Cumulative validation and cleanup | S | PBI-4.13-3, PBI-4.13-4 | `interop_crossversion_kafka_test.go` |

## Isolation Matrix

| PBI | Files | Overlaps With |
|-----|-------|---------------|
| PBI-4.13-1 | `KafkaProduceHandler.java`, `sidecar.go` | none |
| PBI-4.13-2 | `interop_crossversion_kafka_test.go` | PBI-4.13-3, PBI-4.13-4, PBI-4.13-5 (append-only: PBI-2 writes skeleton + helpers, PBI-3/4 fill in test functions, PBI-5 adds nothing new) |
| PBI-4.13-3 | `interop_crossversion_kafka_test.go` | PBI-4.13-2, PBI-4.13-4 (disjoint functions: PBI-3 writes `TestInterop_CrossVersion_JavaProduce_GoConsume`, PBI-4 writes `TestInterop_CrossVersion_GoProduce_JavaConsume`) |
| PBI-4.13-4 | `interop_crossversion_kafka_test.go` | PBI-4.13-2, PBI-4.13-3 (disjoint functions) |
| PBI-4.13-5 | `interop_crossversion_kafka_test.go` | PBI-4.13-2, PBI-4.13-3, PBI-4.13-4 (read-only validation, may add minor polish) |

## Topological Order

1. PBI-4.13-1 (no deps)
2. PBI-4.13-2 (depends on PBI-4.13-1)
3. PBI-4.13-3, PBI-4.13-4 (parallel, both depend on PBI-4.13-2)
4. PBI-4.13-5 (depends on PBI-4.13-3 + PBI-4.13-4)

---

## PBI-4.13-1: Sidecar Compatibility Extension

**id:** PBI-4.13-1
**title:** Sidecar compatibility extension
**size:** M (50-100 lines across 2 files)
**depends_on:** none
**parallelizable_with:** none

### Directive

This PBI adds an optional `compatibility` field to the Java sidecar's `/kafka-produce` endpoint and the Go `KafkaProduceRequest` struct. It does NOT add any test logic, new endpoints, or schema definitions. It does NOT modify any existing Phase 4.6.5 test code.

### Context Pointer

- Spec section 5.2 (Modified endpoint: POST /kafka-produce)
- Spec section 5.4 (Go sidecar client change)

### Deliverables

**Files touched:**
- `integration-tests/java-interop/src/main/java/com/amazonaws/services/schemaregistry/interop/KafkaProduceHandler.java`
- `integration-tests/pkg/javasidecar/sidecar.go`

**Changes:**
1. `KafkaProduceHandler.java`: Extract optional `compatibility` field from request JSON (default `"NONE"`). Replace the hardcoded `"NONE"` on line 94 with the extracted value.
2. `sidecar.go`: Add `Compatibility string` field to `KafkaProduceRequest` struct. In `KafkaProduce` method body-map construction, conditionally include `"compatibility"` key when non-empty.

### Acceptance Criteria

- [AC-8] The Java sidecar `KafkaProduceHandler` change is backward-compatible: when `compatibility` is absent from request JSON, behavior is identical to before (defaults to `"NONE"`).
- [AC-9] No new public surface area added to core library (`pkg/gsrserde-go/`).
- [INV-1] Phase 4.6.5 `interop_kafka_roundtrip_test.go` continues to pass unchanged (the existing calls omit `Compatibility` in `KafkaProduceRequest`, which means the field stays empty string, which means the Java side defaults to `"NONE"`).

### Verification Steps

1. `cd integration-tests/java-interop && mvn package -q` (JAR rebuilds without error).
2. `grep -n "compatibility" KafkaProduceHandler.java` shows the new extraction + usage.
3. `grep -n "Compatibility" pkg/javasidecar/sidecar.go` shows the new struct field and conditional body inclusion.
4. Confirm no changes to any file in `pkg/gsrserde-go/`.
5. `go build ./...` from `integration-tests/` compiles cleanly.

### Risks / Open Questions

- The `HttpUtil.optionalString` method already exists (confirmed in source). No risk here.
- Must ensure the Java field name in JSON is lowercase `"compatibility"` (matching Go struct tag convention used by the sidecar).

---

## PBI-4.13-2: Test Scaffolding and Schema Helpers

**id:** PBI-4.13-2
**title:** Test scaffolding and schema helpers
**size:** L (200+ lines)
**depends_on:** PBI-4.13-1
**parallelizable_with:** none

### Directive

This PBI creates the new test file `interop_crossversion_kafka_test.go` with: build tag, package declaration, imports, schema constant strings (v1 and v2 for all 3 formats), the `crossVersionCase` struct, the `crossVersionMatrix()` function returning 6 cases, helper functions (`registerV2ViaJava`, `buildDynamicProtoMessage`), and empty/stubbed `TestInterop_CrossVersion_JavaProduce_GoConsume` and `TestInterop_CrossVersion_GoProduce_JavaConsume` function skeletons with setup (sidecar start, kafka broker, realglue cleanup wiring). It does NOT implement the actual cell logic (that is PBI-3 and PBI-4).

### Context Pointer

- Spec sections 4.1, 4.2, 4.3 (schema definitions)
- Spec section 6.1-6.3 (package, file, test structure)
- Spec section 6.6 (helpers reused vs added)
- Spec section 6.7 (PROTOBUF Go-side record construction)
- Spec section 7 (test gating and cleanup)

### Deliverables

**Files touched:**
- `integration-tests/tests/interop_crossversion_kafka_test.go` (NEW)

**Functions/types introduced:**
- `type crossVersionCase struct` (spec section 6.3)
- `func crossVersionMatrix() []crossVersionCase` (returns 6 cases: AVRO_NONE, AVRO_ZLIB, JSON_NONE, JSON_ZLIB, PROTOBUF_NONE, PROTOBUF_ZLIB)
- `func registerV2ViaJava(ctx context.Context, sc *javasidecar.Sidecar, req javasidecar.KafkaProduceRequest) error`
- `func buildDynamicProtoMessage(schemaDef string, fields map[string]interface{}) (proto.Message, error)`
- `func TestInterop_CrossVersion_JavaProduce_GoConsume(t *testing.T)` (skeleton only: gate, sidecar, broker, cleanup setup, matrix loop with `t.Skip("Cell A not yet implemented")`)
- `func TestInterop_CrossVersion_GoProduce_JavaConsume(t *testing.T)` (skeleton only: same pattern with `t.Skip("Cell B not yet implemented")`)

**Schema constants (6 total):**
- `crossVersionAvroV1`, `crossVersionAvroV2`
- `crossVersionJSONV1`, `crossVersionJSONV2`
- `crossVersionProtoV1`, `crossVersionProtoV2`

### Acceptance Criteria

- [AC-7] Tests are gated by `AWS_INTEGRATION=1`, `GSR_GLUE=real`, `GSR_INTEROP_MODE=local`.
- [AC-10] Schema names use unique per-test-run suffix (random 8 hex chars via `uniqueInteropSuffix`).
- [AC-12] Test structure produces exactly 12 subtests (verified by matrix length 6 x 2 directions).
- [INV-2] Cleanup prefix is `gsr-go-it-xver-` (distinct from Phase 4.6.5's `gsr-go-it-interop-`).
- [INV-3] No new exported symbols in `pkg/gsrserde-go/`. All helpers live in `integration-tests/tests/`.
- File compiles: `go build ./...` from `integration-tests/` passes.
- The `crossVersionMatrix()` function returns exactly 6 cases with subtest names matching spec section 6.3 table.
- `buildDynamicProtoMessage` constructs a valid `dynamicpb.Message` from a proto schema definition string.

### Verification Steps

1. `go build ./...` from `integration-tests/` compiles cleanly.
2. `grep -c "crossVersionCase" tests/interop_crossversion_kafka_test.go` confirms struct usage.
3. Confirm 6 entries in `crossVersionMatrix()` with exact names: AVRO_NONE, AVRO_ZLIB, JSON_NONE, JSON_ZLIB, PROTOBUF_NONE, PROTOBUF_ZLIB.
4. Confirm cleanup prefix is `gsr-go-it-xver-` (not `gsr-go-it-interop-`).
5. Confirm no files changed in `pkg/gsrserde-go/`.

### Risks / Open Questions

- `buildDynamicProtoMessage` depends on `google.golang.org/protobuf/types/dynamicpb` and a proto parser (e.g., `github.com/bufbuild/protocompile` or jhump's `protoparse`). Both are already transitive deps. Confirm import path at implementation time.
- JSON Schema `additionalProperties: false` in both v1 and v2 per spec decision. If Glue rejects during PBI-3/4 implementation, the implementor may switch to `true` without re-spec.

---

## PBI-4.13-3: Cell A (Java Produces v1, Go Consumes)

**id:** PBI-4.13-3
**title:** Cell A implementation (Java produces at v1, Go consumes)
**size:** M (100-150 lines)
**depends_on:** PBI-4.13-2
**parallelizable_with:** PBI-4.13-4

### Directive

This PBI implements the full Cell A logic inside `TestInterop_CrossVersion_JavaProduce_GoConsume`. It replaces the `t.Skip` stub from PBI-2 with the complete flow: register v1 + produce at v1 via Java sidecar, register v2 via throwaway-topic produce, Go consumes from Kafka, Go deserializes and asserts v1 fields. It does NOT implement Cell B.

### Context Pointer

- Spec section 6.4 (Cell A flow)
- Spec section 6.6 (reused helpers)
- Spec section 5.3 (v2 registration via throwaway-topic produce)

### Deliverables

**Files touched:**
- `integration-tests/tests/interop_crossversion_kafka_test.go` (modify: fill in `TestInterop_CrossVersion_JavaProduce_GoConsume`)

**Logic per subtest (6 subtests):**
1. Generate unique suffix for schema name and topic.
2. Java sidecar `/kafka-produce` with v1 schema + `compatibility=BACKWARD` to main topic.
3. Java sidecar `/kafka-produce` with v2 schema + same schemaName + `compatibility=BACKWARD` to throwaway topic (registers v2).
4. Go `consumeOne` from main topic to get v1-framed bytes.
5. Go `deserializer.NewDeserializer` + `Deserialize(topic, framed)`.
6. Assert decoded payload matches v1 fields (id, name, age).

### Acceptance Criteria

- [AC-1] `TestInterop_CrossVersion_JavaProduce_GoConsume` passes for all 6 matrix cells (AVRO_NONE, AVRO_ZLIB, JSON_NONE, JSON_ZLIB, PROTOBUF_NONE, PROTOBUF_ZLIB).
- [AC-3] Each subtest registers both v1 and v2 under the same schema name with BACKWARD compatibility before the Go consume step.
- [AC-4] Go-side deserialization returns original v1 payload fields (id, name, age) without data loss.
- [AC-6] All schemas created are cleaned up via `realglue.Cleanup`.
- [AC-11] PROTOBUF test uses `dynamicpb.Message` on the Go side to construct/validate the v1 message.
- [INV-1] Phase 4.6.5 `interop_kafka_roundtrip_test.go` unmodified and still passes.

### Verification Steps

1. `go build ./...` from `integration-tests/` compiles.
2. With `AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local`: `go test -v -run TestInterop_CrossVersion_JavaProduce_GoConsume ./tests/ -tags integration -timeout 600s` passes all 6 subtests.
3. Confirm no modifications to `interop_kafka_roundtrip_test.go`.

### Risks / Open Questions

- Kafka testcontainer startup may be slow; the 120s per-subtest timeout and shared broker should handle this.
- Glue may throttle under parallel schema registrations. The 120s timeout + library-level retries mitigate.
- If `additionalProperties: false` causes Glue rejection for JSON, switch to `true` per spec risk mitigation.

---

## PBI-4.13-4: Cell B (Go Produces v1, Java Consumes)

**id:** PBI-4.13-4
**title:** Cell B implementation (Go produces at v1, Java consumes)
**size:** M (100-150 lines)
**depends_on:** PBI-4.13-2
**parallelizable_with:** PBI-4.13-3

### Directive

This PBI implements the full Cell B logic inside `TestInterop_CrossVersion_GoProduce_JavaConsume`. It replaces the `t.Skip` stub from PBI-2 with the complete flow: register v1 and v2 via Java sidecar (both to throwaway topics), Go serializes v1 record + produces to Kafka, Java consumes via `/kafka-consume` and asserts fields. It does NOT implement Cell A. Cell B independently registers its own schemas (per spec section 6.2 design note).

### Context Pointer

- Spec section 6.5 (Cell B flow)
- Spec section 6.2 (Cell B independent registration justification)
- Spec section 6.7 (PROTOBUF Go-side record construction via dynamicpb)

### Deliverables

**Files touched:**
- `integration-tests/tests/interop_crossversion_kafka_test.go` (modify: fill in `TestInterop_CrossVersion_GoProduce_JavaConsume`)

**Logic per subtest (6 subtests):**
1. Generate unique suffix for schema name and topic.
2. Java sidecar `/kafka-produce` with v1 schema + `compatibility=BACKWARD` to throwaway topic (creates schema + registers v1).
3. Java sidecar `/kafka-produce` with v2 schema + same schemaName + `compatibility=BACKWARD` to throwaway topic (registers v2).
4. Go `serializer.NewSerializer` + `Serialize(topic, v1Record)` (auto-registration detects existing v1 UUID).
5. Go `produceOne` to main topic.
6. Java sidecar `/kafka-consume` from main topic.
7. Assert Java envelope matches v1 fields (id, name, age).

### Acceptance Criteria

- [AC-2] `TestInterop_CrossVersion_GoProduce_JavaConsume` passes for all 6 matrix cells.
- [AC-3] Each subtest registers both v1 and v2 under the same schema name with BACKWARD compatibility before the Java consume step.
- [AC-5] Java-side deserialization (via `/kafka-consume`) returns original v1 payload fields without data loss.
- [AC-6] All schemas created are cleaned up via `realglue.Cleanup`.
- [AC-11] PROTOBUF test uses `dynamicpb.Message` on the Go side for v1 serialization.
- [INV-1] Phase 4.6.5 `interop_kafka_roundtrip_test.go` unmodified.

### Verification Steps

1. `go build ./...` from `integration-tests/` compiles.
2. With `AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local`: `go test -v -run TestInterop_CrossVersion_GoProduce_JavaConsume ./tests/ -tags integration -timeout 600s` passes all 6 subtests.
3. Confirm no modifications to `interop_kafka_roundtrip_test.go`.

### Risks / Open Questions

- Go serializer's `schemaAutoRegistrationEnabled=true` with an already-existing schema name should resolve to the existing v1 UUID via `GetSchemaByDefinition`. If the library's behavior differs (e.g., it calls `CreateSchema` and gets `AlreadyExistsException` but doesn't fall through), this will surface as a test failure in the FIRST subtest.
- For PROTOBUF, Go must use `buildDynamicProtoMessage` (from PBI-2) and configure the serializer with the CrossVersionMessage descriptor instead of testpb.TestMessage descriptor. This requires a variant of `buildGoConfig` or inline config for the proto case.

---

## PBI-4.13-5: Cumulative Validation and Cleanup Verification

**id:** PBI-4.13-5
**title:** Cumulative validation and cleanup verification
**size:** S (< 50 lines of changes, mostly verification commands)
**depends_on:** PBI-4.13-3, PBI-4.13-4
**parallelizable_with:** none

### Directive

This PBI runs the full test suite to confirm all 12 subtests pass end-to-end, verifies regression invariants (INV-1, INV-2, INV-3), and confirms cleanup removes all `gsr-go-it-xver-` schemas. It does NOT add new code unless minor fixes are needed for integration issues discovered during the cumulative run. It does NOT modify Phase 4.6.5 tests.

### Context Pointer

- Spec section 8 (all 12 acceptance criteria)
- Spec section 9 (regression guardrails INV-1, INV-2, INV-3)
- Spec section 7.2 (cleanup)

### Deliverables

**Files touched:**
- `integration-tests/tests/interop_crossversion_kafka_test.go` (minor polish only if needed)

**Verification artifacts:**
- Full test run log showing 12 subtests passing.
- Phase 4.6.5 regression run showing `interop_kafka_roundtrip_test.go` passes unchanged.
- Confirmation that `pkg/gsrserde-go/` has zero changes from this phase.

### Acceptance Criteria

- [AC-1] through [AC-12] all satisfied simultaneously in a single test run.
- [INV-1] Phase 4.6.5 `TestInterop_KafkaJavaProduce_GoConsume` and `TestInterop_KafkaGoProduce_JavaConsume` pass unchanged.
- [INV-2] Cleanup prefix `gsr-go-it-xver-` is distinct from `gsr-go-it-interop-` (grep confirmation).
- [INV-3] `git diff --name-only` shows no files in `pkg/gsrserde-go/`.

### Verification Steps

1. Full run: `AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local go test -v -run "TestInterop_CrossVersion" ./tests/ -tags integration -timeout 600s` — all 12 subtests pass.
2. Regression: `AWS_INTEGRATION=1 GSR_GLUE=real GSR_INTEROP_MODE=local go test -v -run "TestInterop_Kafka" ./tests/ -tags integration -timeout 600s` — Phase 4.6.5 tests pass.
3. `git diff --name-only` confirms no changes in `pkg/gsrserde-go/`.
4. `grep "gsr-go-it-xver-" tests/interop_crossversion_kafka_test.go` confirms prefix usage.
5. `grep "gsr-go-it-interop-" tests/interop_crossversion_kafka_test.go` returns nothing (no collision).

### Risks / Open Questions

- If PBI-3 and PBI-4 were implemented in parallel and both passed independently but fail together (resource contention, schema name collision), this PBI discovers and fixes the integration issue.
- Real AWS costs: running all 12 subtests registers up to 6 schemas (3 formats x 2 cells, each with v1+v2 = 12 schema versions total). Cleanup sweeps them.
