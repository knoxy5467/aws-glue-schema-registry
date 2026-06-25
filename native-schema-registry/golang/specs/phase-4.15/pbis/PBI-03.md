# PBI-03: Implement cross-version Go-to-Java scenarios (Direction B) for all 3 formats

## Description

Implements the Direction B scenarios (Go produces at v1, Java consumes with v2 registered) for Avro, JSON Schema, and Protobuf. Each scenario: reuses schemas already registered in Direction A (no new registration), has Go serialize a v1 record to Kafka, has Java sidecar consume and deserialize, narrates the full flow (Go config, wire-byte hex dump with decomposition, Java response envelope, equality check). Implements spec sections 4.2 Direction B, 4.3, 6.2, 7.1-7.3.

## Scope Boundary

Does NOT implement Direction A (Java-to-Go) scenarios (that is PBI-02). Does NOT implement result aggregation, summary table printing, or exit-code logic (deferred to PBI-04). Does NOT re-register schemas; reuses registrations from Direction A at runtime.

## Acceptance Criteria

- [ ] `runDirectionB(ctx, format, cell, broker, sidecar, narrator)` function exists and handles all three formats.
- [ ] For each format: narrates `[SCHEMA-SETUP]` indicating schemas are reused from Direction A.
- [ ] For each format: Go serializer produces a v1 record to Kafka on a separate topic (`demo-4.15-<format>-b-<8hex>`). Narrated with `[GO-PRODUCE]` prefix showing Go config and serialized wire-byte hex dump.
- [ ] For each format: Java sidecar consumes from Kafka via `sc.KafkaConsume(ctx, req)` and returns the deserialized envelope. Narrated with `[JAVA-CONSUME]` prefix.
- [ ] Equality check compares Java envelope fields with expected values. Narrated with `[VERIFY]` prefix.
- [ ] Wire-byte hex dump from Go serialization is decomposed per spec (header, compression, UUID, payload).
- [ ] Each format's Direction B returns a `ScenarioResult` (from PBI-01).
- [ ] `go build -tags integration ./cmd/demo-interop/` exits 0.
- [ ] Implementor SHOULD execute at least one format scenario locally if AWS creds are available, and attach the relevant stdout segment showing PASS to the IMPL_RESULT. If AWS creds are unavailable in the implementor's worktree, document this in the IMPL_RESULT and proceed.

## Files Touched

- `integration-tests/cmd/demo-interop/scenarios.go` (modified: add Direction B runner functions)
- `integration-tests/cmd/demo-interop/main.go` (modified: wire Direction B into `runAllScenarios`)

## Dependencies

- PBI-01
- PBI-02 (Direction B reuses schemas registered in Direction A at runtime, and the code must be structured so Direction A runs first per format)

## Size

M (100-400 LOC: ~100 LOC per format scenario, shared structure with Direction A)

## Verification

```bash
cd integration-tests && go build -tags integration ./cmd/demo-interop/ && go vet -tags integration ./cmd/demo-interop/...
```

Build and vet pass. If AWS creds are available, at least one format scenario executes locally and shows PASS in stdout.

## Implementor Notes

- Direction B does NOT re-register schemas. The narration should print `[SCHEMA-SETUP] v1 and v2 already registered from Direction A (reusing schema name).` (spec 4.2 Direction B).
- Go serializer construction: use `serializer.NewSerializer(cfg)` with config from spec 6.2.
- For Avro: create `AvroRecord{Schema: v1Schema, Data: recordMap}`.
- For JSON: create `JsonDataWithSchema{Schema: v1Schema, Payload: jsonBytes}`.
- For Protobuf: use `buildDynamicProtoMessage(crossVersionProtoV1, fields)` from PBI-01.
- The serializer returns framed bytes. Decode them for hex narration before producing to Kafka.
- Java sidecar consume: `sc.KafkaConsume(ctx, req)` returns an envelope with format-specific fields (spec 7.3). Parse and narrate the `record` field.
- Topic for Direction B: `demo-4.15-<format>-b-<8hex>` (distinct from Direction A's `demo-4.15-<format>-a-<8hex>`).
- Return `ScenarioResult` (defined in PBI-01's `scenarios.go`) for each format scenario.
