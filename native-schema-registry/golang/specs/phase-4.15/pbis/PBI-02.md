# PBI-02: Implement cross-version Java-to-Go scenarios (Direction A) for all 3 formats

## Description

Implements the Direction A scenarios (Java produces at v1, Go consumes with v2 registered) for Avro, JSON Schema, and Protobuf. Each scenario: registers v1 schema via Java sidecar, registers v2 schema via Java sidecar (to prove cross-version tolerance), has Java produce a v1 record to Kafka, has Go deserialize from Kafka, narrates the full flow (schema body, Glue version-id, wire-byte hex dump with decomposition, Go config, decoded values, equality check). Implements spec sections 4.2 Direction A, 4.3, 6.2, 7.1-7.3.

## Scope Boundary

Does NOT implement Direction B (Go-to-Java) scenarios (deferred to PBI-03). Does NOT implement result aggregation, summary table printing, or exit-code logic (deferred to PBI-04). Does NOT re-register schemas for Direction B; those reuse this PBI's registrations at runtime.

## Acceptance Criteria

- [ ] `runDirectionA(ctx, format, cell, broker, sidecar, narrator)` function exists and handles all three formats (Avro, JSON Schema, Protobuf).
- [ ] For each format: Java sidecar registers v1 schema with BACKWARD compatibility, then v2 schema, then produces a v1 record to Kafka. Each step is narrated with `[SCHEMA-V1]`, `[SCHEMA-V2]`, `[JAVA-PRODUCE]` prefixes.
- [ ] For each format: Go deserializer consumes from Kafka, decodes the wire bytes, and verifies the decoded values match the expected record (`id="demo-id"`, `name="demo-name"`, `age=42`). Narrated with `[GO-CONSUME]` and `[VERIFY]` prefixes.
- [ ] Wire-byte hex dump is printed per spec: header (0x03), compression byte (0x00), 16-byte UUID, payload body.
- [ ] Go config map is printed before each deserialization (spec 4.2 format).
- [ ] Each format's Direction A returns a `ScenarioResult` (from PBI-01) that feeds into the summary table.
- [ ] `go build -tags integration ./cmd/demo-interop/` exits 0.
- [ ] Implementor SHOULD execute at least one format scenario locally if AWS creds are available, and attach the relevant stdout segment showing PASS to the IMPL_RESULT. If AWS creds are unavailable in the implementor's worktree, document this in the IMPL_RESULT and proceed.

## Files Touched

- `integration-tests/cmd/demo-interop/scenarios.go` (modified: add Direction A runner functions)
- `integration-tests/cmd/demo-interop/main.go` (modified: wire Direction A into `runAllScenarios`)

## Dependencies

- PBI-01

## Size

L (400+ LOC: ~130 LOC per format scenario logic x 3 formats, shared helpers)

## Verification

```bash
cd integration-tests && go build -tags integration ./cmd/demo-interop/ && go vet -tags integration ./cmd/demo-interop/...
```

Build and vet pass. If AWS creds are available, at least one format scenario executes locally and shows PASS in stdout.

## Implementor Notes

- Java sidecar endpoints: `sc.KafkaProduce(ctx, req)` for registration + produce (see spec 7.1). The sidecar registers the schema AND produces in one call. Call it twice for v1 then v2, but only the v1 call produces a record. For v2, use a throwaway topic (spec 6.3: `demo-4.15-<format>-reg-<8hex>`).
- Go deserializer construction: use `deserializer.NewDeserializer(cfg)` with config from spec 6.2. For Avro use `AvroRecordTypeGeneric`, for Protobuf set `ProtobufMessageDescriptorKey`.
- The base64 `bytes` field from `/kafka-produce` response must be decoded to hex for the wire-byte narration (spec 7.3).
- Kafka consume for Go: read raw bytes from topic, pass to `deserializer.Deserialize(ctx, topic, data)`.
- Direction B (PBI-03) reuses the schemas registered here. The schema name is shared per format; only the topic differs.
- Equality check: compare field-by-field and narrate each comparison per spec 4.2 `[VERIFY]` format.
- Return `ScenarioResult` (defined in PBI-01's `scenarios.go`) for each format scenario.
