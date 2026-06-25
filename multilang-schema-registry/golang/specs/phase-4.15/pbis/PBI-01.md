# PBI-01: Scaffold demo binary with narration framework, schema fixtures, and scenario runner

## Description

Creates the foundational files for the demo binary: `main.go` (entry point with signal handling, startup narration, and deferred cleanup), `narrator.go` (narration helpers: section headers, hex-dump decomposition, config printing, separator lines, `[STAGE]` prefix tags, equality-check output), and `scenarios.go` (re-declared schema constants byte-for-byte from `interop_crossversion_kafka_test.go:73-139`, `buildDynamicProtoMessage` copy, scenario cell type definition, result type definition, and demo record values). Implements spec sections 3.2, 4.1, 5.1-5.5, 6.1-6.3, 9.2, 9.3.

## Scope Boundary

Does NOT implement scenario execution logic for Direction A or Direction B (deferred to PBI-02 and PBI-03). Does NOT implement summary table printing or exit-code logic (deferred to PBI-04). Does NOT create the Makefile target or README (deferred to PBI-05).

## Acceptance Criteria

- [ ] `integration-tests/cmd/demo-interop/main.go` exists with `package main`, build tag `//go:build integration`, and a `func main()` that: starts Kafka via `kafkaharness.StartShared(ctx)`, starts sidecar via `javasidecar.New(ctx, opts)`, prints the startup banner (spec section 4.1), defers cleanup via `realglue.NewCleanup()` with prefix `demo-4.15-`, installs a SIGINT/SIGTERM signal handler that triggers cleanup, and calls a placeholder `runAllScenarios` function.
- [ ] `integration-tests/cmd/demo-interop/narrator.go` exists with helper functions: `printBanner(...)`, `printSectionHeader(format, direction string)`, `printHexDump(data []byte)` (decomposes header byte, compression byte, 16-byte UUID, payload per spec section 4.2), `printGoConfig(cfg map[string]string)`, `printEqualityCheck(field, expected, actual)`, `printSeparator()`, `printStage(tag, msg string)`.
- [ ] `integration-tests/cmd/demo-interop/scenarios.go` exists with: `crossVersionAvroV1`, `crossVersionAvroV2`, `crossVersionJSONV1`, `crossVersionJSONV2`, `crossVersionProtoV1`, `crossVersionProtoV2` string constants (byte-for-byte match of lines 73-139 in `interop_crossversion_kafka_test.go`), a code comment citing the source file and line range, `buildDynamicProtoMessage(schemaDef string, fields map[string]interface{}) (*dynamicpb.Message, error)` function copied from `interop_crossversion_kafka_test.go:387-439` with a code comment citing the source, a `ScenarioCell` struct type (format, direction, schema names, topic names, v1/v2 schemas, record fields), and demo record constants (`demoID="demo-id"`, `demoName="demo-name"`, `demoAge=42`).
- [ ] `scenarios.go` defines a `ScenarioResult` struct with at minimum: `Format string`, `Direction string`, `Pass bool`, `Err error`, `Topic string`, `SchemaVersionID string`. This type is the shared contract consumed by PBI-02, PBI-03, and PBI-04.
- [ ] `go build -tags integration ./cmd/demo-interop/` exits 0 from the `integration-tests/` directory.
- [ ] `go vet -tags integration ./cmd/demo-interop/...` exits 0.

## Files Touched

- `integration-tests/cmd/demo-interop/main.go` (new)
- `integration-tests/cmd/demo-interop/narrator.go` (new)
- `integration-tests/cmd/demo-interop/scenarios.go` (new)

## Dependencies

- none (foundation PBI)

## Size

L (400+ LOC across 3 files: ~150 main.go, ~120 narrator.go, ~180 scenarios.go)

## Verification

```bash
cd integration-tests && go build -tags integration ./cmd/demo-interop/ && go vet -tags integration ./cmd/demo-interop/...
```

Both commands exit 0. The binary builds successfully. No runtime validation needed at this stage (downstream PBIs add scenario logic).

## Implementor Notes

- Spec section 5.5 mandates byte-for-byte re-declaration of schema constants. Copy from `interop_crossversion_kafka_test.go:73-139`, do NOT paraphrase or reformat.
- `buildDynamicProtoMessage` source is `interop_crossversion_kafka_test.go:387-439`. The function already has no `testing.T` dependency; copy as-is.
- The cleanup strategy (spec 9.2) uses `realglue.NewCleanup()` with `TrackSchemaPrefix`. Check `integration-tests/pkg/realglue/cleanup.go` for the exact API.
- Signal handler: use `os/signal.NotifyContext` or channel-based notify for SIGINT/SIGTERM (spec 9.2 point 3).
- Startup narration format is prescribed in spec 4.1. Match the exact banner format including the `================` separator width (80 chars).
- The `ScenarioCell` struct should carry enough context for a generic runner to execute any scenario: format enum, direction, schema name with random suffix, topic name, v1/v2 schema strings, expected record fields map.
- The `ScenarioResult` struct is the contract between scenario runners (PBI-02/03) and the aggregation layer (PBI-04). Define it here so all downstream PBIs reference the same type.
- Topic naming: `demo-4.15-<format>-<direction>-<8hex>`. Schema naming: `demo-4.15-<format>-<8hex>`. Generate the 8-hex suffix once at demo start and share across all cells for a given format (spec 6.3).
- The `runAllScenarios` placeholder should accept the Kafka broker address, sidecar client, and cleanup tracker and return a `[]ScenarioResult`. Downstream PBIs will fill in the body.
