# Golden-byte fixtures for cross-language wire-format parity

This directory holds binary fixtures the Go GSR client uses to prove
wire-format equivalence with the Java reference implementation (§5.5 of
the GSR Golang plan).

## Filename pattern

Each scenario is one `.bin` file named after the (schema-format,
record-type, compression, payload-shape) tuple it locks down:

```
<format>__<rec>__comp-<NONE|ZLIB>__<short-payload-tag>.bin
```

Examples (more get added as Phase 4 walks the §5.2 matrix):

- `wire-only__none__fixed-uuid__hello.bin` — the simplest possible
  fixture: the bare 18-byte wire-format header followed by an ASCII
  payload. Locks down the header layout (version byte = 0x03,
  compression byte = 0x00, 16-byte big-endian UUID) without
  involving any payload-format encoder. This is the scaffolding test
  that runs under `go test -short` even when the Java fixture-
  generator hasn't been run.
- `wire-only__zlib__fixed-uuid__hello.bin` — same as above but with
  the compression byte = 0x05 and a ZLIB-compressed payload. Locks
  down the zlib level (Java uses `Z_DEFAULT_COMPRESSION` ⇒ level 6;
  Go's `zlib.DefaultCompression` ⇒ level 6 — the same number, the
  golden file confirms they produce the same bytes).

## Regeneration command (Java side — TODO(phase 4))

The Java fixture-generator CLI does NOT exist yet. The plan tracks it
as part of Phase 4 (§5.5):

> 1. A short-lived Java tool generates byte fixtures for the same
>    (schema, payload, compression) triples the Go tests exercise.
>    Output goes to `testdata/golden/<scenario>.bin`.

When that tool lands, drop it under
`integration-tests/cmd/golden-gen-java/` (Maven submodule) and document
the invocation here. Expected shape:

```
mvn -pl integration-tests/cmd/golden-gen-java compile exec:java \
    -Dexec.mainClass=com.amazonaws.services.schemaregistry.tests.GoldenGen \
    -Dexec.args="--output ../../pkg/gsrserde-go/core/testdata/golden --scenario all"
```

Until the Java CLI lands, the seed fixtures committed here are
hand-crafted from the Java parity references doc-commented in
`wire_format.go` / `compression.go`. They are *regression* fixtures
locking down the Go side; they become *parity* fixtures the moment the
Java CLI exists and produces byte-identical output for the same inputs.

## How Go tests consume these

`pkg/gsrserde-go/core/golden_assert.go` (added in the same commit as
this README) exposes `AssertGoldenBytes(t, scenarioName, actual)`. It
loads `testdata/golden/<scenarioName>.bin` and `bytes.Equal`s actual
against it. The helper has a `-update` flag wired through
`flag.Bool("update-golden", false, ...)` so when the Java CLI lands,
the regeneration loop is a single `go test -update-golden` invocation.

Tests that exercise the golden path live in
`golden_bytes_parity_test.go`. They run under the default `go test`
(no integration tag) — the whole point is that the wire-format
contract is locked down at unit-test time, not only when AWS is in
reach.

## What is NOT in here

- Avro/JSON/Protobuf payload-encoded fixtures. Those require the Java
  fixture-generator to be authoritative. Adding them before the
  generator exists would only lock down Go-against-Go, which we
  already cover with property tests.
- UUID variation. All fixtures use the same fixed UUID
  `01020304-0506-0708-090a-0b0c0d0e0f10` so diffs across fixtures are
  attributable to schema/payload/compression, not to UUID changes.
