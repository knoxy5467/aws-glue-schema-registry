# TypeScript GSR benchmarks — operator runbook

This directory contains the benchmark suites, the case matrix, the payload
generator, and the comparison-report generator. Everything here is
**informational only** — nothing under `bench/` is a pass/fail gate, and no
throughput value or ratio ever fails a build.

The benchmark exercises the pure encode/decode paths of the `@gsr/serde`
public facade — no network, no AWS Glue registration, no Kafka, no JVM. It
runs offline on a laptop.

## What is here

| File | Purpose |
|---|---|
| `payloads.ts` | Deterministic xorshift64 payload generator + per-format fixture schemas. Byte-identical to the reference client's generator for a given size (asserted by `payloads.test.ts` against a pinned SHA-256 hex digest). |
| `cases.ts` | The format × compression × size × direction case matrix (24 cells per direction, 48 total) and the per-case builders that pre-construct the serializer + request. |
| `encode.bench.ts` | `vitest bench` suite that iterates the 24 encode cases and measures `GsrSerializer.serialize()` throughput. |
| `decode.bench.ts` | `vitest bench` suite that iterates the 24 decode cases and measures `GsrDeserializer.deserialize()` throughput on a pre-encoded buffer. |
| `report.ts` | Comparison-report generator — reads the measured-results JSON and an optional operator-supplied reference-numbers JSON and emits a markdown comparison document to stdout. |
| `report.test.ts` | Tier-1 unit test covering both render paths of `report.ts` (with and without a reference file). |
| `reference-numbers.example.json` | Operator template for the reference-numbers JSON — a starting point to copy and fill in with numbers transcribed from the reference client's benchmark output. |
| `bench-config.test.ts`, `cases.test.ts`, `payloads.test.ts`, `report.test.ts` | Tier-1 unit tests collected by the default `npm test` run. They verify the scaffolding — matrix cardinality, payload determinism, config resolution, report render paths — but do NOT load any `.bench.ts` file. |

The results directory `packages/integration-tests/bench/.results/` is
created on demand by `npm run bench` and is git-ignored. It holds transient
output only; nothing under it is committed.

## Running the benchmark

From the monorepo root (`multilang-schema-registry/typescript/`):

```sh
npm run build          # produces the compiled @gsr/core + @gsr/serde bundles
npm run bench          # runs both bench suites and writes .results/ts-latest.json
```

`npm run bench` is defined as:

```sh
vitest bench --run --config vitest.bench.config.ts \
  --outputJson packages/integration-tests/bench/.results/ts-latest.json \
  packages/integration-tests/bench
```

The `--outputJson` flag is load-bearing: it captures the machine-readable
throughput numbers that `report.ts` consumes. Without it, the report has
nothing to read.

The suites take ~1–2 minutes to run on a modern developer laptop
(dominated by the 1 MiB × ZLIB cells).

### Preserving a run for later comparison

`vitest bench --outputJson` always writes to the same path
(`.results/ts-latest.json`), overwriting the previous run. To keep a
timestamped history, copy the file after the run:

```sh
cp packages/integration-tests/bench/.results/ts-latest.json \
   packages/integration-tests/bench/.results/ts-$(date -u +%Y%m%dT%H%M%SZ).json
```

The `.results/` dir is git-ignored so nothing is accidentally committed.

## Producing reference numbers

The comparison report is optional. When you supply a reference-numbers
JSON, the report labels every cell `measured-here` or `reference-supplied`
and shows a ratio column. When you do not, the report shows only the
TypeScript-side numbers and states plainly that no reference was supplied
— it fabricates nothing.

Reference numbers come from the already-validated reference client
(the reference Go or Java `aws-glue-schema-registry` implementation in a
companion checkout). The reference toolchain is not runnable from this
repo — you need a `go` toolchain (for the Go benchmarks) or a JDK +
Maven + JMH install (for the Java benchmarks). Rather than shell out
to a Go/JDK build from here, this repo consumes reference numbers as a
**transcribed JSON file** the operator produces once.

### Step-by-step

1. Check out the reference client on the host you want to compare
   against.
2. Run the reference benchmark harness (`make bench-go` or
   `make bench-java` in the reference `perf/` directory, whichever
   applies to the checkout you're comparing against). Both write
   baselines under `perf/baselines/` and print a markdown table into
   `perf/README.md`.
3. Read the resulting MB/s numbers. Each row is a `(format, direction,
   compression, sizeBytes)` cell.
4. Transcribe the cells you care about into a `reference-numbers.json`
   matching the schema below. `reference-numbers.example.json` in this
   directory is a starting template.
5. Do NOT commit the resulting `reference-numbers.json` unless you want
   the specific host + date to be part of the checked-in history. Most
   operators keep it in `.results/` or a scratch dir.

Ideally the reference bench runs on the SAME host as the TypeScript bench
so absolute throughput values are comparable. If you compared cells
captured on different hosts, say so in the `source` field so the report
carries that context in its header.

### Reference-numbers JSON schema

```ts
interface ReferenceNumbers {
  // Free-text provenance line surfaced in the report header. Include
  // enough context to identify the benchmark run: reference branch,
  // command run, date, host + CPU model.
  source: string;

  // Which reference client the numbers came from.
  language: "go" | "java";

  // One entry per cell you want the report to compare. Cells absent
  // here render as `—` on the reference side; the report never
  // fabricates a missing cell.
  cells: Array<{
    format: "AVRO" | "PROTOBUF" | "JSON";
    direction: "encode" | "decode";
    compression: "NONE" | "ZLIB";
    sizeBytes: number;    // must match one of {100, 10240, 1048576}
    mbPerSec: number;     // throughput of source payload in MB/s (10^6)
  }>;
}
```

Notes:

- `format` on a reference cell is one of the three wire-format enum
  values. The two Avro variants on the TypeScript side (`avro-generic`
  and `avro-specific`) both compare against the single `AVRO` reference
  cell — the reference does not distinguish the decoded-shape flag.
- `sizeBytes` MUST match one of the three canonical sizes exactly
  (`100`, `10240`, `1048576`). Any other value is ignored.
- `mbPerSec` uses the same 10⁶-byte denominator convention the
  reference `make bench-compare` output uses (MB, not MiB). Both sides
  use this denominator so ratios are apples-to-apples.

## Reading the report

Generate a report from the last bench run:

```sh
npm run bench:report
```

That is defined as `vite-node packages/integration-tests/bench/report.ts`
— it reads the default results path (`packages/integration-tests/bench/.results/ts-latest.json`)
and prints markdown to stdout. Redirect to a file if you want to keep it:

```sh
npm run bench:report > packages/integration-tests/bench/.results/latest-report.md
```

To include a reference comparison, pass `--reference`:

```sh
npx vite-node packages/integration-tests/bench/report.ts \
  packages/integration-tests/bench/.results/ts-latest.json \
  --reference packages/integration-tests/bench/reference-numbers.example.json \
  > report.md
```

### `report.ts` argv contract

```
report.ts [<ts-results.json>] [--reference <reference-numbers.json>]
```

- Positional (optional): the TypeScript results JSON produced by
  `vitest bench --outputJson`. Defaults to
  `packages/integration-tests/bench/.results/ts-latest.json` when
  omitted.
- `--reference <path>` (optional): the operator-supplied reference-numbers
  JSON. When omitted, the report renders TypeScript-only columns and
  states plainly that reference numbers were not supplied.

The report generator exits 0 on success. It exits non-zero ONLY when the
inputs are unreadable or unparseable — never on a throughput value or a
ratio.

### What the report contains

- A "Reading these numbers" preamble encoding the load-bearing honesty
  discipline:
  - warm-cache framing (schemas are supplied to the request already
    parsed and the serializer/deserializer instance is constructed once
    before the measurement loop),
  - a hardware/host caveat (TypeScript and reference numbers were
    almost certainly captured on different hosts unless the operator
    states otherwise),
  - an ecosystem-differences note (`avsc` vs `hamba/avro`, `protobufjs`
    vs the Go/Java protobuf libraries, `ajv` vs a JSON-Schema equivalent),
  - the Protobuf `string blob = 1` vs `bytes blob = 1` schema deviation
    (documented — the payload bytes are byte-identical on both sides;
    only the field wire-type differs),
  - the **methodology caveat that the TypeScript facade re-compiles
    Avro & Protobuf schemas per call** through the public API. The
    reference client warms its compiled type once and measures a pure
    encode/decode; the TypeScript numbers include per-call schema
    resolve work. This means a naive MB/s comparison across languages
    is NOT strictly apples-to-apples for the Avro and Protobuf rows.
    JSON is closer to apples-to-apples because ajv caches internally.
- A full 24-cell-per-direction matrix (48 rows total). Every cell is
  labelled `measured-here` (from the TypeScript results JSON) or
  `reference-supplied` (from the reference-numbers JSON). Cells missing
  on one side render as `—`; the ratio column shows `—` rather than a
  fabricated value.
- A "Raw TypeScript throughput" table listing every measured case with
  its ops/sec, mean-ns/op, and p99-ns/op — the raw tinybench numbers.
- A "Skipped benchmarks" section listing any benchmarks whose names did
  not parse into the expected case-coordinate shape. This is the
  graceful-fallback path if a future vitest version reshuffles its JSON
  schema — the report degrades to a warning rather than crashing.

Nothing in the report is a pass/fail signal. It is a human-read
comparison document.

## Interpreting throughput numbers

Read the report as a characterization, not a scoreboard:

- The MB/s value is source-payload MB/s (payload byte length × hz / 10⁶).
- Compare cells on the SAME row of the matrix (same format, same
  compression, same size) to compare the two client implementations.
- Cross-row comparisons (e.g. NONE vs ZLIB, or 100 B vs 1 MiB) are
  informative about the encode/decode + compression cost curve but
  are NOT client-vs-client comparisons.
- If a Protobuf cell shows a large gap in favour of the reference,
  remember the `bytes` vs `string` deviation: the TypeScript path does
  UTF-8 encode/decode on every byte, and the reference path does a
  straight `memcpy`. This is a fixture choice, not a client-design
  regression.
- If any Avro or Protobuf cell shows the TypeScript path slower than
  the reference by a large factor, the per-call schema-resolve cost
  (documented above) is the first thing to check — it is inside the
  measured loop on the TypeScript side and outside the measured loop
  on the reference side.

## Adding a new case

Extend `cases.ts`'s coordinate lists (`BENCH_FORMAT_VARIANTS`,
`BENCH_COMPRESSIONS`, `BENCH_SIZES`). Add a matching bench in the
appropriate `.bench.ts` file if the new coordinate needs a bespoke
setup, or let the existing loop pick it up if the builders handle it
already. Update the `cases.test.ts` cardinality assertion. The report
generator picks up the new coordinate automatically as long as the
canonical name still follows the `<variant>/comp-<compression>/<size>/<direction>`
shape.
