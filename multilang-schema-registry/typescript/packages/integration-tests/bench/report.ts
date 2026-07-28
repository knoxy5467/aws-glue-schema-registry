/**
 * Comparison report generator for the encode/decode benchmark suites.
 *
 * `vite-node report.ts [<ts-results.json>] [--reference <reference-numbers.json>]`
 *
 * Inputs
 * ------
 *
 *   1. A TypeScript-side measured-results JSON — the file `vitest bench`
 *      writes when passed `--outputJson`. When the positional argument is
 *      omitted, `report.ts` reads the default capture path,
 *      `packages/integration-tests/bench/.results/ts-latest.json`.
 *      The generator consumes the subset of the vitest schema it needs:
 *
 *          {
 *            files: [
 *              {
 *                filepath: string,
 *                groups: [
 *                  {
 *                    fullName: string,
 *                    benchmarks: [
 *                      { name: string, hz: number, mean?: number, p99?: number, ... }
 *                    ]
 *                  }
 *                ]
 *              }
 *            ]
 *          }
 *
 *      The benchmark `name` is the canonical case name emitted by the
 *      encode/decode suites:
 *
 *          <format-variant>/comp-<compression>/<size-label>/<direction>
 *
 *      The report tolerates benchmarks whose name does not parse (skips
 *      them and prints a `> Warning:` line into the "Skipped benchmarks"
 *      section rather than throwing) so a future vitest version reshuffle
 *      degrades gracefully.
 *
 *   2. An OPTIONAL operator-supplied reference-numbers JSON with the
 *      `ReferenceNumbers` shape documented in the runbook — a transcribed
 *      set of Go (or Java) benchmark cells captured from a reference
 *      checkout (e.g. `go test -bench=...` on the reference Go client).
 *
 * Output
 * ------
 *
 * A markdown document written to stdout, containing:
 *
 *   - A fixed "Reading these numbers" preamble encoding the honesty
 *     discipline: warm-cache caveat, hardware/host caveat, the
 *     `avsc` / `protobufjs` / `ajv` ecosystem-differences note, and the
 *     methodology-caveat that the TypeScript facade RE-COMPILES the Avro
 *     and Protobuf schemas per call (only the JSON path's ajv validator is
 *     cached). Readers are told explicitly that a naive MB/s comparison
 *     against a reference client with warm-cached schemas is NOT strictly
 *     apples-to-apples for Avro/Protobuf.
 *   - The FULL matrix for each direction (24 cells): every cell either
 *     `measured-here` (present in the TS results JSON) or `reference-supplied`
 *     (present in the reference JSON) or explicitly marked absent. No
 *     cell is dropped to flatter the TS side.
 *   - When no reference file is supplied the report renders TS-only
 *     columns and states "reference numbers not supplied" (no fabricated
 *     ratios).
 *
 * The report is informational only — it MUST NOT hard-fail on any ratio
 * value. `main()` always exits 0 unless the results JSON is unreadable or
 * invalid.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

// ---------------------------------------------------------------------------
// Case-coordinate types (subset re-declared here to keep this module readable
// as a standalone report — no runtime dependency on cases.ts).
// ---------------------------------------------------------------------------

export type ReportFormatVariant =
  | "avro-generic"
  | "avro-specific"
  | "protobuf"
  | "json";

export type ReportCompression = "NONE" | "ZLIB";

export type ReportDirection = "encode" | "decode";

/** Payload size axis coordinate. */
export interface ReportSize {
  readonly label: string;
  readonly bytes: number;
}

export const REPORT_FORMAT_VARIANTS: readonly ReportFormatVariant[] = [
  "avro-generic",
  "avro-specific",
  "protobuf",
  "json",
];

export const REPORT_COMPRESSIONS: readonly ReportCompression[] = [
  "NONE",
  "ZLIB",
];

export const REPORT_SIZES: readonly ReportSize[] = [
  { label: "small_100B", bytes: 100 },
  { label: "medium_10KB", bytes: 10240 },
  { label: "large_1MB", bytes: 1048576 },
];

// ---------------------------------------------------------------------------
// Measured-results JSON schema (subset consumed by the report).
// ---------------------------------------------------------------------------

export interface MeasuredBenchmark {
  readonly name: string;
  readonly hz: number;
  readonly mean?: number;
  readonly p99?: number;
}

export interface MeasuredGroup {
  readonly fullName?: string;
  readonly benchmarks: readonly MeasuredBenchmark[];
}

export interface MeasuredFile {
  readonly filepath?: string;
  readonly groups: readonly MeasuredGroup[];
}

export interface MeasuredResults {
  readonly files: readonly MeasuredFile[];
}

// ---------------------------------------------------------------------------
// Reference-numbers JSON schema (operator-supplied — see runbook README.md).
// ---------------------------------------------------------------------------

export type ReferenceLanguage = "go" | "java";

/** Format keys on reference cells use the wire-format enum (Generic/Specific collapse to AVRO). */
export type ReferenceFormat = "AVRO" | "PROTOBUF" | "JSON";

export interface ReferenceCell {
  readonly format: ReferenceFormat;
  readonly direction: ReportDirection;
  readonly compression: ReportCompression;
  readonly sizeBytes: number;
  readonly mbPerSec: number;
}

export interface ReferenceNumbers {
  readonly source: string;
  readonly language: ReferenceLanguage;
  readonly cells: readonly ReferenceCell[];
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/**
 * Coordinates parsed out of a canonical benchmark name of the form
 * `<format-variant>/comp-<compression>/<size-label>/<direction>`.
 */
export interface CaseCoordinates {
  readonly formatVariant: ReportFormatVariant;
  readonly compression: ReportCompression;
  readonly size: ReportSize;
  readonly direction: ReportDirection;
}

/**
 * Parse a canonical benchmark name into typed cell coordinates. Returns
 * `null` on any structural mismatch — the caller records a warning and
 * skips the benchmark rather than throwing.
 */
export function parseCaseName(name: string): CaseCoordinates | null {
  const parts = name.split("/");
  if (parts.length !== 4) {
    return null;
  }
  const [variantPart, compressionPart, sizePart, directionPart] = parts;
  if (
    variantPart === undefined ||
    compressionPart === undefined ||
    sizePart === undefined ||
    directionPart === undefined
  ) {
    return null;
  }

  if (!REPORT_FORMAT_VARIANTS.includes(variantPart as ReportFormatVariant)) {
    return null;
  }
  const formatVariant = variantPart as ReportFormatVariant;

  if (!compressionPart.startsWith("comp-")) {
    return null;
  }
  const compressionValue = compressionPart.slice("comp-".length);
  if (!REPORT_COMPRESSIONS.includes(compressionValue as ReportCompression)) {
    return null;
  }
  const compression = compressionValue as ReportCompression;

  const size = REPORT_SIZES.find((s) => s.label === sizePart);
  if (!size) {
    return null;
  }

  if (directionPart !== "encode" && directionPart !== "decode") {
    return null;
  }
  const direction = directionPart;

  return { formatVariant, compression, size, direction };
}

/**
 * Reduce the four format-variants to the three wire-format enum keys used
 * on the reference side. `avro-generic` and `avro-specific` both collapse
 * to `AVRO` (Generic and Specific share encode-side wire bytes and the
 * reference does not distinguish the decoded-shape flag).
 */
function referenceFormatKey(variant: ReportFormatVariant): ReferenceFormat {
  switch (variant) {
    case "avro-generic":
    case "avro-specific":
      return "AVRO";
    case "protobuf":
      return "PROTOBUF";
    case "json":
      return "JSON";
  }
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/**
 * The MB/s throughput of source payload for a benchmark. Derived from the
 * tinybench `hz` (operations/second) and the payload byte-length so the
 * value lines up with the reference's MB/s tables.
 *
 * MB/s = payload_bytes × hz / 1_000_000
 *
 * The 1_000_000 denominator matches the reference's Go bench-compare
 * convention (MB = 10^6 bytes, not MiB = 2^20 bytes). Both sides use the
 * same denominator so ratios are apples-to-apples.
 */
export function mbPerSecFromHz(hz: number, sizeBytes: number): number {
  return (sizeBytes * hz) / 1_000_000;
}

/** One measured cell resolved to its coordinates + throughput values. */
export interface MeasuredCell {
  readonly coords: CaseCoordinates;
  readonly hz: number;
  readonly mbPerSec: number;
  readonly meanNs?: number;
  readonly p99Ns?: number;
}

/** A benchmark whose name did not parse — retained for the "Skipped" section. */
export interface SkippedBenchmark {
  readonly name: string;
  readonly reason: string;
}

/** Result of consuming a measured-results JSON. */
export interface AggregatedMeasured {
  readonly cellsByKey: ReadonlyMap<string, MeasuredCell>;
  readonly skipped: readonly SkippedBenchmark[];
}

/** Coordinate key used for both measured + reference lookups (`variant|compression|sizeLabel|direction`). */
function coordinateKey(coords: CaseCoordinates): string {
  return `${coords.formatVariant}|${coords.compression}|${coords.size.label}|${coords.direction}`;
}

/**
 * Coordinate-key for a reference cell. Because the reference collapses
 * both Avro variants onto `AVRO`, a single reference cell fans out to both
 * `avro-generic` and `avro-specific` rows in the report — the fan-out is
 * done at lookup time (see `renderReport`), not here.
 */
function referenceKey(
  format: ReferenceFormat,
  compression: ReportCompression,
  sizeBytes: number,
  direction: ReportDirection,
): string {
  return `${format}|${compression}|${sizeBytes}|${direction}`;
}

/**
 * Reduce a `MeasuredResults` object to a `Map<coordKey, MeasuredCell>`
 * plus a list of skipped benchmarks. Never throws on a benchmark it
 * cannot map — records a warning and moves on.
 */
export function aggregateMeasured(results: MeasuredResults): AggregatedMeasured {
  const cellsByKey = new Map<string, MeasuredCell>();
  const skipped: SkippedBenchmark[] = [];

  for (const file of results.files ?? []) {
    for (const group of file.groups ?? []) {
      for (const benchmark of group.benchmarks ?? []) {
        const coords = parseCaseName(benchmark.name);
        if (!coords) {
          skipped.push({
            name: benchmark.name,
            reason: "benchmark name did not match the expected <variant>/comp-<compression>/<size>/<direction> shape",
          });
          continue;
        }
        if (typeof benchmark.hz !== "number" || !Number.isFinite(benchmark.hz)) {
          skipped.push({
            name: benchmark.name,
            reason: "missing or non-finite hz field",
          });
          continue;
        }
        const key = coordinateKey(coords);
        cellsByKey.set(key, {
          coords,
          hz: benchmark.hz,
          mbPerSec: mbPerSecFromHz(benchmark.hz, coords.size.bytes),
          meanNs: typeof benchmark.mean === "number" ? benchmark.mean : undefined,
          p99Ns: typeof benchmark.p99 === "number" ? benchmark.p99 : undefined,
        });
      }
    }
  }

  return { cellsByKey, skipped };
}

/**
 * Reduce a `ReferenceNumbers` object to a `Map<refKey, mbPerSec>` keyed
 * on `(format, compression, sizeBytes, direction)`.
 */
export function aggregateReference(
  reference: ReferenceNumbers,
): ReadonlyMap<string, number> {
  const out = new Map<string, number>();
  for (const cell of reference.cells) {
    const key = referenceKey(
      cell.format,
      cell.compression,
      cell.sizeBytes,
      cell.direction,
    );
    out.set(key, cell.mbPerSec);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

const HONESTY_PREAMBLE = `## Reading these numbers

These numbers are informational. They characterise the shipped TypeScript
serializer/deserializer against the already-validated reference client;
they are NOT a pass/fail gate.

- **Warm cache.** Every cell is measured at steady state. The schema is
  supplied to the request in its already-parsed / already-loaded form and
  the serializer/deserializer instance is constructed once per case
  before the measurement loop starts. First-call schema-compile cost is
  outside the measured hot loop.
- **Host caveat.** The TypeScript and reference (Go / Java) numbers were
  almost certainly captured on different hosts unless the operator
  explicitly co-located them. Absolute throughput values are host-bound;
  the ratio between two cells captured on the SAME host is the
  interesting quantity.
- **Ecosystem differences (expected, not hidden).** The TypeScript client
  composes over \`avsc\`, \`protobufjs\`, and \`ajv\`. The reference
  composes over \`hamba/avro\` (Go) or \`avro-tools\` (Java),
  \`google.golang.org/protobuf\` / \`protobuf-java\`, and a
  \`json-schema-validator\` equivalent. Library-choice differences produce
  ecosystem-level performance deltas that are NOT client-design defects
  and MUST NOT be edited out of the report.
- **Protobuf schema deviation.** The reference client uses
  \`bytes blob = 1\` in its perf fixture; this TypeScript benchmark uses
  \`string blob = 1\` so the same printable-ASCII payload round-trips
  through \`protobufjs\` as a JavaScript string. The generated payload
  bytes are identical on both sides; only the field wire-type differs.
- **TypeScript facade re-compiles Avro & Protobuf schemas per call.**
  This is a load-bearing methodology caveat. The public facade
  (\`GsrSerializer.serialize\` / \`GsrDeserializer.deserialize\`) accepts
  a schema OBJECT per request and passes it through the format serdes on
  every call. Under the current implementation:
    - Avro (\`avsc\`) — \`Type.forSchema(...)\` runs per call. There is
      no per-request compiled-type cache in the Avro path.
    - Protobuf (\`protobufjs\`) — \`.proto\` text is parsed per call.
      \`protobufjs\` has an internal parser cache; a real caller who
      hands the same schema instance in every time hits the cache, but
      the per-call resolve is still on the hot path.
    - JSON (\`ajv\`) — \`ajv.compile(...)\` runs per call, but ajv
      caches compiled validators internally by canonicalized schema, so
      the steady-state cost is a cache lookup.
  The reference client's bench, by contrast, resolves and warms the
  compiled type ONCE outside the measurement loop and measures a pure
  encode/decode against the already-warm type. This means a naive MB/s
  comparison across languages is **NOT** strictly apples-to-apples for
  the Avro and Protobuf rows — the TypeScript numbers include per-call
  schema-resolve work that the reference numbers do not. The JSON rows
  are the closest to apples-to-apples because ajv caches internally. A
  narrower comparison that isolates the format layer (which is the load-
  bearing quantity for anyone tuning production hot paths) is planned as
  a follow-up and is NOT covered by this report.
- **No hidden cells.** Every case in the matrix is emitted, including
  cases the reference does not measure and cases where the TypeScript
  suite did not produce a number.`;

/** Human-legible row label for the format axis. */
function formatVariantLabel(variant: ReportFormatVariant): string {
  switch (variant) {
    case "avro-generic":
      return "Avro (generic)";
    case "avro-specific":
      return "Avro (specific)";
    case "protobuf":
      return "Protobuf";
    case "json":
      return "JSON";
  }
}

/** Format a MB/s number with one decimal, or a placeholder. */
function fmtMbps(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) {
    return "—";
  }
  return value.toFixed(1);
}

/** Format a ratio ×N (informational only). */
function fmtRatio(measured: number | undefined, reference: number | undefined): string {
  if (
    measured === undefined ||
    reference === undefined ||
    !Number.isFinite(measured) ||
    !Number.isFinite(reference) ||
    reference === 0
  ) {
    return "—";
  }
  const ratio = measured / reference;
  return `${ratio.toFixed(2)}×`;
}

export interface RenderReportInput {
  readonly measured: MeasuredResults;
  readonly reference?: ReferenceNumbers;
  /** Path of the measured-results JSON, printed in the report header. */
  readonly measuredResultsPath?: string;
  /** Path of the reference-numbers JSON, printed in the report header. */
  readonly referenceNumbersPath?: string;
}

/**
 * Compose the full markdown comparison report. Pure function of its
 * inputs — used by the CLI entry point and by the unit test to exercise
 * both render paths (with and without a reference file).
 */
export function renderReport(input: RenderReportInput): string {
  const aggregatedMeasured = aggregateMeasured(input.measured);
  const referenceByKey = input.reference
    ? aggregateReference(input.reference)
    : undefined;

  const lines: string[] = [];

  // Header + provenance ------------------------------------------------------
  lines.push("# GSR TypeScript benchmark comparison report");
  lines.push("");
  lines.push(
    `- **TypeScript results:** ${input.measuredResultsPath ?? "(in-memory)"}`,
  );
  if (referenceByKey && input.reference) {
    lines.push(
      `- **Reference source:** ${input.reference.source} (${input.reference.language})`,
    );
    if (input.referenceNumbersPath) {
      lines.push(`- **Reference file:** ${input.referenceNumbersPath}`);
    }
  } else {
    lines.push(
      "- **Reference:** reference numbers not supplied — TypeScript-only columns rendered.",
    );
  }
  lines.push("");

  // Fixed honesty preamble ---------------------------------------------------
  lines.push(HONESTY_PREAMBLE);
  lines.push("");

  // Matrix per direction -----------------------------------------------------
  for (const direction of ["encode", "decode"] as const) {
    lines.push(`## ${direction === "encode" ? "Encode" : "Decode"} matrix`);
    lines.push("");
    lines.push(
      "Every cell carries an explicit provenance tag: `measured-here` (from" +
        " the TypeScript results JSON) or `reference-supplied` (from the" +
        " operator-supplied reference-numbers JSON). A `—` in the ratio" +
        " column means one side is missing; the report does not fabricate" +
        " a ratio.",
    );
    lines.push("");

    if (referenceByKey) {
      lines.push(
        "| Format | Compression | Size | TS MB/s (measured-here) | Reference MB/s (reference-supplied) | Ratio (TS/Reference) |",
      );
      lines.push(
        "|---|---|---|---:|---:|---:|",
      );
    } else {
      lines.push(
        "| Format | Compression | Size | TS MB/s (measured-here) |",
      );
      lines.push("|---|---|---|---:|");
    }

    for (const variant of REPORT_FORMAT_VARIANTS) {
      for (const compression of REPORT_COMPRESSIONS) {
        for (const size of REPORT_SIZES) {
          const coords: CaseCoordinates = {
            formatVariant: variant,
            compression,
            size,
            direction,
          };
          const measuredCell = aggregatedMeasured.cellsByKey.get(
            coordinateKey(coords),
          );
          const tsMbps = measuredCell?.mbPerSec;

          if (referenceByKey) {
            const refMbps = referenceByKey.get(
              referenceKey(
                referenceFormatKey(variant),
                compression,
                size.bytes,
                direction,
              ),
            );
            lines.push(
              `| ${formatVariantLabel(variant)} | ${compression} | ${size.label} | ${fmtMbps(tsMbps)} | ${fmtMbps(refMbps)} | ${fmtRatio(tsMbps, refMbps)} |`,
            );
          } else {
            lines.push(
              `| ${formatVariantLabel(variant)} | ${compression} | ${size.label} | ${fmtMbps(tsMbps)} |`,
            );
          }
        }
      }
    }
    lines.push("");
  }

  // ops/sec detail (informational) ------------------------------------------
  lines.push("## Raw TypeScript throughput (ops/sec, mean-ns, p99-ns)");
  lines.push("");
  lines.push(
    "Raw tinybench numbers from the measured-results JSON. `mean` and" +
      " `p99` are in nanoseconds per operation.",
  );
  lines.push("");
  lines.push("| Case | ops/sec | mean (ns) | p99 (ns) |");
  lines.push("|---|---:|---:|---:|");

  const measuredCellList = Array.from(aggregatedMeasured.cellsByKey.values());
  measuredCellList.sort((a, b) =>
    a.coords.direction === b.coords.direction
      ? a.coords.formatVariant.localeCompare(b.coords.formatVariant) ||
        a.coords.compression.localeCompare(b.coords.compression) ||
        a.coords.size.bytes - b.coords.size.bytes
      : a.coords.direction.localeCompare(b.coords.direction),
  );

  for (const cell of measuredCellList) {
    const nameSlug = `${cell.coords.formatVariant}/comp-${cell.coords.compression}/${cell.coords.size.label}/${cell.coords.direction}`;
    lines.push(
      `| ${nameSlug} | ${cell.hz.toFixed(1)} | ${cell.meanNs !== undefined ? cell.meanNs.toFixed(3) : "—"} | ${cell.p99Ns !== undefined ? cell.p99Ns.toFixed(3) : "—"} |`,
    );
  }
  lines.push("");

  // Skipped-benchmarks note (only when there are any) ------------------------
  if (aggregatedMeasured.skipped.length > 0) {
    lines.push("## Skipped benchmarks");
    lines.push("");
    lines.push(
      "The following measured benchmarks were skipped because their names" +
        " did not parse into the expected case-coordinate shape. This is" +
        " typically caused by a vitest schema shift or an unrelated bench" +
        " file being collected under the same config.",
    );
    lines.push("");
    for (const s of aggregatedMeasured.skipped) {
      lines.push(`- \`${s.name}\` — ${s.reason}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/** Argv shape parsed by the CLI. */
export interface ParsedArgv {
  readonly resultsPath: string;
  readonly referencePath?: string;
}

/** Default results path relative to the monorepo root — the fixed capture path. */
export const DEFAULT_RESULTS_PATH =
  "packages/integration-tests/bench/.results/ts-latest.json";

/**
 * Parse the CLI argv (excluding the leading `node` + script args). Accepts
 * either or both of:
 *
 *   - a single positional path (the results JSON),
 *   - `--reference <path>` (the reference-numbers JSON).
 *
 * When the positional is omitted the caller falls back to
 * `DEFAULT_RESULTS_PATH`.
 */
export function parseArgv(argv: readonly string[]): ParsedArgv {
  let resultsPath: string | undefined;
  let referencePath: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === undefined) {
      continue;
    }
    if (token === "--reference") {
      const value = argv[i + 1];
      if (!value) {
        throw new Error("--reference requires a path argument");
      }
      referencePath = value;
      i += 1;
      continue;
    }
    if (token.startsWith("--reference=")) {
      referencePath = token.slice("--reference=".length);
      continue;
    }
    if (token.startsWith("-")) {
      throw new Error(`unrecognised flag: ${token}`);
    }
    if (resultsPath !== undefined) {
      throw new Error(
        `unexpected extra positional argument: ${token} (already consuming results from ${resultsPath})`,
      );
    }
    resultsPath = token;
  }

  return {
    resultsPath: resultsPath ?? DEFAULT_RESULTS_PATH,
    referencePath,
  };
}

/** Read + JSON.parse a file, tagging failures with the caller-supplied kind for diagnostic clarity. */
function readJsonFile<T>(path: string, kind: string): T {
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`failed to read ${kind} file at ${path}: ${message}`);
  }
  try {
    return JSON.parse(raw) as T;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`failed to parse ${kind} JSON at ${path}: ${message}`);
  }
}

/**
 * CLI entry point. Reads the results JSON (and optional reference-numbers
 * JSON), renders the markdown, and writes it to `stdout`. Returns the
 * process exit code — 0 on success, 2 on an unreadable/unparseable input.
 * NEVER exits non-zero on a throughput value or ratio.
 */
export function main(argv: readonly string[], cwd: string = process.cwd()): number {
  const parsed = parseArgv(argv);
  const resultsPath = resolve(cwd, parsed.resultsPath);
  const referencePath = parsed.referencePath
    ? resolve(cwd, parsed.referencePath)
    : undefined;

  let measured: MeasuredResults;
  try {
    measured = readJsonFile<MeasuredResults>(resultsPath, "measured-results");
  } catch (err) {
    process.stderr.write(
      `${err instanceof Error ? err.message : String(err)}\n`,
    );
    return 2;
  }

  let reference: ReferenceNumbers | undefined;
  if (referencePath) {
    try {
      reference = readJsonFile<ReferenceNumbers>(
        referencePath,
        "reference-numbers",
      );
    } catch (err) {
      process.stderr.write(
        `${err instanceof Error ? err.message : String(err)}\n`,
      );
      return 2;
    }
  }

  const markdown = renderReport({
    measured,
    reference,
    measuredResultsPath: parsed.resultsPath,
    referenceNumbersPath: parsed.referencePath,
  });
  process.stdout.write(`${markdown}\n`);
  return 0;
}

// Guard the CLI so the unit test can import the module without triggering
// a stdout write. Both `node` (via a compiled shim) and `vite-node` load
// this file as an ES module — we detect the CLI case by checking whether
// the currently-executing module's URL points at this file's basename.
//
// vite-node keeps its OWN binary in `process.argv[1]`, so the classic
// `require.main === module` shape does not work here. Instead we check
// `import.meta.url` against this file's expected suffix — a stable
// identifier the test process (vitest) will never accidentally produce.
const isMain = ((): boolean => {
  const metaUrl = import.meta.url;
  if (typeof metaUrl !== "string") return false;
  // Test import goes through vitest's transform pipeline and the URL
  // ends the same way (`report.ts`), so also require that this process
  // was invoked via a CLI binary — vitest sets a `VITEST` env var, so
  // its absence is a reliable "not the test runner" signal.
  if (process.env.VITEST) return false;
  return metaUrl.endsWith("/packages/integration-tests/bench/report.ts");
})();

if (isMain) {
  const exitCode = main(process.argv.slice(2));
  if (exitCode !== 0) {
    process.exitCode = exitCode;
  }
}
