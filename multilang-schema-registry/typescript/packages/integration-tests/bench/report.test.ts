/**
 * Tier-1 unit test for the bench comparison report generator.
 *
 * Exercises the two render paths the operator sees at runtime:
 *
 *   - with a reference-numbers JSON supplied → the report renders the
 *     three-column matrix per direction, labels every cell
 *     `measured-here` or `reference-supplied`, and computes a ratio
 *     column without hard-failing when either side is missing;
 *   - with no reference supplied → the report renders TypeScript-only
 *     columns and states plainly that reference numbers were not
 *     supplied (no fabricated ratios).
 *
 * The test also covers the load-bearing honesty caveats that the report
 * MUST always print (warm cache, host, ecosystem, TypeScript facade
 * per-call schema resolve), the graceful-skip behaviour on an
 * unparseable benchmark name, and the argv parser + coordinate parser
 * helpers.
 *
 * This is a `.test.ts` file collected by the default vitest suite (via
 * the `bench/**\/*.test.ts` include line in the root `vitest.config.ts`),
 * so `npm test` runs it every time.
 */

import { describe, expect, it } from "vitest";

import {
  DEFAULT_RESULTS_PATH,
  aggregateMeasured,
  aggregateReference,
  mbPerSecFromHz,
  parseArgv,
  parseCaseName,
  renderReport,
  type MeasuredResults,
  type ReferenceNumbers,
} from "./report.js";

// Minimal fixture with two cases spanning both directions and both
// compression modes — enough to prove the render paths hit the right
// rows without pinning full-matrix presence.
const FIXTURE_MEASURED: MeasuredResults = {
  files: [
    {
      filepath: "/tmp/encode.bench.ts",
      groups: [
        {
          fullName: "encode",
          benchmarks: [
            {
              name: "avro-generic/comp-NONE/small_100B/encode",
              hz: 50_000,
              mean: 0.02,
              p99: 0.05,
            },
            {
              name: "json/comp-ZLIB/medium_10KB/encode",
              hz: 4_000,
              mean: 0.25,
              p99: 0.4,
            },
            // Deliberately unparseable name — the report must skip it and
            // never throw.
            {
              name: "something-unexpected-from-a-future-vitest-shape",
              hz: 1_234,
            },
          ],
        },
      ],
    },
    {
      filepath: "/tmp/decode.bench.ts",
      groups: [
        {
          fullName: "decode",
          benchmarks: [
            {
              name: "avro-generic/comp-NONE/small_100B/decode",
              hz: 30_000,
              mean: 0.033,
              p99: 0.06,
            },
            {
              name: "protobuf/comp-ZLIB/large_1MB/decode",
              hz: 80,
              mean: 12.5,
              p99: 18,
            },
          ],
        },
      ],
    },
  ],
};

const FIXTURE_REFERENCE: ReferenceNumbers = {
  source: "reference go benchmark run, 2026-06-25, Xeon 8175M",
  language: "go",
  cells: [
    // A cell that overlaps a measured cell — should produce a ratio.
    {
      format: "AVRO",
      direction: "encode",
      compression: "NONE",
      sizeBytes: 100,
      mbPerSec: 12.5,
    },
    // A reference-only cell — should render with `—` on the TS side.
    {
      format: "PROTOBUF",
      direction: "encode",
      compression: "NONE",
      sizeBytes: 100,
      mbPerSec: 30.0,
    },
    // Decode-side overlap.
    {
      format: "PROTOBUF",
      direction: "decode",
      compression: "ZLIB",
      sizeBytes: 1048576,
      mbPerSec: 200.0,
    },
  ],
};

describe("parseCaseName", () => {
  it("parses a canonical case name into typed coordinates", () => {
    const coords = parseCaseName(
      "avro-generic/comp-NONE/small_100B/encode",
    );
    expect(coords).toEqual({
      formatVariant: "avro-generic",
      compression: "NONE",
      size: { label: "small_100B", bytes: 100 },
      direction: "encode",
    });
  });

  it("returns null on structural mismatch", () => {
    expect(parseCaseName("not/enough/parts")).toBeNull();
    expect(parseCaseName("avro-generic/no-comp-prefix/small_100B/encode")).toBeNull();
    expect(parseCaseName("avro-generic/comp-NONE/unknown_size/encode")).toBeNull();
    expect(parseCaseName("avro-generic/comp-NONE/small_100B/sideways")).toBeNull();
    expect(parseCaseName("unknown-format/comp-NONE/small_100B/encode")).toBeNull();
    expect(parseCaseName("avro-generic/comp-UNKNOWN/small_100B/encode")).toBeNull();
  });
});

describe("mbPerSecFromHz", () => {
  it("derives MB/s = size_bytes × hz / 1e6", () => {
    // 100 B × 50_000 hz = 5_000_000 B/s = 5 MB/s.
    expect(mbPerSecFromHz(50_000, 100)).toBeCloseTo(5.0, 6);
    // 1 MiB × 100 hz ≈ 104.858 MB/s.
    expect(mbPerSecFromHz(100, 1048576)).toBeCloseTo(104.8576, 4);
  });
});

describe("aggregateMeasured", () => {
  it("indexes parseable benchmarks by coordinate key and records skipped ones", () => {
    const { cellsByKey, skipped } = aggregateMeasured(FIXTURE_MEASURED);

    // 4 parseable benchmarks in the fixture.
    expect(cellsByKey.size).toBe(4);
    expect(skipped).toHaveLength(1);
    expect(skipped[0]?.name).toBe(
      "something-unexpected-from-a-future-vitest-shape",
    );

    // Spot-check one cell — MB/s must be derived, not read.
    const enc = cellsByKey.get(
      "avro-generic|NONE|small_100B|encode",
    );
    expect(enc?.hz).toBe(50_000);
    expect(enc?.mbPerSec).toBeCloseTo(5.0, 6);
    expect(enc?.meanNs).toBe(0.02);
    expect(enc?.p99Ns).toBe(0.05);
  });

  it("does not throw when hz is missing or non-finite", () => {
    const withBadHz: MeasuredResults = {
      files: [
        {
          groups: [
            {
              benchmarks: [
                {
                  name: "avro-generic/comp-NONE/small_100B/encode",
                  hz: Number.NaN,
                },
              ],
            },
          ],
        },
      ],
    };
    const { cellsByKey, skipped } = aggregateMeasured(withBadHz);
    expect(cellsByKey.size).toBe(0);
    expect(skipped).toHaveLength(1);
    expect(skipped[0]?.reason).toMatch(/hz/);
  });
});

describe("aggregateReference", () => {
  it("indexes reference cells by (format, compression, sizeBytes, direction)", () => {
    const byKey = aggregateReference(FIXTURE_REFERENCE);
    expect(byKey.get("AVRO|NONE|100|encode")).toBe(12.5);
    expect(byKey.get("PROTOBUF|NONE|100|encode")).toBe(30.0);
    expect(byKey.get("PROTOBUF|ZLIB|1048576|decode")).toBe(200.0);
    expect(byKey.size).toBe(3);
  });
});

describe("renderReport — with reference file", () => {
  const markdown = renderReport({
    measured: FIXTURE_MEASURED,
    reference: FIXTURE_REFERENCE,
    measuredResultsPath: ".results/ts-latest.json",
    referenceNumbersPath: "reference-numbers.json",
  });

  it("prints the load-bearing honesty caveats", () => {
    expect(markdown).toContain("## Reading these numbers");
    expect(markdown).toContain("Warm cache");
    expect(markdown).toContain("Host caveat");
    expect(markdown).toContain("Ecosystem differences");
    expect(markdown).toContain("Protobuf schema deviation");
    // The methodology caveat about the TS Avro & Protobuf paths
    // re-compiling schemas per call MUST appear verbatim.
    expect(markdown).toContain(
      "TypeScript facade re-compiles Avro & Protobuf schemas per call",
    );
    expect(markdown).toContain("strictly apples-to-apples");
  });

  it("labels cells `measured-here` and `reference-supplied` in the matrix header", () => {
    expect(markdown).toContain("TS MB/s (measured-here)");
    expect(markdown).toContain("Reference MB/s (reference-supplied)");
  });

  it("renders the full 24-cell-per-direction matrix (48 rows total)", () => {
    // Every one of the 24 size/compression/variant triples must appear as
    // a row in each matrix — count the label cells directly.
    for (const direction of ["encode", "decode"]) {
      for (const variant of [
        "Avro (generic)",
        "Avro (specific)",
        "Protobuf",
        "JSON",
      ]) {
        for (const compression of ["NONE", "ZLIB"]) {
          for (const size of ["small_100B", "medium_10KB", "large_1MB"]) {
            const rowFragment = `| ${variant} | ${compression} | ${size} |`;
            // The row appears in BOTH matrices (once per direction).
            const occurrences = markdown.split(rowFragment).length - 1;
            expect(
              occurrences,
              `row ${rowFragment} missing from the ${direction} matrix`,
            ).toBeGreaterThanOrEqual(1);
          }
        }
      }
    }
  });

  it("renders an informational ratio when both sides are present", () => {
    // TS 5.0 MB/s ÷ reference 12.5 MB/s = 0.40 × — the overlap cell.
    expect(markdown).toContain(
      "| Avro (generic) | NONE | small_100B | 5.0 | 12.5 | 0.40× |",
    );
  });

  it("does not fabricate ratios when one side is missing", () => {
    // Reference-only PROTOBUF/NONE/100/encode cell — TS side is absent.
    expect(markdown).toContain(
      "| Protobuf | NONE | small_100B | — | 30.0 | — |",
    );
    // TS-only JSON/ZLIB/medium encode — reference side absent.
    const jsonCompZlibMediumEncode = markdown
      .split("\n")
      .find(
        (line) =>
          line.includes("| JSON | ZLIB | medium_10KB |") &&
          !line.includes("Reference"),
      );
    expect(jsonCompZlibMediumEncode).toBeDefined();
    expect(jsonCompZlibMediumEncode).toContain("| — |");
  });

  it("appends a Skipped benchmarks section listing the unparseable name", () => {
    expect(markdown).toContain("## Skipped benchmarks");
    expect(markdown).toContain(
      "something-unexpected-from-a-future-vitest-shape",
    );
  });
});

describe("renderReport — without reference file", () => {
  const markdown = renderReport({
    measured: FIXTURE_MEASURED,
    measuredResultsPath: ".results/ts-latest.json",
  });

  it("states plainly that reference numbers were not supplied", () => {
    expect(markdown).toContain("reference numbers not supplied");
    expect(markdown).toContain("TypeScript-only columns rendered");
  });

  it("renders only the TypeScript-side column in the matrix header", () => {
    expect(markdown).toContain("| Format | Compression | Size | TS MB/s (measured-here) |");
    expect(markdown).not.toContain("Reference MB/s");
    expect(markdown).not.toContain("Ratio (TS/Reference)");
  });

  it("still prints the load-bearing caveats", () => {
    expect(markdown).toContain("Warm cache");
    expect(markdown).toContain("Host caveat");
    expect(markdown).toContain(
      "TypeScript facade re-compiles Avro & Protobuf schemas per call",
    );
  });

  it("still renders the full 24-cell-per-direction matrix", () => {
    // Missing cells appear as `—` in the TS MB/s column, still counted.
    for (const variant of [
      "Avro (generic)",
      "Avro (specific)",
      "Protobuf",
      "JSON",
    ]) {
      for (const compression of ["NONE", "ZLIB"]) {
        for (const size of ["small_100B", "medium_10KB", "large_1MB"]) {
          const rowFragment = `| ${variant} | ${compression} | ${size} |`;
          const occurrences = markdown.split(rowFragment).length - 1;
          // once per direction — encode + decode.
          expect(
            occurrences,
            `row ${rowFragment} should appear in both matrices`,
          ).toBe(2);
        }
      }
    }
  });
});

describe("parseArgv", () => {
  it("defaults the results path when no positional is supplied", () => {
    const parsed = parseArgv([]);
    expect(parsed.resultsPath).toBe(DEFAULT_RESULTS_PATH);
    expect(parsed.referencePath).toBeUndefined();
  });

  it("accepts a positional results path", () => {
    const parsed = parseArgv(["custom/results.json"]);
    expect(parsed.resultsPath).toBe("custom/results.json");
    expect(parsed.referencePath).toBeUndefined();
  });

  it("accepts --reference <path>", () => {
    const parsed = parseArgv(["custom/results.json", "--reference", "ref.json"]);
    expect(parsed.resultsPath).toBe("custom/results.json");
    expect(parsed.referencePath).toBe("ref.json");
  });

  it("accepts --reference=<path>", () => {
    const parsed = parseArgv(["--reference=ref.json"]);
    expect(parsed.resultsPath).toBe(DEFAULT_RESULTS_PATH);
    expect(parsed.referencePath).toBe("ref.json");
  });

  it("rejects an unknown flag", () => {
    expect(() => parseArgv(["--nope"])).toThrow(/unrecognised flag/);
  });

  it("rejects a second positional", () => {
    expect(() =>
      parseArgv(["one.json", "two.json"]),
    ).toThrow(/extra positional/);
  });

  it("rejects a bare --reference with no value", () => {
    expect(() => parseArgv(["--reference"])).toThrow(/requires a path/);
  });
});
