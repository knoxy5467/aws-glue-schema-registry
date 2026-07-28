/**
 * Tier-1 offline unit tests for the bench case matrix and per-case builders.
 *
 * The load-bearing properties for the benchmark harness are:
 *
 *   1. Matrix cardinality. Both directions cover the full
 *      4 format-variants × 2 compression × 3 sizes grid — exactly 24 cases
 *      per direction and 48 in total. The comparison report's grid layout
 *      depends on this cardinality being exact; a missing or duplicated case
 *      would leave a cell blank or double-plotted in the rendered table.
 *
 *   2. Canonical case names. The report generator recovers each case's
 *      coordinates by splitting its `name` on `/`, so the name must encode
 *      format-variant, compression, size, and direction unambiguously, and
 *      every emitted name must be unique.
 *
 *   3. Round-trip correctness. Every encode case must produce a full wire
 *      message (18-byte header + non-empty payload), and every decode case
 *      must recover a `{ blob: string }` record whose blob length matches
 *      the case's declared payload size. This is what makes the bench
 *      "measuring real work" instead of measuring an empty path.
 *
 * Everything runs offline: pure in-process encode/decode via the
 * `@gsr/serde` facade, no network, no Docker, no filesystem I/O beyond
 * module resolution.
 */

import { describe, expect, it } from "vitest";

import { DataFormat } from "@gsr/serde";

import {
  BENCH_COMPRESSIONS,
  BENCH_FORMAT_VARIANTS,
  BENCH_SIZES,
  buildDecodeCase,
  buildEncodeCase,
  DECODE_CASES,
  ENCODE_CASES,
  type BenchCaseDescriptor,
} from "./cases.js";

const WIRE_FORMAT_HEADER_SIZE = 18;
const EXPECTED_CASES_PER_DIRECTION =
  BENCH_FORMAT_VARIANTS.length * BENCH_COMPRESSIONS.length * BENCH_SIZES.length;

describe("bench case matrix — cardinality and structure", () => {
  it("declares 4 format-variants, 2 compression modes, and 3 sizes", () => {
    // Guard the matrix inputs so a later drift in the coordinate lists
    // fails here, close to the cause, rather than silently changing the
    // 24-per-direction expectation below.
    expect(BENCH_FORMAT_VARIANTS).toHaveLength(4);
    expect(BENCH_COMPRESSIONS).toHaveLength(2);
    expect(BENCH_SIZES).toHaveLength(3);
    // 4 * 2 * 3 = 24 — locking the derived expectation avoids ambiguity if
    // any of the coordinate lists ever changes.
    expect(EXPECTED_CASES_PER_DIRECTION).toBe(24);
  });

  it("covers 24 encode cases and 24 decode cases (48 total)", () => {
    expect(ENCODE_CASES).toHaveLength(EXPECTED_CASES_PER_DIRECTION);
    expect(DECODE_CASES).toHaveLength(EXPECTED_CASES_PER_DIRECTION);
    expect(ENCODE_CASES.length + DECODE_CASES.length).toBe(48);
  });

  it("emits every (format-variant, compression, size) triple in both directions", () => {
    // Build the full expected coordinate set once, then subtract each
    // descriptor's coordinates. A leftover in either direction means the
    // matrix is missing a cell.
    for (const direction of ["encode", "decode"] as const) {
      const cases = direction === "encode" ? ENCODE_CASES : DECODE_CASES;
      const expected = new Set<string>();
      for (const variant of BENCH_FORMAT_VARIANTS) {
        for (const compression of BENCH_COMPRESSIONS) {
          for (const size of BENCH_SIZES) {
            expected.add(`${variant}|${compression}|${size.label}`);
          }
        }
      }
      for (const c of cases) {
        expected.delete(
          `${c.formatVariant}|${c.compression}|${c.size.label}`,
        );
      }
      expect(
        expected.size,
        `missing ${direction} coordinates: ${[...expected].join(", ")}`,
      ).toBe(0);
    }
  });

  it("assigns the right DataFormat to each format-variant", () => {
    // The variant → DataFormat mapping is what the facade routes on, and
    // both avro-generic and avro-specific must resolve to DataFormat.AVRO
    // (they diverge only in the decoded object's shape).
    const byVariant = new Map<string, DataFormat>();
    for (const c of ENCODE_CASES) {
      byVariant.set(c.formatVariant, c.format);
    }
    expect(byVariant.get("avro-generic")).toBe(DataFormat.AVRO);
    expect(byVariant.get("avro-specific")).toBe(DataFormat.AVRO);
    expect(byVariant.get("protobuf")).toBe(DataFormat.PROTOBUF);
    expect(byVariant.get("json")).toBe(DataFormat.JSON);
  });

  it("produces unique canonical case names", () => {
    const allNames = [
      ...ENCODE_CASES.map((c) => c.name),
      ...DECODE_CASES.map((c) => c.name),
    ];
    const unique = new Set(allNames);
    expect(unique.size).toBe(allNames.length);
    // The name format is what the report generator parses to recover cell
    // coordinates — assert a representative example locks the structure.
    expect(ENCODE_CASES[0]!.name).toBe(
      "avro-generic/comp-NONE/small_100B/encode",
    );
    // Every name must end with the direction segment.
    for (const c of ENCODE_CASES) {
      expect(c.name.endsWith("/encode")).toBe(true);
    }
    for (const c of DECODE_CASES) {
      expect(c.name.endsWith("/decode")).toBe(true);
    }
  });

  it("declares payload sizes 100, 10240, 1048576 in ascending order", () => {
    // Ascending is a stability invariant for the report's grid columns.
    const sizesInOrder = BENCH_SIZES.map((s) => s.bytes);
    expect(sizesInOrder).toEqual([100, 10240, 1048576]);
  });
});

describe("case builders — round-trip correctness", () => {
  it("buildEncodeCase produces a non-empty wire message for every encode descriptor", () => {
    for (const descriptor of ENCODE_CASES) {
      const encodeCase = buildEncodeCase(descriptor);
      const wire = encodeCase.serializer.serialize(encodeCase.request);
      expect(
        Buffer.isBuffer(wire),
        `${descriptor.name}: serialize output must be a Buffer`,
      ).toBe(true);
      expect(
        wire.length,
        `${descriptor.name}: wire length must exceed the 18-byte header`,
      ).toBeGreaterThan(WIRE_FORMAT_HEADER_SIZE);
      // Wire-format byte 0 is always the version marker `0x03`.
      expect(wire[0], `${descriptor.name}: first byte must be 0x03`).toBe(0x03);
    }
  });

  it("buildDecodeCase yields a buffer that round-trips to a {blob:string} record", () => {
    for (const descriptor of DECODE_CASES) {
      const decodeCase = buildDecodeCase(descriptor);
      expect(
        Buffer.isBuffer(decodeCase.encodedBuffer),
        `${descriptor.name}: encodedBuffer must be a Buffer`,
      ).toBe(true);
      expect(
        decodeCase.encodedBuffer.length,
        `${descriptor.name}: encodedBuffer must exceed the 18-byte header`,
      ).toBeGreaterThan(WIRE_FORMAT_HEADER_SIZE);

      const decoded = decodeCase.deserializer.deserialize(decodeCase.request);
      // The single field is `blob`, a string of the same byte length as the
      // payload the generator produced. Every alphabet byte is printable
      // ASCII so `.toString()` is lossless: the decoded string's length
      // equals the payload size in bytes.
      expect(
        (decoded as { blob: string }).blob.length,
        `${descriptor.name}: decoded blob length must equal payload size`,
      ).toBe(descriptor.size.bytes);
    }
  });

  it("buildEncodeCase pins the compression byte at wire[1] per descriptor", () => {
    // Compression byte 0x00 = NONE, 0x05 = ZLIB. This is the wire-header
    // property that lets the report label each cell's compression mode
    // just from the emitted bytes — the case matrix's compression axis
    // must actually reach the wire.
    const NONE_BYTE = 0x00;
    const ZLIB_BYTE = 0x05;
    for (const descriptor of ENCODE_CASES) {
      const encodeCase = buildEncodeCase(descriptor);
      const wire = encodeCase.serializer.serialize(encodeCase.request);
      const expected = descriptor.compression === "NONE" ? NONE_BYTE : ZLIB_BYTE;
      expect(
        wire[1],
        `${descriptor.name}: wire[1] compression byte must reflect the descriptor`,
      ).toBe(expected);
    }
  });

  it("rejects a mismatched direction on either builder", () => {
    // Guardrails against a caller passing an encode descriptor to
    // `buildDecodeCase` or vice versa — those would silently produce a
    // case that measures the wrong direction.
    const encodeDesc = ENCODE_CASES[0]!;
    const decodeDesc = DECODE_CASES[0]!;
    expect(() =>
      buildDecodeCase(encodeDesc as BenchCaseDescriptor),
    ).toThrow(/direction/);
    expect(() =>
      buildEncodeCase(decodeDesc as BenchCaseDescriptor),
    ).toThrow(/direction/);
  });
});
