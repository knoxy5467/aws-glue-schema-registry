/**
 * WIRE BYTE-IDENTITY GATE — offline golden-vector conformance suite.
 *
 * This test suite is the exit condition for the byte-identical
 * wire-format core. For every Java-canonical golden vector captured by
 * `golden-gen-java`, it asserts:
 *
 *   1. The `.bin` bytes on disk hash to the SHA-256 recorded in its `.json`
 *      descriptor (oracle integrity — the vectors have not drifted).
 *   2. The TypeScript encode path — the format's `serde` slice composed with
 *      `@gsr/core.encodeMessage` — produces the FULL wire bytes (18-byte
 *      header + optional ZLIB compression + payload) byte-for-byte identical
 *      to the golden `.bin`.
 *   3. `@gsr/core.decodeMessage` combined with the format's `serde` decode
 *      reconstructs the canonical record from the golden bytes (offline
 *      round-trip correctness).
 *
 * The suite reads every cell dynamically from `PROVENANCE.md`'s "Captured
 * cells" markdown table, so the enumeration is AUDITED FROM THE ORACLE
 * rather than hard-coded — if the table changes, the gate tracks it.
 *
 * A dedicated check also asserts that `packages/core/src/` contains no
 * Kafka import — the wire core is transport-agnostic.
 *
 * Everything runs offline: no network, no Docker, no live Glue registration.
 * The fixed schema-version UUID recorded in every descriptor is injected
 * directly into `encodeWireHeader` so the full 18-byte header is
 * byte-checkable without any registry interaction.
 *
 * All cells (avro / protobuf / jsonschema × NONE / ZLIB) pass byte-identity
 * against the Java-canonical vectors on the shapes captured under
 * `testdata/golden-java/`. `packages/core/src/wire/compression.ts` uses pako
 * (a pure-JS port of stock zlib) rather than Node's bundled
 * `zlib.deflateSync`, so deflate output matches the Java `Deflater` defaults
 * bit-for-bit. The gate stays STRICT (no waiver, no skip) — any regression
 * on any cell fails the file.
 *
 * ## Evidence scope
 *
 * This gate proves byte-identity **on the committed golden-vector set**,
 * not on arbitrary payloads. Concretely: 18 cells → 14 distinct SHA-256
 * vectors, one Avro record shape, one Protobuf message type
 * (`google.protobuf.StringValue`, single-field), five JSON-Schema
 * Draft-07 shapes, captured 2026-07-23 at a pinned reference-monorepo
 * SHA. Vectors are committed and re-consulted per gate invocation — the
 * gate is not a live Java run. Live cross-language parity is exercised
 * by the Tier-3 interop suites and the narrated demo instead.
 * See `testdata/golden-java/PROVENANCE.md` for the full breakdown.
 */

import { createHash } from "node:crypto";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  decodeMessage,
  encodeMessage,
  type CompressionType,
} from "@gsr/core";
import {
  avroHazardRecords,
  decodeAvro,
  decodeJsonSchema,
  decodeProtobuf,
  encodeAvro,
  encodeJsonSchema,
  encodeProtobuf,
  jsonSchemaHazardRecords,
  protobufHazardRecords,
  WRAPPERS_PROTO_TEXT,
} from "@gsr/serde/internal";

import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
  SHARED_TEST_DIR,
} from "../../src/reference-root.js";

const PROVENANCE_PATH = resolve(GOLDEN_JAVA_DIR, "PROVENANCE.md");

type Format = "avro" | "protobuf" | "jsonschema";
type Compression = "NONE" | "ZLIB";

/** One row of the `PROVENANCE.md` "Captured cells" table. */
interface CapturedCell {
  format: Format;
  recordType: string;
  compression: Compression;
  file: string;
  byteLength: number;
  sha256: string;
}

/** The `.json` descriptor sitting alongside each `.bin`. */
interface GoldenDescriptor {
  format: Format;
  recordType: string;
  compression: Compression;
  schemaVersionId: string;
  sourceSchema: string;
  payloadTag: string;
  sha256: string;
  byteLength: number;
}

/**
 * Parse the "Captured cells" markdown table out of `PROVENANCE.md`. The
 * table shape is fixed by the `golden-gen-java` generator (see the reference
 * repo). Rows are pipe-separated with a header + separator + one row per
 * captured cell. This function reads whatever is on disk; the caller
 * asserts the count is non-zero.
 */
function parseProvenanceTable(content: string): CapturedCell[] {
  const lines = content.split(/\r?\n/);
  const start = lines.findIndex((line) =>
    line.startsWith("| Format |"),
  );
  if (start < 0) {
    throw new Error(
      "PROVENANCE.md is missing the '| Format |' captured-cells table header",
    );
  }
  const rows: CapturedCell[] = [];
  // Skip header (start) and the separator row (start + 1); iterate data rows.
  for (let i = start + 2; i < lines.length; i++) {
    const line = lines[i];
    if (line === undefined || !line.startsWith("|")) break;
    const cells = line
      .split("|")
      .slice(1, -1)
      .map((cell) => cell.trim());
    if (cells.length < 6) continue;
    const [format, recordType, compression, fileCell, byteLengthCell, shaCell] =
      cells;
    if (
      format === undefined ||
      recordType === undefined ||
      compression === undefined ||
      fileCell === undefined ||
      byteLengthCell === undefined ||
      shaCell === undefined
    ) {
      continue;
    }
    // The file cell is wrapped in backticks: `avro__generic__comp-NONE__test-v1.bin`.
    const file = fileCell.replace(/`/g, "");
    // The sha256 cell is wrapped in backticks.
    const sha256 = shaCell.replace(/`/g, "");
    const byteLength = Number.parseInt(byteLengthCell, 10);
    if (!isValidFormat(format) || !isValidCompression(compression)) {
      continue;
    }
    rows.push({
      format,
      recordType,
      compression,
      file,
      byteLength,
      sha256,
    });
  }
  return rows;
}

function isValidFormat(value: string): value is Format {
  return value === "avro" || value === "protobuf" || value === "jsonschema";
}

function isValidCompression(value: string): value is Compression {
  return value === "NONE" || value === "ZLIB";
}

function sha256Hex(buf: Buffer): string {
  return createHash("sha256").update(buf).digest("hex");
}

/**
 * Rich diagnostic for a byte-identity failure. Surfaces the produced-vs-golden
 * hex so a reviewer can tell immediately where the divergence sits. Compression
 * is handled by pako in `packages/core/src/wire/compression.ts`, so a ZLIB
 * mismatch is a real regression — the hint points there rather than at Node's
 * bundled zlib.
 */
function formatByteMismatchDiagnostic(
  cell: CapturedCell,
  produced: Buffer,
  golden: Buffer,
): string {
  const hint =
    cell.compression === "ZLIB"
      ? [
          "",
          "Hint: ZLIB-compressed cell diverged. Compression runs through pako",
          "      (`packages/core/src/wire/compression.ts`), which tracks stock",
          "      zlib bit-for-bit. Check that pako was not swapped for another",
          "      deflate implementation and that the payload has not drifted",
          "      relative to the golden vector.",
        ].join("\n")
      : [
          "",
          "Hint: NONE-compressed cell diverged — this is a format-encoder",
          "      issue (avro / protobuf / jsonschema), not a compression",
          "      issue. Check the corresponding serde slice.",
        ].join("\n");

  return [
    `byte-identity mismatch on ${cell.file}`,
    `  format      = ${cell.format}/${cell.recordType}`,
    `  compression = ${cell.compression}`,
    `  golden      = ${golden.length} bytes, sha256 ${sha256Hex(golden)}`,
    `  produced    = ${produced.length} bytes, sha256 ${sha256Hex(produced)}`,
    `  golden hex   : ${golden.toString("hex")}`,
    `  produced hex : ${produced.toString("hex")}`,
    hint,
  ].join("\n");
}

function readDescriptor(binFile: string): GoldenDescriptor {
  const jsonFile = binFile.replace(/\.bin$/, ".json");
  const raw = readFileSync(resolve(GOLDEN_JAVA_DIR, jsonFile), "utf8");
  const parsed = JSON.parse(raw) as GoldenDescriptor;
  return parsed;
}

/**
 * Compose the FULL wire bytes for a captured cell via the core codec + the
 * matching serde slice. The output is what `encodeMessage` produces given
 * the canonical record for the cell; it should equal the golden `.bin`
 * byte-for-byte.
 */
function encodeFullWire(
  cell: CapturedCell,
  descriptor: GoldenDescriptor,
): Buffer {
  const compressionType: CompressionType = cell.compression;
  switch (cell.format) {
    case "avro": {
      const hazard = avroHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) {
        throw new Error(
          `no avroHazardRecords entry for payloadTag "${descriptor.payloadTag}"`,
        );
      }
      const schemaPath = resolve(SHARED_TEST_DIR, hazard.schemaPath);
      const payload = encodeAvro(schemaPath, hazard.record);
      return encodeMessage({
        schemaVersionId: descriptor.schemaVersionId,
        payload,
        compressionType,
      });
    }
    case "protobuf": {
      const hazard = protobufHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) {
        throw new Error(
          `no protobufHazardRecords entry for payloadTag "${descriptor.payloadTag}"`,
        );
      }
      // The protobuf serde slice returns `messageIndex-varint || body` from
      // its own encode; but the core codec owns the message-index composition
      // step so callers cannot get the ordering wrong. To route through the
      // core codec, hand it the bare protobuf body and let it prepend the
      // varint pre-compression.
      const body = encodeProtobufBody(
        hazard.protoSchemaText,
        hazard.messageFullName,
        hazard.record,
      );
      return encodeMessage({
        schemaVersionId: descriptor.schemaVersionId,
        payload: body,
        compressionType,
        protobuf: {
          schemaText: hazard.protoSchemaText,
          messageFullName: hazard.messageFullName,
        },
      });
    }
    case "jsonschema": {
      const hazard = jsonSchemaHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) {
        throw new Error(
          `no jsonSchemaHazardRecords entry for payloadTag "${descriptor.payloadTag}"`,
        );
      }
      const schemaPath = resolve(SHARED_TEST_DIR, hazard.schemaPath);
      const payload = encodeJsonSchema(schemaPath, hazard.instance);
      return encodeMessage({
        schemaVersionId: descriptor.schemaVersionId,
        payload,
        compressionType,
      });
    }
  }
}

/**
 * Encode just the raw protobuf message body (no message-index varint) via
 * the serde slice's public output. `encodeProtobuf` returns
 * `varint || body`; the varint is a single byte at index 0 for
 * `google.protobuf.StringValue` (message-index 6, encoded as `0x06`). The
 * core codec re-prepends the varint after computing the message-index
 * itself, so this helper strips the slice's leading varint before handing
 * the body to the codec.
 */
function encodeProtobufBody(
  protoSchemaText: string,
  messageFullName: string,
  record: object,
): Buffer {
  const withIndex = encodeProtobuf(protoSchemaText, messageFullName, record);
  // Consume the unsigned varint (up to 5 bytes) from the head of the buffer.
  let pos = 0;
  while (pos < withIndex.length && pos < 5) {
    const b = withIndex[pos] as number;
    pos++;
    if ((b & 0x80) === 0) break;
  }
  return withIndex.subarray(pos);
}

/**
 * Decode a full wire message and reconstruct the canonical record via the
 * matching serde slice. Returns the reconstructed record so the caller can
 * assert it deep-equals the golden hazard entry.
 */
function decodeFullWire(
  cell: CapturedCell,
  descriptor: GoldenDescriptor,
  bytes: Buffer,
): unknown {
  const isProtobuf = cell.format === "protobuf";
  const result = decodeMessage(bytes, isProtobuf ? { protobuf: true } : undefined);
  switch (cell.format) {
    case "avro": {
      const hazard = avroHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) throw new Error("missing avro hazard");
      const schemaPath = resolve(SHARED_TEST_DIR, hazard.schemaPath);
      return decodeAvro(result.payload, schemaPath);
    }
    case "protobuf": {
      const hazard = protobufHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) throw new Error("missing protobuf hazard");
      // decodeMessage stripped the message-index for us — hand the bare body
      // to protobufjs. decodeProtobuf itself expects `varint || body`, so
      // wrap the body with a `0x00` varint (index 0) that decodeProtobuf
      // will re-strip. This is a small round-trip that keeps the slice's
      // public API stable while the codec owns the index composition.
      const wrapped = Buffer.concat([Buffer.from([0x00]), result.payload]);
      return decodeProtobuf(
        wrapped,
        hazard.protoSchemaText,
        hazard.messageFullName,
      );
    }
    case "jsonschema": {
      const hazard = jsonSchemaHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      if (!hazard) throw new Error("missing jsonschema hazard");
      const schemaPath = resolve(SHARED_TEST_DIR, hazard.schemaPath);
      return decodeJsonSchema(result.payload, schemaPath);
    }
  }
}

function canonicalRecordFor(
  cell: CapturedCell,
  descriptor: GoldenDescriptor,
): unknown {
  switch (cell.format) {
    case "avro": {
      const hazard = avroHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      return hazard?.record;
    }
    case "protobuf": {
      const hazard = protobufHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      return hazard?.record;
    }
    case "jsonschema": {
      const hazard = jsonSchemaHazardRecords.find(
        (h) => h.tag === descriptor.payloadTag,
      );
      return hazard?.instance;
    }
  }
}

/**
 * Assertion-count FLOOR — the guardrail that turns "silent skip" into a hard
 * failure. The parametrized suite below is wrapped in
 * `describe.skipIf(!referenceRootExists)` so a fresh clone that has not yet
 * checked out the reference tree can `npm test` without an ENOENT during
 * collection; vitest does NOT fail on a skipped suite, so if the walk-up in
 * `../../src/reference-root.ts` ever silently drifts (a package-dir rename,
 * a fixture-tree move) the whole byte-identity gate would go quiet while
 * `npm test` stayed green — same silent-skip hazard as the original
 * hermetic-resolution defect.
 *
 * This floor block is deliberately OUTSIDE the `describe.skipIf`. It:
 *   1. Reads the committed `multilang-schema-registry/fixtures.manifest.json`
 *      and counts every `testdata/golden-java/*.bin` entry — the authoritative
 *      expected floor, so a manifest expansion automatically raises the bar.
 *   2. Asserts `referenceRootExists === true`. Hermetic in-repo resolution
 *      must succeed on every environment the test runs in (developer box,
 *      CI, fresh clone) — a `false` here is exactly the silent-skip drift
 *      class this floor is designed to catch.
 *   3. Asserts the parametrized-suite oracle enumerated at least the manifest
 *      count of cells. If the parser degrades to a placeholder or the
 *      captured-cells table gets truncated, the count drops and the floor
 *      breaks — a REGRESSION rather than a quiet no-op.
 *   4. Verifies every golden `.bin` on disk matches its committed SHA-256 in
 *      the manifest. `scripts/verify-fixtures.mjs` runs the equivalent check
 *      as a CI step before `npm test` — this narrow re-check closes the
 *      bare-`npm test`-in-isolation gap so local content drift is caught
 *      here rather than surfacing only as an opaque per-cell mismatch.
 *
 * If a future change genuinely intends to run this suite in an environment
 * without the reference tree, the floor must be updated deliberately (the
 * manifest is the single source of truth); a silent skip is no longer a
 * possibility.
 */
describe("wire byte-identity gate — assertion-count floor (silent-skip guard)", () => {
  // fixtures.manifest.json lives at the monorepo root:
  //   integration-tests/test/gate → typescript → multilang-schema-registry
  const FIXTURES_MANIFEST_PATH = resolve(
    dirname(fileURLToPath(import.meta.url)),
    "../../../../..",
    "fixtures.manifest.json",
  );

  interface FixtureManifestEntry {
    path: string;
    size: number;
    sha256: string;
  }
  interface FixtureManifest {
    entries: FixtureManifestEntry[];
  }

  const manifestRaw = readFileSync(FIXTURES_MANIFEST_PATH, "utf8");
  const manifest = JSON.parse(manifestRaw) as FixtureManifest;
  const goldenBinEntries = manifest.entries.filter((e) =>
    /^testdata\/golden-java\/[^/]+\.bin$/.test(e.path),
  );
  const expectedGoldenCellCount = goldenBinEntries.length;
  // The manifest lives at the monorepo root, so its dirname is that root.
  // Derived once so the SHA cross-check below and the manifest resolver stay
  // in lock-step.
  const MONOREPO_ROOT = dirname(FIXTURES_MANIFEST_PATH);

  it("reads the committed golden-cell floor from fixtures.manifest.json", () => {
    // Sanity: the manifest itself must not be empty / corrupt. If this ever
    // trips it's a monorepo-integrity signal — the byte-identity gate is
    // meaningless without an oracle.
    expect(expectedGoldenCellCount).toBeGreaterThan(0);
  });

  it("resolves the hermetic in-repo reference root (no silent skip)", () => {
    // If this fails, the reference-root walk-up in
    // `packages/integration-tests/src/reference-root.ts` has drifted (e.g.
    // a package-dir rename broke the `../../../..` chain). Repairs go there;
    // do NOT lower this assertion. This is what turns the `describe.skipIf`
    // below from "silently skipped" into "loudly red" when resolution breaks.
    expect(
      referenceRootExists,
      `referenceRootExists is false — the byte-identity gate below would ` +
        `silently skip. Check the in-repo walk-up in ` +
        `packages/integration-tests/src/reference-root.ts. ` +
        `Expected golden-cell floor: ${String(expectedGoldenCellCount)}.`,
    ).toBe(true);
  });

  it("enumerates at least the manifest-committed number of golden cells", () => {
    // `capturedCells` is read from PROVENANCE.md at module scope below. If
    // parsing degrades (table truncation, header rename, placeholder
    // fallback), this floor fails — a REGRESSION signal rather than a
    // quiet 0-assertion pass on the parametrized suite.
    expect(
      capturedCells.length,
      `PROVENANCE.md yielded ${String(capturedCells.length)} captured cells; ` +
        `manifest lists ${String(expectedGoldenCellCount)}. ` +
        `The byte-identity suite would run fewer assertions than expected.`,
    ).toBeGreaterThanOrEqual(expectedGoldenCellCount);
  });

  it("golden .bin files match the SHA-256 committed in fixtures.manifest.json", () => {
    // Defense-in-depth for the bare-`npm test`-in-isolation path. CI runs
    // `scripts/verify-fixtures.mjs` before this step (see
    // `.github/workflows/typescript-ci.yml`), which already SHA-checks every
    // fixture against the manifest. A local `npm test` skips that CI step,
    // so this assertion re-runs the same check narrowly on the golden `.bin`
    // subset — the files the byte-identity gate consumes as its oracle.
    // Content drift (a golden vector edited without regenerating the
    // manifest) fails HERE rather than showing up only as an obscure
    // per-cell byte mismatch downstream.
    const drifted: string[] = [];
    for (const entry of goldenBinEntries) {
      const abs = resolve(MONOREPO_ROOT, entry.path);
      const bytes = readFileSync(abs);
      const actual = createHash("sha256").update(bytes).digest("hex");
      if (bytes.length !== entry.size || actual !== entry.sha256) {
        drifted.push(
          `${entry.path}: expected ${String(entry.size)} bytes / ${entry.sha256}, ` +
            `got ${String(bytes.length)} bytes / ${actual}`,
        );
      }
    }
    expect(
      drifted,
      `${String(drifted.length)} golden .bin file(s) drifted from ` +
        `fixtures.manifest.json:\n  • ${drifted.join("\n  • ")}\n` +
        `Regenerate with 'node multilang-schema-registry/scripts/verify-fixtures.mjs --write' ` +
        `if the drift is intentional.`,
    ).toEqual([]);
  });
});

// Load the oracle once at module scope so the parametrized `it.each` blocks
// can enumerate cells from a stable snapshot. On a fresh clone / CI without
// the reference-repo checkout the module-scope read would ENOENT during
// collection; guard the read behind `referenceRootExists` so the suite skips
// cleanly instead. `it.each([])` refuses an empty array, so the fallback is
// a one-element placeholder that the outer `describe.skipIf` never runs.
const capturedCells: CapturedCell[] = referenceRootExists
  ? (() => {
      const provenanceContent = readFileSync(PROVENANCE_PATH, "utf8");
      const rows = parseProvenanceTable(provenanceContent);
      // Sanity — with the reference present, the oracle must be non-empty.
      // If it is empty the whole test file must fail loudly rather than
      // silently pass.
      if (rows.length === 0) {
        throw new Error(
          `PROVENANCE.md at ${PROVENANCE_PATH} yielded zero captured cells; ` +
            `the golden-vector oracle appears empty or malformed`,
        );
      }
      return rows;
    })()
  : [
      // Placeholder so `it.each([])` — which vitest rejects — is never
      // reached; the outer `describe.skipIf(!referenceRootExists)` prevents
      // execution.
      {
        format: "avro",
        recordType: "placeholder",
        compression: "NONE",
        file: "placeholder.bin",
        byteLength: 0,
        sha256: "placeholder",
      },
    ];

describe.skipIf(!referenceRootExists)("wire byte-identity gate — offline golden-vector", () => {
  it("enumerates the captured-cells table from PROVENANCE.md", () => {
    // The count is AUDITED FROM THE ORACLE, not hard-coded. This assertion
    // exists so a regression that shrinks or empties the table is visible;
    // it does NOT lock in the current count.
    expect(capturedCells.length).toBeGreaterThan(0);
    for (const cell of capturedCells) {
      expect(cell.file).toMatch(/\.bin$/);
      expect(cell.sha256).toMatch(/^[0-9a-f]{64}$/);
      expect(cell.byteLength).toBeGreaterThan(0);
    }
  });

  describe("cell coverage", () => {
    it.each(capturedCells)(
      "$format/$recordType/$compression — $file byte-identity + round-trip",
      (cell) => {
        const binPath = resolve(GOLDEN_JAVA_DIR, cell.file);
        const golden = readFileSync(binPath);

        // (a) Oracle integrity: on-disk SHA-256 matches the descriptor.
        expect(golden.length).toBe(cell.byteLength);
        expect(sha256Hex(golden)).toBe(cell.sha256);

        // The `.json` descriptor is also on disk and its sha256 must match
        // the PROVENANCE row (defence-in-depth against a table/descriptor
        // drift).
        const descriptor = readDescriptor(cell.file);
        expect(descriptor.sha256).toBe(cell.sha256);
        expect(descriptor.byteLength).toBe(cell.byteLength);
        expect(descriptor.format).toBe(cell.format);
        expect(descriptor.recordType).toBe(cell.recordType);
        expect(descriptor.compression).toBe(cell.compression);

        // (b) Encode byte-identity: full wire bytes reproduce the golden.
        const produced = encodeFullWire(cell, descriptor);
        expect(
          produced.equals(golden),
          formatByteMismatchDiagnostic(cell, produced, golden),
        ).toBe(true);

        // (c) Decode correctness: golden bytes round-trip to the canonical
        //     record. Independent of encode byte-identity — any valid RFC
        //     1951 deflate output decompresses to the same plaintext, so
        //     this signal stays truthful even if encode ever regresses.
        const decoded = decodeFullWire(cell, descriptor, golden);
        expect(decoded).toEqual(canonicalRecordFor(cell, descriptor));
      },
    );
  });

  describe("format-specific invariants", () => {
    it("locks the protobuf message-index at 6 for StringValue", () => {
      // Verifies the message-index > 0 case is exercised — the second byte
      // of the varint region (or the first, since 6 fits in one byte) must
      // be 0x06. Encoded via the slice; the varint region is the head of
      // the returned buffer.
      const withIndex = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        "google.protobuf.StringValue",
        { value: "hello" },
      );
      expect(withIndex[0]).toBe(0x06);
    });

    it("covers every captured protobuf cell (index-6 case among them)", () => {
      const protobufCells = capturedCells.filter(
        (cell) => cell.format === "protobuf",
      );
      expect(protobufCells.length).toBeGreaterThan(0);
      for (const cell of protobufCells) {
        // Every protobuf cell in the oracle should have a matching hazard
        // record and its descriptor's payloadTag should resolve.
        const descriptor = readDescriptor(cell.file);
        const hazard = protobufHazardRecords.find(
          (h) => h.tag === descriptor.payloadTag,
        );
        expect(hazard).toBeDefined();
      }
    });

    it("covers every jsonschema cell with a hazard instance", () => {
      const jsonCells = capturedCells.filter(
        (cell) => cell.format === "jsonschema",
      );
      expect(jsonCells.length).toBeGreaterThan(0);
      const uniqueTags = new Set<string>();
      for (const cell of jsonCells) {
        const descriptor = readDescriptor(cell.file);
        uniqueTags.add(descriptor.payloadTag);
        const hazard = jsonSchemaHazardRecords.find(
          (h) => h.tag === descriptor.payloadTag,
        );
        expect(hazard).toBeDefined();
      }
      // Every unique jsonschema payloadTag in the oracle should be mirrored
      // by a fixture entry — the slice's authored set must cover the
      // oracle's authored set.
      expect(uniqueTags.size).toBe(jsonSchemaHazardRecords.length);
    });

    it("covers every avro cell with a fixture record", () => {
      const avroCells = capturedCells.filter(
        (cell) => cell.format === "avro",
      );
      expect(avroCells.length).toBeGreaterThan(0);
      for (const cell of avroCells) {
        const descriptor = readDescriptor(cell.file);
        const hazard = avroHazardRecords.find(
          (h) => h.tag === descriptor.payloadTag,
        );
        expect(hazard).toBeDefined();
      }
    });
  });

  describe("transport-agnostic invariant", () => {
    /**
     * Walk `packages/core/src/` and assert no source file imports or
     * requires anything named `kafka` (case-insensitive). The wire core is
     * transport-agnostic and MUST NOT depend on any Kafka library.
     */
    const packageRoot = resolve(
      dirname(fileURLToPath(import.meta.url)),
      "../../../..", // integration-tests/test/gate → typescript/
    );
    const coreSrcDir = resolve(packageRoot, "packages/core/src");

    function walkTsFiles(dir: string): string[] {
      const out: string[] = [];
      for (const entry of readdirSync(dir)) {
        const full = resolve(dir, entry);
        const info = statSync(full);
        if (info.isDirectory()) {
          out.push(...walkTsFiles(full));
        } else if (entry.endsWith(".ts")) {
          out.push(full);
        }
      }
      return out;
    }

    it("packages/core contains no Kafka import", () => {
      const tsFiles = walkTsFiles(coreSrcDir);
      expect(tsFiles.length).toBeGreaterThan(0);
      const kafkaHits: Array<{ file: string; line: string }> = [];
      for (const file of tsFiles) {
        // Skip test files — the invariant is about SHIPPED source.
        if (file.endsWith(".test.ts")) continue;
        const source = readFileSync(file, "utf8");
        const lines = source.split(/\r?\n/);
        for (const line of lines) {
          // Look for any import/require that names a "kafka" package.
          if (
            /import[^\n]*['"][^'"]*kafka[^'"]*['"]/i.test(line) ||
            /require\s*\(\s*['"][^'"]*kafka[^'"]*['"]/i.test(line)
          ) {
            kafkaHits.push({ file, line });
          }
        }
      }
      expect(kafkaHits).toEqual([]);
    });
  });
});
