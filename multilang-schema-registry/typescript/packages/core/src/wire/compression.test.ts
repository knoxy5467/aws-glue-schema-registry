import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { WIRE_FORMAT_HEADER_SIZE } from "./constants.js";
import {
  compressionByteForType,
  compressionTypeForByte,
  compressZlib,
  decompressZlib,
} from "./compression.js";
import { GsrIncompatibleDataError } from "./errors.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
} from "../test-support/reference-root.js";

/**
 * The uncompressed avsc-encoded body of the `test_v1.avsc` record
 * `{data:"hello", count:7}`, taken from the payload region of the
 * NONE-compressed avro generic golden vector. Kept as a literal so this
 * test doesn't depend on the header codec or the serde slice.
 */
const AVRO_TEST_V1_PAYLOAD = Buffer.from([
  0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0e,
]);

const ZLIB_HEADER = Buffer.from([0x78, 0x9c]);
const GO_SYNC_FLUSH_MARKER = Buffer.from([0x00, 0x00, 0xff, 0xff]);

/**
 * Every Java ZLIB golden cell, paired with its NONE counterpart. The NONE
 * payload region (after the 18-byte header) is the plaintext the Java
 * canonical deflates; the ZLIB compressed region is the byte-identical
 * target we must reproduce with pako. Parametrizing over every cell keeps
 * the byte-identity assertion honest across payload sizes — a single
 * short-payload smoke test would pass on Node's Chromium zlib for the
 * 7-byte avro plaintext (which happens to deflate identically) and only
 * regress on the larger JSON-Schema payloads that diverge above ~20 bytes.
 */
const ZLIB_GOLDEN_CELLS: ReadonlyArray<{ zlib: string; none: string }> = [
  {
    zlib: "avro__generic__comp-ZLIB__test-v1.bin",
    none: "avro__generic__comp-NONE__test-v1.bin",
  },
  {
    zlib: "avro__specific__comp-ZLIB__test-v1.bin",
    none: "avro__specific__comp-NONE__test-v1.bin",
  },
  {
    zlib: "protobuf__concrete__comp-ZLIB__stringvalue-hello.bin",
    none: "protobuf__concrete__comp-NONE__stringvalue-hello.bin",
  },
  {
    zlib: "protobuf__dynamic__comp-ZLIB__stringvalue-hello.bin",
    none: "protobuf__dynamic__comp-NONE__stringvalue-hello.bin",
  },
  {
    zlib: "jsonschema__draft07__comp-ZLIB__product-v1.bin",
    none: "jsonschema__draft07__comp-NONE__product-v1.bin",
  },
  {
    zlib: "jsonschema__draft07__comp-ZLIB__customer-v1.bin",
    none: "jsonschema__draft07__comp-NONE__customer-v1.bin",
  },
  {
    zlib: "jsonschema__draft07__comp-ZLIB__invoice-v1.bin",
    none: "jsonschema__draft07__comp-NONE__invoice-v1.bin",
  },
  {
    zlib: "jsonschema__draft07__comp-ZLIB__event-v1.bin",
    none: "jsonschema__draft07__comp-NONE__event-v1.bin",
  },
  {
    zlib: "jsonschema__draft07__comp-ZLIB__single.bin",
    none: "jsonschema__draft07__comp-NONE__single.bin",
  },
];

function readGolden(name: string): Buffer {
  return readFileSync(resolve(GOLDEN_JAVA_DIR, name));
}

describe("wire/compression", () => {
  describe.skipIf(!referenceRootExists)(
    "compressZlib byte-identity vs Java golden",
    () => {
      // One parameterized test per ZLIB cell — the whole point of the pako
      // swap is that these ALL match. If any regress, the byte-identity
      // gate re-reddens.
      it.each(ZLIB_GOLDEN_CELLS)(
        "matches Java golden compressed region for $zlib",
        ({ zlib, none }) => {
          const plaintext = readGolden(none).subarray(WIRE_FORMAT_HEADER_SIZE);
          const goldenCompressed = readGolden(zlib).subarray(
            WIRE_FORMAT_HEADER_SIZE,
          );

          const produced = compressZlib(plaintext);

          expect(produced.equals(goldenCompressed)).toBe(true);
        },
      );

      it("emits output that begins with the zlib default-level header 78 9c", () => {
        const compressed = compressZlib(AVRO_TEST_V1_PAYLOAD);
        expect(compressed.subarray(0, 2).equals(ZLIB_HEADER)).toBe(true);
      });

      it("does not emit Go's 00 00 ff ff sync-flush framing on any golden payload", () => {
        // Exercise across every plaintext payload; a sync-flush marker in
        // the middle of a JSON payload would be indistinguishable from a
        // one-shot deflate coincidence, so check every one.
        for (const { none } of ZLIB_GOLDEN_CELLS) {
          const plaintext = readGolden(none).subarray(WIRE_FORMAT_HEADER_SIZE);
          const compressed = compressZlib(plaintext);
          expect(compressed.includes(GO_SYNC_FLUSH_MARKER)).toBe(false);
        }
      });
    },
  );

  describe("decompressZlib", () => {
    it("round-trips compressZlib output back to the original payload", () => {
      const original = AVRO_TEST_V1_PAYLOAD;
      const roundTripped = decompressZlib(compressZlib(original));
      expect(roundTripped.equals(original)).toBe(true);
    });

    it.skipIf(!referenceRootExists).each(ZLIB_GOLDEN_CELLS)(
      "decompresses Java golden compressed region for $zlib",
      ({ zlib, none }) => {
        const plaintext = readGolden(none).subarray(WIRE_FORMAT_HEADER_SIZE);
        const goldenCompressed = readGolden(zlib).subarray(
          WIRE_FORMAT_HEADER_SIZE,
        );

        const decompressed = decompressZlib(goldenCompressed);

        expect(decompressed.equals(plaintext)).toBe(true);
      },
    );

    it.skipIf(!referenceRootExists).each(ZLIB_GOLDEN_CELLS)(
      "round-trips through compressZlib/decompressZlib for $zlib",
      ({ none }) => {
        const plaintext = readGolden(none).subarray(WIRE_FORMAT_HEADER_SIZE);
        const roundTripped = decompressZlib(compressZlib(plaintext));
        expect(roundTripped.equals(plaintext)).toBe(true);
      },
    );

    describe("negative paths", () => {
      // pako 1.x throws a BARE STRING (not an Error subclass) on corrupt
      // ZLIB input — the buffer below has the zlib default-level header
      // 78 9c but a body of 0xff bytes that pako rejects with the literal
      // string "invalid block type". Without the wire-format wrapping this
      // string would propagate out as-is; the assertion here proves the
      // wrap converts every such throw into GsrIncompatibleDataError so
      // the wire-format error taxonomy stays uniform.
      const CORRUPT_ZLIB_BODY = Buffer.from([
        0x78, 0x9c, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      ]);

      it("wraps pako's bare-string throw on corrupt input as GsrIncompatibleDataError", () => {
        expect(() => decompressZlib(CORRUPT_ZLIB_BODY)).toThrow(
          GsrIncompatibleDataError,
        );
      });

      it("preserves the underlying pako throw on the wrapped error's cause", () => {
        try {
          decompressZlib(CORRUPT_ZLIB_BODY);
          expect.unreachable("decompressZlib should have thrown");
        } catch (err) {
          expect(err).toBeInstanceOf(GsrIncompatibleDataError);
          // pako 1.x throws a bare string — this is the whole reason the
          // wrap exists. Assert the original throw is preserved on cause
          // so diagnostics can still see the raw pako reason.
          expect(typeof (err as { cause?: unknown }).cause).toBe("string");
          expect((err as Error).message).toContain(
            (err as { cause?: string }).cause ?? "",
          );
        }
      });

      it("wraps a garbage (non-ZLIB) buffer as GsrIncompatibleDataError", () => {
        // No zlib header magic — pako rejects with "unknown compression method".
        const garbage = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05]);
        expect(() => decompressZlib(garbage)).toThrow(GsrIncompatibleDataError);
      });

      it("wraps an empty buffer as GsrIncompatibleDataError", () => {
        // pako rejects an empty buffer with "buffer error".
        expect(() => decompressZlib(Buffer.alloc(0))).toThrow(
          GsrIncompatibleDataError,
        );
      });
    });
  });

  describe("compressionByteForType", () => {
    it("maps NONE to 0x00", () => {
      expect(compressionByteForType("NONE")).toBe(0x00);
    });

    it("maps ZLIB to 0x05", () => {
      expect(compressionByteForType("ZLIB")).toBe(0x05);
    });
  });

  describe("compressionTypeForByte", () => {
    it("maps 0x00 back to NONE", () => {
      expect(compressionTypeForByte(0x00)).toBe("NONE");
    });

    it("maps 0x05 back to ZLIB", () => {
      expect(compressionTypeForByte(0x05)).toBe("ZLIB");
    });

    it("throws GsrIncompatibleDataError on an unknown byte", () => {
      expect(() => compressionTypeForByte(0x01)).toThrow(GsrIncompatibleDataError);
      expect(() => compressionTypeForByte(0xff)).toThrow(GsrIncompatibleDataError);
    });

    it("names the offending byte in the thrown error message", () => {
      try {
        compressionTypeForByte(0x01);
        expect.unreachable("compressionTypeForByte should have thrown");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as Error).message).toContain("0x01");
      }
    });
  });

  describe("byte↔type round-trip", () => {
    it("round-trips both supported modes through both maps", () => {
      for (const type of ["NONE", "ZLIB"] as const) {
        expect(compressionTypeForByte(compressionByteForType(type))).toBe(type);
      }
      for (const byte of [0x00, 0x05]) {
        expect(compressionByteForType(compressionTypeForByte(byte))).toBe(byte);
      }
    });
  });
});
