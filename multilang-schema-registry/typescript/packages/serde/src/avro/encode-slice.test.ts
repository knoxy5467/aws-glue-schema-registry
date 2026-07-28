import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  avroHazardRecords,
  decodeAvro,
  encodeAvro,
} from "./encode-slice.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
  SHARED_TEST_DIR,
} from "../test-support/reference-root.js";

/**
 * The 18-byte wire header prefix in each golden `.bin`. Kept as a local
 * constant so this test file has no cross-package import — the header codec
 * lives in `@gsr/core` and is exercised by the byte-identity gate suite; the
 * slice only needs to skip the fixed-size prefix to reach the payload region.
 */
const WIRE_FORMAT_HEADER_SIZE = 18;

const TEST_V1_RECORD = { data: "hello", count: 7 };
const TEST_V1_SCHEMA_PATH = resolve(
  SHARED_TEST_DIR,
  "avro/none/test_v1.avsc",
);

/**
 * The uncompressed avsc-encoded body of the `test_v1.avsc` record
 * `{data:"hello", count:7}`, taken from the payload region (bytes 18..) of the
 * NONE-compressed avro generic golden vector. Kept as a literal alongside the
 * golden-file check so the assertion is legible on failure.
 */
const AVRO_TEST_V1_PAYLOAD = Buffer.from([
  0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0e,
]);

describe("serde/avro/encode-slice", () => {
  describe("avroHazardRecords", () => {
    it("exposes the test-v1 fixture record with the {data,count} shape", () => {
      expect(avroHazardRecords).toHaveLength(1);
      const entry = avroHazardRecords[0];
      expect(entry).toBeDefined();
      expect(entry?.tag).toBe("test-v1");
      expect(entry?.schemaPath).toBe("avro/none/test_v1.avsc");
      expect(entry?.record).toEqual(TEST_V1_RECORD);
    });
  });

  describe.skipIf(!referenceRootExists)("encodeAvro", () => {
    it("produces the payload region of the NONE-compressed Java golden vector", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "avro__generic__comp-NONE__test-v1.bin",
      );
      const golden = readFileSync(goldenFile);
      const goldenPayload = golden.subarray(WIRE_FORMAT_HEADER_SIZE);

      const encoded = encodeAvro(TEST_V1_SCHEMA_PATH, TEST_V1_RECORD);

      expect(encoded.equals(goldenPayload)).toBe(true);
    });

    it("matches the recorded avsc-encoded byte literal for {data:'hello', count:7}", () => {
      const encoded = encodeAvro(TEST_V1_SCHEMA_PATH, TEST_V1_RECORD);
      expect(encoded.equals(AVRO_TEST_V1_PAYLOAD)).toBe(true);
    });

    it("also matches the SPECIFIC record-type golden payload (equal SHA-256)", () => {
      // PROVENANCE.md records generic and specific test-v1 cells with identical
      // SHA-256s, so one avsc encode covers both golden record-type cells.
      const specificGolden = readFileSync(
        resolve(GOLDEN_JAVA_DIR, "avro__specific__comp-NONE__test-v1.bin"),
      ).subarray(WIRE_FORMAT_HEADER_SIZE);

      const encoded = encodeAvro(TEST_V1_SCHEMA_PATH, TEST_V1_RECORD);

      expect(encoded.equals(specificGolden)).toBe(true);
    });
  });

  describe.skipIf(!referenceRootExists)("decodeAvro", () => {
    it("reconstructs {data:'hello', count:7} from the golden payload region", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "avro__generic__comp-NONE__test-v1.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );

      const decoded = decodeAvro(goldenPayload, TEST_V1_SCHEMA_PATH);

      expect(decoded).toEqual(TEST_V1_RECORD);
    });

    it("round-trips encodeAvro output back to the original record", () => {
      const encoded = encodeAvro(TEST_V1_SCHEMA_PATH, TEST_V1_RECORD);
      const decoded = decodeAvro(encoded, TEST_V1_SCHEMA_PATH);

      expect(decoded).toEqual(TEST_V1_RECORD);
    });
  });
});
