import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
  SHARED_TEST_DIR,
} from "../test-support/reference-root.js";

import avsc from "avsc";
import { describe, expect, it } from "vitest";

import { GsrIncompatibleDataError } from "@gsr/core";

import { deserializeAvro, serializeAvro } from "./serde.js";

/**
 * The 18-byte wire header prefix in each golden `.bin`. Kept as a local
 * constant so this test file has no cross-package import beyond the error
 * type — the header codec lives in `@gsr/core` and is exercised by the
 * wire-core gate suite; the serde only needs to skip the fixed-size prefix
 * to reach the payload region.
 */
const WIRE_FORMAT_HEADER_SIZE = 18;

/**
 * An arbitrary, schema-general record that is deliberately NOT the hazard
 * record used by the wire-core gate — exercising nested records, arrays,
 * unions with null, maps, and enums proves the serde accepts arbitrary
 * avsc-parseable schemas rather than the hardcoded golden fixture shape.
 */
const ARBITRARY_SCHEMA: avsc.schema.AvroSchema = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    { name: "active", type: "boolean" },
    { name: "note", type: ["null", "string"], default: null },
    {
      name: "tags",
      type: { type: "array", items: "string" },
    },
    {
      name: "metadata",
      type: { type: "map", values: "string" },
    },
    {
      name: "status",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["OPEN", "PAID", "CANCELLED"],
      },
    },
    {
      name: "shipping",
      type: {
        type: "record",
        name: "Address",
        fields: [
          { name: "city", type: "string" },
          { name: "postal", type: "string" },
        ],
      },
    },
  ],
};

const ARBITRARY_RECORD = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  active: true,
  note: "gift wrapped",
  tags: ["urgent", "prime"],
  metadata: { channel: "web", region: "us-west-2" },
  status: "PAID",
  shipping: { city: "Seattle", postal: "98109" },
};

const TEST_V1_SCHEMA_PATH = resolve(
  SHARED_TEST_DIR,
  "avro/none/test_v1.avsc",
);
const TEST_V1_RECORD = { data: "hello", count: 7 };

describe("serde/avro/serde", () => {
  describe("serializeAvro + deserializeAvro (GENERIC)", () => {
    it("round-trips an arbitrary nested record via a parsed schema object", () => {
      const payload = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD);
      const decoded = deserializeAvro(payload, ARBITRARY_SCHEMA);

      expect(decoded).toEqual(ARBITRARY_RECORD);
    });

    it("accepts a JSON-string schema equivalent to the parsed object", () => {
      const schemaJson = JSON.stringify(ARBITRARY_SCHEMA);
      const payload = serializeAvro(schemaJson, ARBITRARY_RECORD);
      const decoded = deserializeAvro(payload, schemaJson);

      expect(decoded).toEqual(ARBITRARY_RECORD);
    });

    it("accepts a pre-compiled avsc.Type as the schema input", () => {
      const type = avsc.Type.forSchema(ARBITRARY_SCHEMA);
      const payload = serializeAvro(type, ARBITRARY_RECORD);
      const decoded = deserializeAvro(payload, type);

      expect(decoded).toEqual(ARBITRARY_RECORD);
    });

    it("returns a datum without avsc helper methods on GENERIC decode", () => {
      const payload = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD);
      const decoded = deserializeAvro(payload, ARBITRARY_SCHEMA, {
        recordType: "GENERIC",
      });

      expect(typeof decoded).toBe("object");
      // A GENERIC datum has no avsc typed-record helpers — avsc's typed
      // record prototype adds .toBuffer / .clone / .compare / .isValid /
      // .wrap; a plain datum omits them entirely.
      expect(
        (decoded as Record<string, unknown>)["toBuffer"],
      ).toBeUndefined();
      expect(
        (decoded as Record<string, unknown>)["clone"],
      ).toBeUndefined();
      expect(
        (decoded as Record<string, unknown>)["isValid"],
      ).toBeUndefined();
    });
  });

  describe("deserializeAvro (SPECIFIC)", () => {
    it("returns a typed-record instance with avsc helper methods", () => {
      const payload = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD);
      const decoded = deserializeAvro(payload, ARBITRARY_SCHEMA, {
        recordType: "SPECIFIC",
      });

      // The typed record is a plain data holder plus a prototype exposing
      // avsc helper methods — the presence of `.toBuffer` is a stable signal
      // that we are on the SPECIFIC path.
      expect(
        typeof (decoded as { toBuffer?: unknown }).toBuffer,
      ).toBe("function");
      expect(decoded).toMatchObject(ARBITRARY_RECORD);
    });

    it("decodes the same wire bytes as the GENERIC path (record-type is decode-shape only)", () => {
      const payload = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD);

      const genericDecoded = deserializeAvro(payload, ARBITRARY_SCHEMA, {
        recordType: "GENERIC",
      }) as Record<string, unknown>;
      const specificDecoded = deserializeAvro(payload, ARBITRARY_SCHEMA, {
        recordType: "SPECIFIC",
      }) as Record<string, unknown>;

      // Both paths reconstruct the same field values from the same bytes;
      // only the object prototype differs.
      expect(specificDecoded).toMatchObject(genericDecoded);
    });

    it("produces identical wire bytes regardless of the encode-time recordType", () => {
      const generic = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD, {
        recordType: "GENERIC",
      });
      const specific = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD, {
        recordType: "SPECIFIC",
      });

      expect(specific.equals(generic)).toBe(true);
    });
  });

  describe.skipIf(!referenceRootExists)("golden-payload regression", () => {
    /**
     * Reproduces the payload region (bytes 18..) of the Java-canonical
     * NONE-compressed Avro generic golden vector for `test_v1`. This is the
     * byte-regression assertion — the full-breadth serde MUST reproduce
     * the same bytes the minimum-viable slice already produced.
     */
    it("reproduces the golden test-v1 generic payload byte-for-byte", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "avro__generic__comp-NONE__test-v1.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );
      const schemaJson = readFileSync(TEST_V1_SCHEMA_PATH, "utf8");

      const encoded = serializeAvro(schemaJson, TEST_V1_RECORD);

      expect(encoded.equals(goldenPayload)).toBe(true);
    });

    it("also matches the SPECIFIC test-v1 golden payload (equal SHA-256)", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "avro__specific__comp-NONE__test-v1.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );
      const schemaJson = readFileSync(TEST_V1_SCHEMA_PATH, "utf8");

      const encoded = serializeAvro(schemaJson, TEST_V1_RECORD, {
        recordType: "SPECIFIC",
      });

      expect(encoded.equals(goldenPayload)).toBe(true);
    });

    it("round-trips the golden test-v1 payload back to the canonical record", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "avro__generic__comp-NONE__test-v1.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );
      const schemaJson = readFileSync(TEST_V1_SCHEMA_PATH, "utf8");

      const decoded = deserializeAvro(goldenPayload, schemaJson);

      expect(decoded).toEqual(TEST_V1_RECORD);
    });
  });

  describe("decode negative paths", () => {
    it("wraps a truncated Avro payload as GsrIncompatibleDataError", () => {
      const payload = serializeAvro(ARBITRARY_SCHEMA, ARBITRARY_RECORD);
      const truncated = payload.subarray(0, Math.max(1, payload.length - 5));

      expect(() =>
        deserializeAvro(truncated, ARBITRARY_SCHEMA),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("wraps a garbage buffer decoded against a real schema as GsrIncompatibleDataError", () => {
      // 32 bytes of high-bit noise decodes as a series of malformed varints
      // and out-of-range field selectors against the arbitrary schema.
      const garbage = Buffer.alloc(32, 0xff);

      expect(() =>
        deserializeAvro(garbage, ARBITRARY_SCHEMA),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("preserves the original throw as the wrapped error's cause", () => {
      const truncated = Buffer.from([0x02]); // one-byte varint header, no body
      try {
        deserializeAvro(truncated, ARBITRARY_SCHEMA);
        // If this returns instead of throwing, avsc silently accepted the
        // corrupt input — the assertion below flags the regression.
        expect.fail("expected deserializeAvro to throw on truncated input");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        // The wrapper preserves the underlying avsc throw for diagnostics.
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("throws Error subclasses only — no bare-string or raw library throws escape", () => {
      const corruptInputs: readonly Buffer[] = [
        Buffer.alloc(0), // empty
        Buffer.alloc(4, 0xff), // short garbage
        Buffer.from([0xff, 0xff, 0xff, 0xff, 0x7f]), // varint at boundary
      ];

      for (const buf of corruptInputs) {
        let caught: unknown;
        try {
          deserializeAvro(buf, ARBITRARY_SCHEMA);
        } catch (e) {
          caught = e;
        }
        // If avsc happened to accept a given corrupt input (returning a
        // garbage record), the assertion is inapplicable — but any throw
        // MUST be a wrapped Error, never a bare value.
        if (caught !== undefined) {
          expect(caught).toBeInstanceOf(Error);
          expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
        }
      }
    });
  });
});
