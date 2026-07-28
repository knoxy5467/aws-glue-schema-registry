/**
 * Unit tests for the Avro reader-schema projection module.
 *
 * The projection function is the production surface for reader/writer
 * schema evolution: it consumes a writer-encoded Avro payload (post-
 * header, post-decompress) and returns a datum in the reader schema's
 * shape. The three resolution-rule classes exercised below — field
 * drop, default fill, and type promotion — mirror the resolution
 * behavior the Java and Go reference clients implement.
 *
 * These tests deliberately encode with `avsc.Type.toBuffer` on the
 * writer schema and feed those bytes into the projection function
 * under test. That matches how the projection module is composed at
 * runtime: the wire codec in `@gsr/core` recovers the payload and
 * hands it (already decompressed) to this module. The encode path is
 * never touched from here.
 */

import avsc from "avsc";
import { describe, expect, it } from "vitest";

import { GsrIncompatibleDataError } from "@gsr/core";

import { projectAvroPayload } from "./avro-projection.js";

/**
 * Encode a datum with a writer schema, using the same
 * `omitRecordMethods: true` flag the shipped encode path uses so the
 * bytes handed to the projection function match a real payload
 * produced upstream.
 */
function encodeWithWriter(
  writerSchema: avsc.schema.AvroSchema,
  datum: unknown,
): Buffer {
  const writerType = avsc.Type.forSchema(writerSchema, {
    omitRecordMethods: true,
  });
  return writerType.toBuffer(datum);
}

describe("serde/evolution/avro-projection", () => {
  describe("field drop", () => {
    it("drops a scalar field the reader schema omits", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
        ],
      };
      const payload = encodeWithWriter(writerSchema, {
        id: "u-1",
        name: "Ada",
        legacy: "please-drop",
      });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(projected).toEqual({ id: "u-1", name: "Ada" });
      expect(projected as Record<string, unknown>).not.toHaveProperty(
        "legacy",
      );
    });

    it("drops multiple writer fields the reader omits", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "OrderRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "internalRoute", type: "string" },
          { name: "internalHash", type: "int" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "OrderRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, {
        id: "ord-9",
        internalRoute: "warehouse-a",
        internalHash: 42,
      });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(projected).toEqual({ id: "ord-9" });
    });
  });

  describe("default fill", () => {
    it("fills a string default the writer never wrote", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "status", type: "string", default: "ACTIVE" },
        ],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-42" });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(projected).toEqual({ id: "u-42", status: "ACTIVE" });
    });

    it("fills a nullable-union default for a writer-absent field", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "note", type: ["null", "string"], default: null },
        ],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-42" });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(projected).toEqual({ id: "u-42", note: null });
    });
  });

  describe("type promotion", () => {
    it("promotes writer int to reader long", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "int" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "long" }],
      };
      const payload = encodeWithWriter(writerSchema, { value: 123 });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      // avsc represents both int and long as plain JavaScript numbers when
      // the value fits in a 32-bit range, so the promoted long is still a
      // number here — the parity claim is that the decode succeeds and
      // preserves the value.
      expect(projected).toEqual({ value: 123 });
    });

    it("promotes writer float to reader double", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "float" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "NumericRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "double" }],
      };
      const payload = encodeWithWriter(writerSchema, { value: 1.5 });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      // 1.5 is exactly representable as both float32 and float64, so the
      // promoted double is bit-exact — no epsilon comparison is needed.
      expect(projected).toEqual({ value: 1.5 });
    });
  });

  describe("combined resolution", () => {
    it("drops, fills a default, and promotes a numeric in one projection", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CombinedRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "count", type: "int" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "CombinedRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "count", type: "long" },
          { name: "status", type: "string", default: "OK" },
        ],
      };
      const payload = encodeWithWriter(writerSchema, {
        id: "row-1",
        count: 99,
        legacy: "gone",
      });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(projected).toEqual({ id: "row-1", count: 99, status: "OK" });
    });
  });

  describe("schema input variants", () => {
    it("accepts JSON-string forms for both writer and reader schemas", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, {
        id: "u-77",
        legacy: "x",
      });

      const projected = projectAvroPayload(
        payload,
        JSON.stringify(writerSchema),
        JSON.stringify(readerSchema),
      );

      expect(projected).toEqual({ id: "u-77" });
    });
  });

  describe("record-type selector", () => {
    it("returns a plain datum without avsc helper methods on GENERIC", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-1" });

      const projected = projectAvroPayload(
        payload,
        writerSchema,
        readerSchema,
        { recordType: "GENERIC" },
      );

      expect(typeof projected).toBe("object");
      expect(
        (projected as Record<string, unknown>)["toBuffer"],
      ).toBeUndefined();
      expect(
        (projected as Record<string, unknown>)["clone"],
      ).toBeUndefined();
      expect(
        (projected as Record<string, unknown>)["isValid"],
      ).toBeUndefined();
    });

    it("returns a typed-record instance with avsc helper methods on SPECIFIC", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, {
        id: "u-1",
        legacy: "drop-me",
      });

      const projected = projectAvroPayload(
        payload,
        writerSchema,
        readerSchema,
        { recordType: "SPECIFIC" },
      );

      // The typed record carries an avsc-generated prototype exposing
      // `.toBuffer` — presence is a stable signal we are on SPECIFIC.
      expect(
        typeof (projected as { toBuffer?: unknown }).toBuffer,
      ).toBe("function");
      expect(projected).toMatchObject({ id: "u-1" });
      expect(projected as Record<string, unknown>).not.toHaveProperty(
        "legacy",
      );
    });

    it("defaults to GENERIC when opts.recordType is omitted", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-1" });

      const projected = projectAvroPayload(payload, writerSchema, readerSchema);

      expect(
        (projected as Record<string, unknown>)["toBuffer"],
      ).toBeUndefined();
    });
  });

  describe("error handling", () => {
    it("throws GsrIncompatibleDataError when the reader schema is unparseable JSON", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-1" });

      try {
        projectAvroPayload(payload, writerSchema, "{ this is not JSON");
        expect.fail("expected projectAvroPayload to throw for unparseable reader schema");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as Error).message).toMatch(/reader schema/);
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("throws GsrIncompatibleDataError when the reader schema is not a valid Avro schema", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const payload = encodeWithWriter(writerSchema, { id: "u-1" });

      // Valid JSON, but not a valid Avro schema — avsc.Type.forSchema throws.
      const brokenReader = { type: "not-a-real-avro-type" };

      try {
        projectAvroPayload(payload, writerSchema, brokenReader);
        expect.fail("expected projectAvroPayload to throw for invalid reader schema");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as Error).message).toMatch(/reader schema/);
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("throws GsrIncompatibleDataError when the writer schema is unparseable JSON", () => {
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };

      try {
        projectAvroPayload(
          Buffer.from([0x00]),
          "{ this is not JSON",
          readerSchema,
        );
        expect.fail("expected projectAvroPayload to throw for unparseable writer schema");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as Error).message).toMatch(/writer schema/);
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("throws GsrIncompatibleDataError when the reader/writer pair is unresolvable", () => {
      // A string writer paired with an int reader has no avsc-supported
      // resolution — createResolver will throw.
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UnresolvableRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UnresolvableRecord",
        namespace: "example.evolution",
        fields: [{ name: "value", type: "int" }],
      };
      const payload = encodeWithWriter(writerSchema, { value: "not a number" });

      try {
        projectAvroPayload(payload, writerSchema, readerSchema);
        expect.fail("expected projectAvroPayload to throw for unresolvable pair");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as Error).message).toMatch(/incompatible/);
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("wraps a truncated projected payload as GsrIncompatibleDataError", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const full = encodeWithWriter(writerSchema, {
        id: "u-1",
        legacy: "abcdefghij",
      });
      const truncated = full.subarray(0, Math.max(1, full.length - 5));

      try {
        projectAvroPayload(truncated, writerSchema, readerSchema);
        expect.fail("expected projectAvroPayload to throw for truncated payload");
      } catch (err) {
        expect(err).toBeInstanceOf(GsrIncompatibleDataError);
        expect((err as { cause?: unknown }).cause).toBeDefined();
      }
    });

    it("never lets a raw avsc error escape the public surface", () => {
      const writerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema: avsc.schema.AvroSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };

      const corruptInputs: readonly Buffer[] = [
        Buffer.alloc(0), // empty
        Buffer.alloc(4, 0xff), // short garbage
        Buffer.from([0xff, 0xff, 0xff, 0xff, 0x7f]), // varint at boundary
      ];

      for (const buf of corruptInputs) {
        let caught: unknown;
        try {
          projectAvroPayload(buf, writerSchema, readerSchema);
        } catch (e) {
          caught = e;
        }
        // If avsc happens to accept a given corrupt input (returning a
        // garbage record), the assertion is inapplicable — but any throw
        // must be the wrapped error type, never a bare avsc error.
        if (caught !== undefined) {
          expect(caught).toBeInstanceOf(Error);
          expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
        }
      }
    });
  });
});
