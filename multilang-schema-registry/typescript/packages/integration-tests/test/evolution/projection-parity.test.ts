/**
 * Cross-format evolution parity suite.
 *
 * Exercises the full public decode surface (`GsrSerializer` +
 * `GsrDeserializer`) with a reader-schema attached to the deserialize
 * request. The scenarios below assert the three Avro resolution rule
 * classes the Java and Go reference clients implement — field drop,
 * default fill, and numeric type promotion — plus the negative-path
 * parity contract: an incompatible reader/writer pair, an invalid
 * compatibility-mode string, and the writer-only regression when no
 * reader schema is supplied.
 *
 * The parity suite is a facade-level test: it composes the full wire
 * codec (`@gsr/core`) with the per-format Avro serde on the encode
 * side and with the reader-projection module on the decode side. It
 * never touches encode-side bytes and never talks to Glue — every
 * assertion is offline.
 */

import { describe, expect, it } from "vitest";

import { GsrIncompatibleDataError } from "@gsr/core";
import {
  CompatibilityMode,
  DataFormat,
  DEFAULT_COMPATIBILITY_MODE,
  GsrDeserializer,
  GsrInvalidCompatibilityModeError,
  GsrSerializer,
  isCompatibilityMode,
  parseCompatibilityMode,
} from "@gsr/serde";

/**
 * A fixed schema-version UUID for every wire message the suite
 * assembles. The header UUID is opaque to the projection path
 * (projection runs on the payload after `@gsr/core.decodeMessage`
 * strips the header), so its value is inconsequential — a constant
 * keeps the tests deterministic and reads cleanly.
 */
const FIXED_SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

/**
 * Wrap the writer-side setup — instantiating a serializer, encoding a
 * record with the writer schema, and returning the full wire bytes —
 * so every parity case can name the reader/writer pair inline without
 * repeating boilerplate.
 */
function encodeWireWithWriter(
  writerSchema: unknown,
  record: unknown,
): Buffer {
  const serializer = new GsrSerializer();
  return serializer.serialize({
    format: DataFormat.AVRO,
    schemaVersionId: FIXED_SCHEMA_VERSION_ID,
    topic: "orders",
    schema: writerSchema,
    data: record,
  });
}

describe("integration-tests/evolution/projection-parity", () => {
  describe("Avro reader-schema projection through the facade", () => {
    describe("field drop", () => {
      const writerSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
          { name: "legacy", type: "string" },
        ],
      };
      const readerSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "name", type: "string" },
        ],
      };
      const writerRecord = {
        id: "u-1",
        name: "Ada",
        legacy: "please-drop",
      };

      it("drops the writer-only field on GENERIC decode", () => {
        const wire = encodeWireWithWriter(writerSchema, writerRecord);
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
        });
        expect(decoded).toEqual({ id: "u-1", name: "Ada" });
        expect(decoded as Record<string, unknown>).not.toHaveProperty(
          "legacy",
        );
      });

      it("drops the writer-only field on SPECIFIC decode", () => {
        const wire = encodeWireWithWriter(writerSchema, writerRecord);
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
          avro: { recordType: "SPECIFIC" },
        });
        // SPECIFIC returns an avsc typed-record instance whose prototype
        // exposes `.toBuffer`; field values project identically.
        expect(
          typeof (decoded as { toBuffer?: unknown }).toBuffer,
        ).toBe("function");
        expect(decoded).toMatchObject({ id: "u-1", name: "Ada" });
        expect(decoded as Record<string, unknown>).not.toHaveProperty(
          "legacy",
        );
      });
    });

    describe("default fill", () => {
      const writerSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [{ name: "id", type: "string" }],
      };
      const readerSchema = {
        type: "record",
        name: "UserRecord",
        namespace: "example.evolution",
        fields: [
          { name: "id", type: "string" },
          { name: "status", type: "string", default: "ACTIVE" },
        ],
      };

      it("fills the reader default on GENERIC decode when the writer omits the field", () => {
        const wire = encodeWireWithWriter(writerSchema, { id: "u-42" });
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
        });
        expect(decoded).toEqual({ id: "u-42", status: "ACTIVE" });
      });

      it("fills the reader default on SPECIFIC decode when the writer omits the field", () => {
        const wire = encodeWireWithWriter(writerSchema, { id: "u-42" });
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
          avro: { recordType: "SPECIFIC" },
        });
        expect(
          typeof (decoded as { toBuffer?: unknown }).toBuffer,
        ).toBe("function");
        expect(decoded).toMatchObject({ id: "u-42", status: "ACTIVE" });
      });
    });

    describe("type promotion", () => {
      it("promotes writer int to reader long on GENERIC decode", () => {
        const writerSchema = {
          type: "record",
          name: "NumericRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "int" }],
        };
        const readerSchema = {
          type: "record",
          name: "NumericRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "long" }],
        };
        const wire = encodeWireWithWriter(writerSchema, { value: 123 });
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
        });
        // avsc represents both int and long as JavaScript numbers when
        // the value fits into a 32-bit range — parity claim is that
        // decode succeeds and the value is preserved.
        expect(decoded).toEqual({ value: 123 });
      });

      it("promotes writer float to reader double on GENERIC decode", () => {
        const writerSchema = {
          type: "record",
          name: "NumericRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "float" }],
        };
        const readerSchema = {
          type: "record",
          name: "NumericRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "double" }],
        };
        // 1.5 is exactly representable in both float32 and float64 so
        // no epsilon is needed to assert equality on the promotion.
        const wire = encodeWireWithWriter(writerSchema, { value: 1.5 });
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
        });
        expect(decoded).toEqual({ value: 1.5 });
      });

      it("combines drop + default fill + promotion in one projection", () => {
        const writerSchema = {
          type: "record",
          name: "CombinedRecord",
          namespace: "example.evolution",
          fields: [
            { name: "id", type: "string" },
            { name: "count", type: "int" },
            { name: "legacy", type: "string" },
          ],
        };
        const readerSchema = {
          type: "record",
          name: "CombinedRecord",
          namespace: "example.evolution",
          fields: [
            { name: "id", type: "string" },
            { name: "count", type: "long" },
            { name: "status", type: "string", default: "OK" },
          ],
        };
        const wire = encodeWireWithWriter(writerSchema, {
          id: "row-1",
          count: 99,
          legacy: "gone",
        });
        const decoded = new GsrDeserializer().deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: writerSchema,
          readerSchema,
        });
        expect(decoded).toEqual({
          id: "row-1",
          count: 99,
          status: "OK",
        });
      });
    });

    describe("negative path — projection surfaces the wire-format error", () => {
      // NB: `@gsr/serde`'s tsup bundle inlines a private copy of
      // `GsrIncompatibleDataError`, so an error thrown from the built
      // serde is not `instanceof` the class imported from `@gsr/core`
      // from another package. The facade contract is that the thrown
      // error's public name is `"GsrIncompatibleDataError"`; we assert
      // that name plus the message-parse-failure marker rather than
      // rely on constructor identity across the package boundary.
      it("throws GsrIncompatibleDataError for an unresolvable reader/writer pair", () => {
        // string writer paired with int reader has no avsc-supported
        // resolution — the pair MUST surface as the wire-format decode
        // error through the facade, never as a raw avsc throw.
        const writerSchema = {
          type: "record",
          name: "UnresolvableRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "string" }],
        };
        const readerSchema = {
          type: "record",
          name: "UnresolvableRecord",
          namespace: "example.evolution",
          fields: [{ name: "value", type: "int" }],
        };
        const wire = encodeWireWithWriter(writerSchema, {
          value: "not a number",
        });
        const deserializer = new GsrDeserializer();
        let captured: unknown;
        try {
          deserializer.deserialize({
            format: DataFormat.AVRO,
            data: wire,
            schema: writerSchema,
            readerSchema,
          });
          expect.fail(
            "expected deserialize to throw for an unresolvable reader/writer pair",
          );
        } catch (err) {
          captured = err;
        }
        expect(captured).toBeInstanceOf(Error);
        expect((captured as Error).name).toBe("GsrIncompatibleDataError");
        expect((captured as Error).message).toMatch(/incompatible/i);
      });

      it("throws GsrIncompatibleDataError for an unparseable reader schema", () => {
        const writerSchema = {
          type: "record",
          name: "UserRecord",
          namespace: "example.evolution",
          fields: [{ name: "id", type: "string" }],
        };
        const wire = encodeWireWithWriter(writerSchema, { id: "u-1" });
        const deserializer = new GsrDeserializer();
        let captured: unknown;
        try {
          deserializer.deserialize({
            format: DataFormat.AVRO,
            data: wire,
            schema: writerSchema,
            readerSchema: "{ this is not JSON",
          });
          expect.fail(
            "expected deserialize to throw for unparseable reader schema",
          );
        } catch (err) {
          captured = err;
        }
        expect(captured).toBeInstanceOf(Error);
        expect((captured as Error).name).toBe("GsrIncompatibleDataError");
        expect((captured as Error).message).toMatch(/reader schema/);
      });
    });
  });

  describe("writer-only regression (no readerSchema)", () => {
    // The projection path is opt-in. When the caller supplies no
    // reader schema, the AVRO decode path must preserve the prior
    // behavior — the returned record is the writer shape.
    const writerSchema = {
      type: "record",
      name: "UserRecord",
      namespace: "example.evolution",
      fields: [
        { name: "id", type: "string" },
        { name: "name", type: "string" },
        { name: "legacy", type: "string" },
      ],
    };
    const writerRecord = {
      id: "u-1",
      name: "Ada",
      legacy: "still-here",
    };

    it("returns the writer shape when readerSchema is absent", () => {
      const wire = encodeWireWithWriter(writerSchema, writerRecord);
      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.AVRO,
        data: wire,
        schema: writerSchema,
      });
      expect(decoded).toEqual(writerRecord);
    });

    it("returns the writer shape when readerSchema is explicitly undefined", () => {
      const wire = encodeWireWithWriter(writerSchema, writerRecord);
      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.AVRO,
        data: wire,
        schema: writerSchema,
        readerSchema: undefined,
      });
      expect(decoded).toEqual(writerRecord);
    });
  });

  describe("compatibility-mode validation", () => {
    // The compatibility-mode API is a config-validation seam that
    // never touches a decode path. These cases verify the invalid-mode
    // error surface and confirm the exported default and predicate
    // are wired through the barrel — the parity contract for the
    // config half of the evolution surface.
    it("parseCompatibilityMode accepts all 8 modes case-exactly", () => {
      const modes: readonly CompatibilityMode[] = [
        CompatibilityMode.NONE,
        CompatibilityMode.DISABLED,
        CompatibilityMode.BACKWARD,
        CompatibilityMode.BACKWARD_ALL,
        CompatibilityMode.FORWARD,
        CompatibilityMode.FORWARD_ALL,
        CompatibilityMode.FULL,
        CompatibilityMode.FULL_ALL,
      ];
      for (const mode of modes) {
        expect(parseCompatibilityMode(mode)).toBe(mode);
        expect(isCompatibilityMode(mode)).toBe(true);
      }
    });

    it("parseCompatibilityMode rejects a lowercase variant with GsrInvalidCompatibilityModeError", () => {
      expect(() => parseCompatibilityMode("backward")).toThrow(
        GsrInvalidCompatibilityModeError,
      );
    });

    it("parseCompatibilityMode rejects a typo with GsrInvalidCompatibilityModeError", () => {
      expect(() => parseCompatibilityMode("FORWARDS")).toThrow(
        GsrInvalidCompatibilityModeError,
      );
    });

    it("parseCompatibilityMode rejects the empty string with GsrInvalidCompatibilityModeError", () => {
      expect(() => parseCompatibilityMode("")).toThrow(
        GsrInvalidCompatibilityModeError,
      );
    });

    it("GsrInvalidCompatibilityModeError is distinct from the wire-decode taxonomy", () => {
      // The config-validation error must NOT be an instance of the
      // decode error type — the taxonomies are deliberately split.
      let captured: unknown;
      try {
        parseCompatibilityMode("bogus");
      } catch (err) {
        captured = err;
      }
      expect(captured).toBeInstanceOf(GsrInvalidCompatibilityModeError);
      expect(captured).not.toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("DEFAULT_COMPATIBILITY_MODE is BACKWARD (Java-parity default)", () => {
      expect(DEFAULT_COMPATIBILITY_MODE).toBe(CompatibilityMode.BACKWARD);
    });
  });

  describe("readerSchema ignored for non-Avro formats", () => {
    // The wire-in contract is Avro-only: a PROTOBUF or JSON
    // deserialize request with a `readerSchema` present must still
    // succeed under the writer-only path and return the same
    // record it would have without the field.
    it("PROTOBUF request round-trips unchanged when readerSchema is supplied", () => {
      const PROTO_TEXT = `
syntax = "proto3";
package example;

message Ping {
  string message = 1;
  int32  seq     = 2;
}
`;
      const record = { message: "pong", seq: 7 };
      const wire = new GsrSerializer().serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: FIXED_SCHEMA_VERSION_ID,
        topic: "pings",
        schema: PROTO_TEXT,
        messageFullName: "example.Ping",
        data: record,
      });

      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.PROTOBUF,
        data: wire,
        schema: PROTO_TEXT,
        messageFullName: "example.Ping",
        // Sentinel: readerSchema is set to a completely unrelated
        // object shape — a PROTOBUF request must ignore it.
        readerSchema: { irrelevant: true },
      });
      expect(decoded).toMatchObject(record);
    });

    it("JSON request round-trips unchanged when readerSchema is supplied", () => {
      const jsonSchema = {
        $schema: "http://json-schema.org/draft-07/schema#",
        type: "object",
        required: ["id", "count"],
        properties: {
          id: { type: "string", minLength: 1 },
          count: { type: "integer", minimum: 0 },
        },
        additionalProperties: false,
      };
      const record = { id: "widget-1", count: 3 };
      const wire = new GsrSerializer().serialize({
        format: DataFormat.JSON,
        schemaVersionId: FIXED_SCHEMA_VERSION_ID,
        topic: "widgets",
        schema: jsonSchema,
        data: record,
      });

      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.JSON,
        data: wire,
        schema: jsonSchema,
        // Sentinel: JSON documents are self-describing and readerSchema
        // must be ignored on this path.
        readerSchema: { unrelated: "shape" },
      });
      expect(decoded).toEqual(record);
    });
  });
});
