import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { describe, expect, it } from "vitest";

import {
  decodeMessage,
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
  WIRE_FORMAT_HEADER_SIZE,
} from "@gsr/core";

import { GsrDeserializer } from "./deserializer.js";
import {
  DefaultSchemaNameStrategy,
  RecordNameStrategy,
} from "./schema-name-strategy.js";
import { GsrSerializer } from "./serializer.js";
import { DataFormat } from "./types.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
} from "../test-support/reference-root.js";

const GOLDEN_SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

/**
 * The nine wrapper messages from `google/protobuf/wrappers.proto`, inlined
 * so this test has exactly one library-import dependency (protobufjs at
 * runtime, transitively via the serde). Lexicographic order places
 * `google.protobuf.StringValue` at message-index 6 in the sorted BFS
 * output — the canonical `index > 0` example the Java reference uses.
 */
const WRAPPERS_PROTO_TEXT = `
syntax = "proto3";
package google.protobuf;

message DoubleValue { double value = 1; }
message FloatValue  { float  value = 1; }
message Int64Value  { int64  value = 1; }
message UInt64Value { uint64 value = 1; }
message Int32Value  { int32  value = 1; }
message UInt32Value { uint32 value = 1; }
message BoolValue   { bool   value = 1; }
message StringValue { string value = 1; }
message BytesValue  { bytes  value = 1; }
`;
const STRINGVALUE_FULL_NAME = "google.protobuf.StringValue";
const STRINGVALUE_HELLO_RECORD = { value: "hello" };

/**
 * A two-message user-defined `.proto` whose sorted BFS output places
 * `Order` at index 1 and `Customer` at index 0 — exercises the
 * arbitrary-schema multi-message path (nonzero message index for a
 * caller-supplied schema).
 */
const ORDERS_PROTO_TEXT = `
syntax = "proto3";
package example;

message Customer {
  string name = 1;
  int64  id   = 2;
}

message Order {
  string sku      = 1;
  int32  quantity = 2;
  Customer buyer  = 3;
}
`;

/** Deliberately not one of the wire-core fixture records — exercises the
 *  schema-general Avro path end-to-end through the facade. */
const AVRO_ORDER_SCHEMA = {
  type: "record",
  name: "Order",
  namespace: "example.orders",
  fields: [
    { name: "id", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    {
      name: "tags",
      type: { type: "array", items: "string" },
    },
    {
      name: "status",
      type: {
        type: "enum",
        name: "Status",
        symbols: ["OPEN", "PAID", "CANCELLED"],
      },
    },
  ],
};

const AVRO_ORDER_RECORD = {
  id: "ord-42",
  amount: 19.99,
  quantity: 3,
  tags: ["urgent", "prime"],
  status: "PAID",
};

const JSON_ORDER_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  required: ["orderId", "total"],
  properties: {
    orderId: { type: "string", minLength: 1 },
    total: { type: "number", minimum: 0, multipleOf: 0.01 },
    tags: { type: "array", items: { type: "string" } },
  },
  additionalProperties: false,
};

const JSON_ORDER_INSTANCE = {
  orderId: "ord-2026-0001",
  total: 12.34,
  tags: ["premium", "expedited"],
};

describe("serde/facade — GsrSerializer + GsrDeserializer round-trip", () => {
  describe("AVRO", () => {
    it("serialize produces a full 18-byte-header wire message the deserializer round-trips (NONE)", () => {
      const serializer = new GsrSerializer();
      const deserializer = new GsrDeserializer();
      const wire = serializer.serialize({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });

      // Full wire message: at least the 18-byte header + non-empty payload.
      expect(wire.length).toBeGreaterThan(WIRE_FORMAT_HEADER_SIZE);
      expect(wire[0]).toBe(0x03);
      expect(wire[1]).toBe(0x00); // compression byte for NONE

      const decoded = deserializer.deserialize({
        format: DataFormat.AVRO,
        data: wire,
        schema: AVRO_ORDER_SCHEMA,
      });
      expect(decoded).toEqual(AVRO_ORDER_RECORD);
    });

    it("delegates compression to @gsr/core (ZLIB flag flips the header compression byte)", () => {
      const serializer = new GsrSerializer({ compression: "ZLIB" });
      const wire = serializer.serialize({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });

      expect(wire[1]).toBe(0x05); // ZLIB compression byte

      const deserialized = new GsrDeserializer().deserialize({
        format: DataFormat.AVRO,
        data: wire,
        schema: AVRO_ORDER_SCHEMA,
      });
      expect(deserialized).toEqual(AVRO_ORDER_RECORD);
    });

    it("honours the SPECIFIC decode-shape flag end-to-end", () => {
      const serializer = new GsrSerializer();
      const wire = serializer.serialize({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });
      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.AVRO,
        data: wire,
        schema: AVRO_ORDER_SCHEMA,
        avro: { recordType: "SPECIFIC" },
      });
      // Field values preserved (record-type is a decode-shape selector; wire
      // bytes are identical).
      expect((decoded as { id: string }).id).toBe(AVRO_ORDER_RECORD.id);
      expect((decoded as { amount: number }).amount).toBe(
        AVRO_ORDER_RECORD.amount,
      );
    });

    it("wraps an unparseable Avro schema as GsrIncompatibleDataError on serialize", () => {
      const serializer = new GsrSerializer();
      expect(() =>
        serializer.serialize({
          format: DataFormat.AVRO,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "orders",
          schema: "not-an-avro-schema",
          data: AVRO_ORDER_RECORD,
        }),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("wraps an unparseable Avro schema as GsrIncompatibleDataError on deserialize", () => {
      // Build a well-formed wire message first, then re-decode it with an
      // unparseable schema — the header is fine so the failure must come
      // from the format layer's schema-compile step.
      const wire = new GsrSerializer().serialize({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });
      const deserializer = new GsrDeserializer();
      expect(() =>
        deserializer.deserialize({
          format: DataFormat.AVRO,
          data: wire,
          schema: "not-an-avro-schema",
        }),
      ).toThrow(GsrIncompatibleDataError);
    });
  });

  describe("PROTOBUF", () => {
    it.skipIf(!referenceRootExists)("facade-produced StringValue message equals the NONE golden .bin byte-for-byte", () => {
      const golden = readFileSync(
        resolve(
          GOLDEN_JAVA_DIR,
          "protobuf__dynamic__comp-NONE__stringvalue-hello.bin",
        ),
      );
      const wire = new GsrSerializer().serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "wrappers",
        schema: WRAPPERS_PROTO_TEXT,
        data: STRINGVALUE_HELLO_RECORD,
        messageFullName: STRINGVALUE_FULL_NAME,
      });

      expect(wire.equals(golden)).toBe(true);
      // The message-index byte for StringValue is 0x06 (index 6 in
      // sorted-BFS of wrappers.proto).
      expect(wire[WIRE_FORMAT_HEADER_SIZE]).toBe(0x06);
    });

    it.skipIf(!referenceRootExists)("facade-produced StringValue message equals the ZLIB golden .bin byte-for-byte", () => {
      const golden = readFileSync(
        resolve(
          GOLDEN_JAVA_DIR,
          "protobuf__dynamic__comp-ZLIB__stringvalue-hello.bin",
        ),
      );
      const wire = new GsrSerializer({ compression: "ZLIB" }).serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "wrappers",
        schema: WRAPPERS_PROTO_TEXT,
        data: STRINGVALUE_HELLO_RECORD,
        messageFullName: STRINGVALUE_FULL_NAME,
      });

      expect(wire.equals(golden)).toBe(true);
    });

    it("round-trips a caller-defined multi-message .proto (Order with nested Customer)", () => {
      const serializer = new GsrSerializer();
      const deserializer = new GsrDeserializer();
      const order = {
        sku: "SKU-42",
        quantity: 3,
        buyer: { name: "alice", id: 1001 },
      };

      const wire = serializer.serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: ORDERS_PROTO_TEXT,
        data: order,
        messageFullName: "example.Order",
      });

      const decoded = deserializer.deserialize({
        format: DataFormat.PROTOBUF,
        data: wire,
        schema: ORDERS_PROTO_TEXT,
        messageFullName: "example.Order",
      }) as {
        sku: string;
        quantity: number;
        buyer: { name: string; id: number | bigint | { toNumber(): number } };
      };
      expect(decoded.sku).toBe("SKU-42");
      expect(decoded.quantity).toBe(3);
      expect(decoded.buyer.name).toBe("alice");
      // int64 comes back as a protobufjs Long by default; coerce for a
      // legible assertion (this is a test-only observation).
      const idAsNumber =
        typeof decoded.buyer.id === "object" && decoded.buyer.id !== null
          ? (decoded.buyer.id as { toNumber(): number }).toNumber()
          : Number(decoded.buyer.id);
      expect(idAsNumber).toBe(1001);
    });

    it("returns a concrete message instance when POJO is requested", () => {
      const serializer = new GsrSerializer();
      const deserializer = new GsrDeserializer();
      const wire = serializer.serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "wrappers",
        schema: WRAPPERS_PROTO_TEXT,
        data: STRINGVALUE_HELLO_RECORD,
        messageFullName: STRINGVALUE_FULL_NAME,
      });
      const decoded = deserializer.deserialize({
        format: DataFormat.PROTOBUF,
        data: wire,
        schema: WRAPPERS_PROTO_TEXT,
        messageFullName: STRINGVALUE_FULL_NAME,
        protobuf: { messageType: "POJO" },
      }) as { value: string; $type: { fullName: string } };
      expect(decoded.value).toBe("hello");
      // POJO instances carry the protobufjs $type back-reference; DYNAMIC
      // plain objects do not.
      expect(decoded.$type.fullName).toBe(`.${STRINGVALUE_FULL_NAME}`);
    });

    it("delegates message-index computation to @gsr/core — facade never touches the index", () => {
      // Verified indirectly: a serialize call for a message that sorts to
      // a nonzero index produces a wire message whose first payload byte
      // is that index (0x06 for StringValue). If the facade prepended the
      // index itself, the encode order would double-prepend and the
      // decode would fail; here the round-trip verifies correctness.
      const serializer = new GsrSerializer();
      const wire = serializer.serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "wrappers",
        schema: WRAPPERS_PROTO_TEXT,
        data: STRINGVALUE_HELLO_RECORD,
        messageFullName: STRINGVALUE_FULL_NAME,
      });
      // Confirm the message-index sits at the first byte of the payload
      // region — the core codec prepended it, not the facade.
      const { messageIndex } = decodeMessage(wire, { protobuf: true });
      expect(messageIndex).toBe(6);
    });

    it("throws GsrMessageTypeNotFoundError when messageFullName is missing on serialize", () => {
      const serializer = new GsrSerializer();
      expect(() =>
        serializer.serialize({
          format: DataFormat.PROTOBUF,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "wrappers",
          schema: WRAPPERS_PROTO_TEXT,
          data: STRINGVALUE_HELLO_RECORD,
        }),
      ).toThrow(GsrMessageTypeNotFoundError);
    });

    it("throws GsrIncompatibleDataError when the schema is not a string on serialize", () => {
      const serializer = new GsrSerializer();
      expect(() =>
        serializer.serialize({
          format: DataFormat.PROTOBUF,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "wrappers",
          schema: { not: "a string" },
          data: STRINGVALUE_HELLO_RECORD,
          messageFullName: STRINGVALUE_FULL_NAME,
        }),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("throws GsrMessageTypeNotFoundError when messageFullName is missing on deserialize", () => {
      const serializer = new GsrSerializer();
      const wire = serializer.serialize({
        format: DataFormat.PROTOBUF,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "wrappers",
        schema: WRAPPERS_PROTO_TEXT,
        data: STRINGVALUE_HELLO_RECORD,
        messageFullName: STRINGVALUE_FULL_NAME,
      });
      const deserializer = new GsrDeserializer();
      expect(() =>
        deserializer.deserialize({
          format: DataFormat.PROTOBUF,
          data: wire,
          schema: WRAPPERS_PROTO_TEXT,
        }),
      ).toThrow(GsrMessageTypeNotFoundError);
    });
  });

  describe("JSON", () => {
    it("serialize produces a full 18-byte-header wire message the deserializer round-trips (NONE)", () => {
      const serializer = new GsrSerializer();
      const deserializer = new GsrDeserializer();
      const wire = serializer.serialize({
        format: DataFormat.JSON,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: JSON_ORDER_SCHEMA,
        data: JSON_ORDER_INSTANCE,
      });

      expect(wire.length).toBeGreaterThan(WIRE_FORMAT_HEADER_SIZE);
      expect(wire[0]).toBe(0x03);
      expect(wire[1]).toBe(0x00);

      const decoded = deserializer.deserialize({
        format: DataFormat.JSON,
        data: wire,
        schema: JSON_ORDER_SCHEMA,
      });
      expect(decoded).toEqual(JSON_ORDER_INSTANCE);
    });

    it("delegates ZLIB compression to @gsr/core and round-trips through the deserializer", () => {
      const serializer = new GsrSerializer({ compression: "ZLIB" });
      const wire = serializer.serialize({
        format: DataFormat.JSON,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: JSON_ORDER_SCHEMA,
        data: JSON_ORDER_INSTANCE,
      });
      expect(wire[1]).toBe(0x05);
      const decoded = new GsrDeserializer().deserialize({
        format: DataFormat.JSON,
        data: wire,
        schema: JSON_ORDER_SCHEMA,
      });
      expect(decoded).toEqual(JSON_ORDER_INSTANCE);
    });
  });

  describe("schemaNameFor", () => {
    it("returns the topic when no schemaNameStrategy is configured (default is DefaultSchemaNameStrategy)", () => {
      const serializer = new GsrSerializer();
      const name = serializer.schemaNameFor({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });
      expect(name).toBe("orders");
    });

    it("honours an explicitly-configured DefaultSchemaNameStrategy", () => {
      const serializer = new GsrSerializer({
        schemaNameStrategy: new DefaultSchemaNameStrategy(),
      });
      expect(
        serializer.schemaNameFor({
          format: DataFormat.JSON,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "products",
          schema: JSON_ORDER_SCHEMA,
          data: JSON_ORDER_INSTANCE,
        }),
      ).toBe("products");
    });

    it("routes through a RecordNameStrategy when configured", () => {
      const serializer = new GsrSerializer({
        schemaNameStrategy: new RecordNameStrategy(),
      });
      const message = {
        value: "hello",
        $type: { fullName: `.${STRINGVALUE_FULL_NAME}` },
      };
      expect(
        serializer.schemaNameFor({
          format: DataFormat.PROTOBUF,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "wrappers",
          schema: WRAPPERS_PROTO_TEXT,
          data: message,
          messageFullName: STRINGVALUE_FULL_NAME,
        }),
      ).toBe(`wrappers-${STRINGVALUE_FULL_NAME}`);
    });
  });

  describe("error taxonomy", () => {
    it("wraps a bad-magic-byte wire message as GsrIncompatibleDataError on deserialize", () => {
      const serializer = new GsrSerializer();
      const wire = serializer.serialize({
        format: DataFormat.JSON,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: JSON_ORDER_SCHEMA,
        data: JSON_ORDER_INSTANCE,
      });
      const tampered = Buffer.from(wire);
      tampered[0] = 0xff; // bad magic byte

      const deserializer = new GsrDeserializer();
      expect(() =>
        deserializer.deserialize({
          format: DataFormat.JSON,
          data: tampered,
          schema: JSON_ORDER_SCHEMA,
        }),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("wraps a truncated payload as GsrIncompatibleDataError on deserialize", () => {
      const serializer = new GsrSerializer();
      const wire = serializer.serialize({
        format: DataFormat.AVRO,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: AVRO_ORDER_SCHEMA,
        data: AVRO_ORDER_RECORD,
      });
      // Chop off half the payload region — header stays valid, avsc will
      // trip on the truncated body and the facade must wrap that throw.
      const truncated = wire.subarray(
        0,
        WIRE_FORMAT_HEADER_SIZE + 4,
      );

      const deserializer = new GsrDeserializer();
      expect(() =>
        deserializer.deserialize({
          format: DataFormat.AVRO,
          data: truncated,
          schema: AVRO_ORDER_SCHEMA,
        }),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("rejects an unsupported DataFormat with GsrIncompatibleDataError", () => {
      const serializer = new GsrSerializer();
      expect(() =>
        serializer.serialize({
          format: "UNKNOWN" as unknown as DataFormat,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "orders",
          schema: JSON_ORDER_SCHEMA,
          data: JSON_ORDER_INSTANCE,
        }),
      ).toThrow(GsrIncompatibleDataError);
    });
  });

  describe("cross-package error identity (built dist)", () => {
    // Verifies that a serde-thrown `@gsr/core` error satisfies `instanceof`
    // against the class imported from `@gsr/core` at test-import time.
    // Loading the BUILT dist (not source) is essential — a source-level
    // import path resolves back to the same module graph as the test's
    // `@gsr/core` import, which makes the check trivially true and would
    // not catch a tsup bundle that inlined the class body. When the
    // built serde correctly externalizes `@gsr/core`, both throw-site
    // and test import share one class identity across the boundary.
    const packageRoot = resolve(
      dirname(fileURLToPath(import.meta.url)),
      "..",
      "..",
    );
    const esmEntry = resolve(packageRoot, "dist", "index.mjs");
    const cjsEntry = resolve(packageRoot, "dist", "index.cjs");

    it("built ESM serde: bad-magic-byte error is instanceof @gsr/core's GsrIncompatibleDataError", async () => {
      if (!existsSync(esmEntry)) {
        throw new Error(
          `Built serde ESM entry missing at ${esmEntry}; run \`npm run build\` in packages/serde before this test.`,
        );
      }
      const builtSerde = (await import(pathToFileURL(esmEntry).href)) as {
        GsrSerializer: new () => {
          serialize: (req: unknown) => Buffer;
        };
        GsrDeserializer: new () => {
          deserialize: (req: unknown) => unknown;
        };
        DataFormat: { JSON: unknown };
      };

      const wire = new builtSerde.GsrSerializer().serialize({
        format: builtSerde.DataFormat.JSON,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: JSON_ORDER_SCHEMA,
        data: JSON_ORDER_INSTANCE,
      });
      const tampered = Buffer.from(wire);
      tampered[0] = 0xff;

      let caught: unknown;
      try {
        new builtSerde.GsrDeserializer().deserialize({
          format: builtSerde.DataFormat.JSON,
          data: tampered,
          schema: JSON_ORDER_SCHEMA,
        });
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("built CJS serde: bad-magic-byte error is instanceof @gsr/core's GsrIncompatibleDataError", () => {
      if (!existsSync(cjsEntry)) {
        throw new Error(
          `Built serde CJS entry missing at ${cjsEntry}; run \`npm run build\` in packages/serde before this test.`,
        );
      }
      const require = createRequire(import.meta.url);
      const builtSerde = require(cjsEntry) as {
        GsrSerializer: new () => {
          serialize: (req: unknown) => Buffer;
        };
        GsrDeserializer: new () => {
          deserialize: (req: unknown) => unknown;
        };
        DataFormat: { JSON: unknown };
      };
      // The built CJS serde loads `@gsr/core` via `require()`, so we must
      // import the class from the SAME CJS build to compare identities.
      // (An ESM `@gsr/core` import lives in a separate Node module registry
      // — that is the standard ESM/CJS dual-package hazard, orthogonal to
      // the tsup-inlining hazard fixed by externalizing `@gsr/core`.)
      const builtCore = require("@gsr/core") as {
        GsrIncompatibleDataError: new (...args: unknown[]) => Error;
      };

      const wire = new builtSerde.GsrSerializer().serialize({
        format: builtSerde.DataFormat.JSON,
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        topic: "orders",
        schema: JSON_ORDER_SCHEMA,
        data: JSON_ORDER_INSTANCE,
      });
      const tampered = Buffer.from(wire);
      tampered[0] = 0xff;

      let caught: unknown;
      try {
        new builtSerde.GsrDeserializer().deserialize({
          format: builtSerde.DataFormat.JSON,
          data: tampered,
          schema: JSON_ORDER_SCHEMA,
        });
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(builtCore.GsrIncompatibleDataError);
    });
  });

  describe("serialize/deserialize composition contract", () => {
    it("routes every DataFormat and round-trips back to the input record", () => {
      const serializer = new GsrSerializer();
      const deserializer = new GsrDeserializer();

      const cases: Array<{
        format: DataFormat;
        schema: unknown;
        data: unknown;
        expected: unknown;
        messageFullName?: string;
      }> = [
        {
          format: DataFormat.AVRO,
          schema: AVRO_ORDER_SCHEMA,
          data: AVRO_ORDER_RECORD,
          expected: AVRO_ORDER_RECORD,
        },
        {
          format: DataFormat.PROTOBUF,
          schema: WRAPPERS_PROTO_TEXT,
          data: STRINGVALUE_HELLO_RECORD,
          expected: STRINGVALUE_HELLO_RECORD,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
        {
          format: DataFormat.JSON,
          schema: JSON_ORDER_SCHEMA,
          data: JSON_ORDER_INSTANCE,
          expected: JSON_ORDER_INSTANCE,
        },
      ];

      for (const c of cases) {
        const wire = serializer.serialize({
          format: c.format,
          schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
          topic: "topic-x",
          schema: c.schema,
          data: c.data,
          messageFullName: c.messageFullName,
        });
        // Every wire message starts with the 18-byte header (magic + comp
        // byte + 16-byte UUID). The header itself is produced by
        // `@gsr/core.encodeMessage` — the facade never touches those bytes.
        expect(wire.length).toBeGreaterThan(WIRE_FORMAT_HEADER_SIZE);
        expect(wire[0]).toBe(0x03);

        const decoded = deserializer.deserialize({
          format: c.format,
          data: wire,
          schema: c.schema,
          messageFullName: c.messageFullName,
        });
        expect(decoded).toEqual(c.expected);
      }
    });
  });
});
