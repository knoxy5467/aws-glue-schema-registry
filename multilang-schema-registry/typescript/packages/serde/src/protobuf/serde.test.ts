import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import protobuf from "protobufjs";
import { describe, expect, it } from "vitest";

import {
  decodeMessage,
  encodeMessage,
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
  WIRE_FORMAT_HEADER_SIZE,
} from "@gsr/core";

import { GsrDeserializer } from "../facade/deserializer.js";
import { DataFormat } from "../facade/types.js";

import {
  deserializeProtobufBody,
  serializeProtobufBody,
} from "./serde.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
} from "../test-support/reference-root.js";

const STRINGVALUE_FULL_NAME = "google.protobuf.StringValue";
const STRINGVALUE_HELLO_RECORD = { value: "hello" };
const GOLDEN_SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

/**
 * The nine wrapper messages from `google/protobuf/wrappers.proto`, inlined so
 * this test has exactly one library-import dependency (`protobufjs`) and
 * needs no cross-repo `.proto` file access. Lexicographic order places
 * `google.protobuf.StringValue` at message-index 6 in the sorted BFS output,
 * the canonical `index > 0` example the Java reference uses.
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

/**
 * Two-message user-defined `.proto` whose sorted BFS output places `Order`
 * at index 1 and `Customer` at index 0 — exercises the arbitrary-schema
 * multi-message path (nonzero message index for a caller-supplied schema).
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

describe("serde/protobuf/serde — full-breadth Dynamic + POJO", () => {
  describe("serializeProtobufBody", () => {
    it("produces the message body only (no message-index varint prefix)", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      // Java-canonical StringValue{value:"hello"} body: tag(1,LEN=0x0a) len=5 "hello".
      // The message-index (0x06) is NOT present — that's a core-codec concern.
      expect(Array.from(body)).toEqual([0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]);
    });

    it("accepts a leading-dot fully-qualified message name", () => {
      const withDot = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        ".google.protobuf.StringValue",
        STRINGVALUE_HELLO_RECORD,
      );
      const withoutDot = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      expect(withDot.equals(withoutDot)).toBe(true);
    });

    it("accepts an existing protobufjs message instance (not just plain objects)", () => {
      const root = protobuf.parse(WRAPPERS_PROTO_TEXT, { keepCase: true }).root;
      const type = root.lookupType(STRINGVALUE_FULL_NAME);
      const instance = type.create(STRINGVALUE_HELLO_RECORD);

      const fromInstance = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        instance,
      );
      const fromObject = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      expect(fromInstance.equals(fromObject)).toBe(true);
    });

    it("throws GsrMessageTypeNotFoundError for a message name absent from the schema", () => {
      expect(() =>
        serializeProtobufBody(
          WRAPPERS_PROTO_TEXT,
          "google.protobuf.NopeValue",
          STRINGVALUE_HELLO_RECORD,
        ),
      ).toThrow(GsrMessageTypeNotFoundError);
    });

    it("throws GsrIncompatibleDataError for unparseable .proto text", () => {
      expect(() =>
        serializeProtobufBody(
          "this is not a .proto",
          STRINGVALUE_FULL_NAME,
          STRINGVALUE_HELLO_RECORD,
        ),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("throws GsrIncompatibleDataError when the record is not an object", () => {
      expect(() =>
        serializeProtobufBody(
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
          "hello" as unknown,
        ),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("throws GsrIncompatibleDataError when the record fails protobuf verify", () => {
      // StringValue.value is a string; passing an integer trips Type.verify.
      let caught: unknown;
      try {
        serializeProtobufBody(WRAPPERS_PROTO_TEXT, STRINGVALUE_FULL_NAME, {
          value: 42,
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      expect((caught as Error).message).toContain("verify failed");
    });
  });

  describe("deserializeProtobufBody — DYNAMIC (default)", () => {
    it("round-trips an arbitrary StringValue body back to a plain object", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      const decoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
      );

      expect(decoded).toEqual(STRINGVALUE_HELLO_RECORD);
    });

    it("round-trips an arbitrary caller-defined multi-message .proto (Order with nested Customer)", () => {
      const order = {
        sku: "SKU-42",
        quantity: 3,
        buyer: { name: "alice", id: 1001 },
      };

      const body = serializeProtobufBody(
        ORDERS_PROTO_TEXT,
        "example.Order",
        order,
      );

      const decoded = deserializeProtobufBody(
        body,
        ORDERS_PROTO_TEXT,
        "example.Order",
      );

      // `int64` fields come back as `protobufjs.util.Long` unless the type is
      // rewired — `toObject({ longs: Number })` would coerce them; the serde's
      // default `toObject` (defaults off) leaves them as Long instances. Assert
      // on the shape and the primitive fields, and coerce the Long to a number
      // for a legible comparison.
      const asRecord = decoded as {
        sku: string;
        quantity: number;
        buyer: { name: string; id: { toNumber: () => number } | number };
      };
      expect(asRecord.sku).toBe("SKU-42");
      expect(asRecord.quantity).toBe(3);
      expect(asRecord.buyer.name).toBe("alice");
      const buyerId =
        typeof asRecord.buyer.id === "object"
          ? asRecord.buyer.id.toNumber()
          : asRecord.buyer.id;
      expect(buyerId).toBe(1001);
    });

    it("returns a plain object (not a protobufjs message instance)", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      const decoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
      );

      // A plain object's prototype is Object.prototype — a message instance's
      // prototype is Type.ctor.prototype (never Object.prototype directly).
      expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    });

    it("throws GsrIncompatibleDataError on a truncated body (varint LEN promises more than is present)", () => {
      // StringValue tag=0x0a says LEN follows; declare LEN=5 but supply only 3
      // bytes — protobufjs throws on the truncated slice and we must wrap it.
      const truncated = Buffer.from([0x0a, 0x05, 0x68, 0x65, 0x6c]);
      expect(() =>
        deserializeProtobufBody(
          truncated,
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
        ),
      ).toThrow(GsrIncompatibleDataError);
    });

    it("throws GsrIncompatibleDataError on a garbage body (invalid wire-type)", () => {
      // 0xff is neither a valid tag nor a valid wire type; protobufjs throws.
      const garbage = Buffer.from([0xff, 0xff, 0xff, 0xff]);
      let caught: unknown;
      try {
        deserializeProtobufBody(
          garbage,
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
        );
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      // Confirm no raw library error leaks: a bare string or plain Error would
      // fail this instanceof, and the wrap preserves the original on `cause`.
      expect(typeof caught).not.toBe("string");
    });

    it("throws GsrMessageTypeNotFoundError when the message name is absent on decode", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      expect(() =>
        deserializeProtobufBody(
          body,
          WRAPPERS_PROTO_TEXT,
          "google.protobuf.NopeValue",
        ),
      ).toThrow(GsrMessageTypeNotFoundError);
    });
  });

  describe("deserializeProtobufBody — POJO", () => {
    it("returns a concrete protobufjs message instance (not a plain object)", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      const decoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        { messageType: "POJO" },
      );

      // A protobufjs message instance's $type field points at its Type; a
      // plain object does not carry $type. That is the concrete-vs-dynamic
      // decode-shape distinction the Go reference draws with `POJO` vs
      // `DYNAMIC_MESSAGE`.
      expect((decoded as { $type?: unknown }).$type).toBeDefined();
      expect(
        (decoded as { $type: { fullName: string } }).$type.fullName,
      ).toBe(".google.protobuf.StringValue");
    });

    it("preserves the field value on the concrete instance", () => {
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      const decoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        { messageType: "POJO" },
      );

      expect((decoded as { value: string }).value).toBe("hello");
    });

    it("wire bytes are IDENTICAL for DYNAMIC and POJO — record-type is decode-shape only", () => {
      // Encode once; decode twice with different shapes. The input body
      // never changes — the shape switch is purely on the decoded object.
      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      const dynamicDecoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
      );
      const pojoDecoded = deserializeProtobufBody(
        body,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        { messageType: "POJO" },
      );

      expect(dynamicDecoded).toEqual({ value: "hello" });
      expect((pojoDecoded as { value: string }).value).toBe("hello");
      expect(Object.getPrototypeOf(dynamicDecoded)).toBe(Object.prototype);
      expect(Object.getPrototypeOf(pojoDecoded)).not.toBe(Object.prototype);
    });

    it("round-trips a multi-message .proto via POJO", () => {
      const order = { sku: "SKU-99", quantity: 7, buyer: { name: "bob", id: 5 } };

      const body = serializeProtobufBody(
        ORDERS_PROTO_TEXT,
        "example.Order",
        order,
      );
      const decoded = deserializeProtobufBody(
        body,
        ORDERS_PROTO_TEXT,
        "example.Order",
        { messageType: "POJO" },
      );

      const asRecord = decoded as {
        sku: string;
        quantity: number;
        buyer: { name: string; id: { toNumber: () => number } | number };
        $type: { fullName: string };
      };
      expect(asRecord.sku).toBe("SKU-99");
      expect(asRecord.quantity).toBe(7);
      expect(asRecord.buyer.name).toBe("bob");
      expect(asRecord.$type.fullName).toBe(".example.Order");
    });
  });

  describe.skipIf(!referenceRootExists)("golden-vector regression (composes with @gsr/core to reproduce Java bytes)", () => {
    // A multi-message .proto sorts targets to nonzero message-indices; the
    // canonical fixture is `wrappers.proto` where StringValue lives at index 6
    // (proven in @gsr/core.computeMessageIndex tests). Asserting that the
    // core codec composed with this serde's body reproduces the Java golden
    // .bin byte-for-byte confirms two things at once:
    //   1. The serde produces the correct body bytes.
    //   2. The core prepends the correct message-index (6) around them.

    it("composes with @gsr/core to produce the NONE-compressed protobuf golden .bin", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "protobuf__dynamic__comp-NONE__stringvalue-hello.bin",
      );
      const golden = readFileSync(goldenFile);

      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: body,
        compressionType: "NONE",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      expect(wire.equals(golden)).toBe(true);
      // The prepended message-index byte for StringValue is 0x06 (message
      // sits at position 6 in sorted-BFS of wrappers.proto).
      expect(wire[WIRE_FORMAT_HEADER_SIZE]).toBe(0x06);
    });

    it("composes with @gsr/core to produce the ZLIB-compressed protobuf golden .bin", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "protobuf__dynamic__comp-ZLIB__stringvalue-hello.bin",
      );
      const golden = readFileSync(goldenFile);

      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: body,
        compressionType: "ZLIB",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      expect(wire.equals(golden)).toBe(true);
    });

    it("matches the CONCRETE record-type golden .bin as well (equal SHA-256 per provenance)", () => {
      // PROVENANCE records dynamic/concrete stringvalue-hello with identical
      // SHA-256 — one dynamic-encode-plus-core-compose reproduces both.
      const concreteGolden = readFileSync(
        resolve(
          GOLDEN_JAVA_DIR,
          "protobuf__concrete__comp-NONE__stringvalue-hello.bin",
        ),
      );

      const body = serializeProtobufBody(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: body,
        compressionType: "NONE",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      expect(wire.equals(concreteGolden)).toBe(true);
    });
  });

  /**
   * Composed end-to-end negative-path wrap: drive a protobuf failure through
   * the full `@gsr/core` decode composition (18-byte header parse →
   * decompression → message-index strip → protobuf body decode) and confirm
   * the surfaced error is `GsrIncompatibleDataError` with the ORIGINAL
   * `protobufjs` throw preserved on `.cause`.
   *
   * Isolated `deserializeProtobufBody` failures are already covered above.
   * These tests close the remaining gap by proving the wrap survives
   * composition — both when driven directly via `decodeMessage` +
   * `deserializeProtobufBody`, and when driven via the `GsrDeserializer`
   * facade which additionally routes the throw through `normalizeSerdeThrow`.
   * Preserving `.cause` across composition matters: diagnostics from a
   * wire-facing customer depend on the wrapped `protobufjs` message to
   * pinpoint the truncation/offset that caused the failure.
   */
  describe("composed end-to-end wrap (encode → wire → decode)", () => {
    /**
     * Build a valid wire message, then corrupt only the protobuf body region
     * so the header and message-index survive `decodeMessage` and the failure
     * lands squarely inside `protobufjs.Type.decode`.
     */
    function buildWireWithCorruptedBody(): Buffer {
      // A single-byte payload after the message-index. StringValue tag 0x0a
      // signals a LEN-delimited field; without the length varint following
      // it, `protobufjs.Type.decode` throws mid-parse. The message-index
      // (0x06 for StringValue in the sorted BFS) is prepended by the core
      // codec; the wire decode succeeds through header + index strip and
      // hands the malformed body to the serde.
      const badBody = Buffer.from([0x0a]);
      return encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: badBody,
        compressionType: "NONE",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });
    }

    it("direct composition — malformed body surfaces GsrIncompatibleDataError with protobufjs .cause preserved", () => {
      const wire = buildWireWithCorruptedBody();

      // Sanity-check: the core layer decodes cleanly (header + index strip).
      // The failure MUST originate inside the protobuf serde, not the core.
      const { payload } = decodeMessage(wire, { protobuf: true });
      expect(payload.length).toBeGreaterThan(0);

      let caught: unknown;
      try {
        deserializeProtobufBody(payload, WRAPPERS_PROTO_TEXT, STRINGVALUE_FULL_NAME);
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      // No raw library throw escapes — a bare string or plain Error would
      // fail this instanceof, and a double-wrap (cause wrapped as a
      // GsrIncompatibleDataError instead of the original protobufjs throw)
      // would fail the .cause instanceof Error check below.
      const wrapped = caught as GsrIncompatibleDataError & { cause?: unknown };
      expect(wrapped.cause).toBeDefined();
      expect(wrapped.cause).toBeInstanceOf(Error);
      // `.cause` must be the ORIGINAL protobufjs throw, not another
      // GsrIncompatibleDataError — the wrap flattens exactly one level so
      // diagnostics see the raw protobuf offset/reason.
      expect(wrapped.cause).not.toBeInstanceOf(GsrIncompatibleDataError);
      // The wrap's message prefixes the body-decode context, and the
      // original protobufjs message is appended after the colon.
      expect(wrapped.message).toContain("failed to decode protobuf body");
    });

    it("facade composition — malformed body surfaces GsrIncompatibleDataError with protobufjs .cause preserved", () => {
      const wire = buildWireWithCorruptedBody();

      let caught: unknown;
      try {
        new GsrDeserializer().deserialize({
          format: DataFormat.PROTOBUF,
          data: wire,
          schema: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        });
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      const wrapped = caught as GsrIncompatibleDataError & { cause?: unknown };
      // The facade's `normalizeSerdeThrow` passes an already-wrapped
      // `GsrIncompatibleDataError` through unchanged — the serde-level
      // `.cause` (= raw protobufjs throw) MUST survive the facade layer.
      expect(wrapped.cause).toBeDefined();
      expect(wrapped.cause).toBeInstanceOf(Error);
      expect(wrapped.cause).not.toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("direct composition — truncated LEN body surfaces GsrIncompatibleDataError with .cause preserved", () => {
      // A second failure shape: tag 0x0a + LEN=0x05 promises 5 body bytes but
      // supplies only 3. `protobufjs.Type.decode` throws mid-slice.
      const truncatedBody = Buffer.from([0x0a, 0x05, 0x68, 0x65, 0x6c]);
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: truncatedBody,
        compressionType: "NONE",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      const { payload } = decodeMessage(wire, { protobuf: true });
      let caught: unknown;
      try {
        deserializeProtobufBody(payload, WRAPPERS_PROTO_TEXT, STRINGVALUE_FULL_NAME);
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      const wrapped = caught as GsrIncompatibleDataError & { cause?: unknown };
      expect(wrapped.cause).toBeInstanceOf(Error);
      expect(wrapped.cause).not.toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("direct composition — ZLIB-compressed malformed body surfaces GsrIncompatibleDataError with .cause preserved", () => {
      // Composed decode with ZLIB adds a decompression stage between header
      // parse and message-index strip. The wrap must hold through that too.
      const badBody = Buffer.from([0x0a]);
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: badBody,
        compressionType: "ZLIB",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      const { payload, compressionType } = decodeMessage(wire, { protobuf: true });
      expect(compressionType).toBe("ZLIB");

      let caught: unknown;
      try {
        deserializeProtobufBody(payload, WRAPPERS_PROTO_TEXT, STRINGVALUE_FULL_NAME);
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      const wrapped = caught as GsrIncompatibleDataError & { cause?: unknown };
      expect(wrapped.cause).toBeInstanceOf(Error);
      expect(wrapped.cause).not.toBeInstanceOf(GsrIncompatibleDataError);
    });

    it("facade composition — unresolvable message type surfaces GsrMessageTypeNotFoundError", () => {
      // Second failure shape: encoding is fine; decode names a message absent
      // from the schema. The taxonomy directs this to
      // `GsrMessageTypeNotFoundError`, not `GsrIncompatibleDataError`, and
      // there is no library-level throw to preserve — `looked instanceof
      // protobuf.Type` is `false` and the serde raises the typed error
      // directly.
      const wire = encodeMessage({
        schemaVersionId: GOLDEN_SCHEMA_VERSION_ID,
        payload: serializeProtobufBody(
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
          STRINGVALUE_HELLO_RECORD,
        ),
        compressionType: "NONE",
        protobuf: {
          schemaText: WRAPPERS_PROTO_TEXT,
          messageFullName: STRINGVALUE_FULL_NAME,
        },
      });

      expect(() =>
        new GsrDeserializer().deserialize({
          format: DataFormat.PROTOBUF,
          data: wire,
          schema: WRAPPERS_PROTO_TEXT,
          messageFullName: "google.protobuf.NopeValue",
        }),
      ).toThrow(GsrMessageTypeNotFoundError);
    });
  });
});
