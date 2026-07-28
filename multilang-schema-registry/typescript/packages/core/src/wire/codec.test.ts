import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { deflateSync } from "node:zlib";
import { describe, expect, it } from "vitest";

import {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_ZLIB,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_VERSION_BYTE,
} from "./constants.js";
import { GsrIncompatibleDataError } from "./errors.js";
import { encodeWireHeader } from "./wire-format.js";
import {
  decodeMessage,
  encodeMessage,
  type EncodeParams,
} from "./codec.js";

const GOLDEN_UUID = "01020304-0506-0708-090a-0b0c0d0e0f10";

const WRAPPERS_PROTO = `syntax = "proto3";
package google.protobuf;

message DoubleValue { double value = 1; }
message FloatValue { float value = 1; }
message Int64Value { int64 value = 1; }
message UInt64Value { uint64 value = 1; }
message Int32Value { int32 value = 1; }
message UInt32Value { uint32 value = 1; }
message BoolValue { bool value = 1; }
message StringValue { string value = 1; }
message BytesValue { bytes value = 1; }
`;

// StringValue { value: "hello" } encoded body: tag=1(string) len=5 h e l l o.
const STRING_VALUE_HELLO_BODY = Buffer.from([
  0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
]);

describe("encodeMessage — order: index → compress → header", () => {
  it("NONE + non-protobuf: payload appears verbatim after the 18-byte header", () => {
    const payload = Buffer.from("plain-avro-body");
    const params: EncodeParams = {
      schemaVersionId: GOLDEN_UUID,
      payload,
      compressionType: "NONE",
    };

    const out = encodeMessage(params);

    expect(out[0]).toBe(WIRE_FORMAT_VERSION_BYTE);
    expect(out[1]).toBe(COMPRESSION_BYTE_NONE);
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE).equals(payload)).toBe(true);
  });

  it("ZLIB + non-protobuf: post-header bytes equal zlib.deflateSync(payload)", () => {
    const payload = Buffer.from("a-repeating-body-".repeat(4));
    const expectedCompressed = deflateSync(payload);

    const out = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload,
      compressionType: "ZLIB",
    });

    expect(out[1]).toBe(COMPRESSION_BYTE_ZLIB);
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE).equals(expectedCompressed)).toBe(true);
    // Locks the zlib default-level header prefix.
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE, WIRE_FORMAT_HEADER_SIZE + 2).toString("hex")).toBe("789c");
  });

  it("protobuf + NONE: prepends the message-index varint, does not compress", () => {
    const out = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload: STRING_VALUE_HELLO_BODY,
      compressionType: "NONE",
      protobuf: {
        schemaText: WRAPPERS_PROTO,
        messageFullName: "google.protobuf.StringValue",
      },
    });

    expect(out[1]).toBe(COMPRESSION_BYTE_NONE);
    // StringValue's index within lexicographically-sorted wrappers.proto is 6.
    // The payload region should be [0x06, ...STRING_VALUE_HELLO_BODY].
    const region = out.subarray(WIRE_FORMAT_HEADER_SIZE);
    expect(region[0]).toBe(0x06);
    expect(region.subarray(1).equals(STRING_VALUE_HELLO_BODY)).toBe(true);
  });

  it("protobuf + ZLIB: compresses the (index || body) buffer, not the body alone", () => {
    // If a broken implementation compressed the body first and then prepended
    // the index, the compressed region would not equal deflateSync([06,...]).
    const indexed = Buffer.concat([Buffer.from([0x06]), STRING_VALUE_HELLO_BODY]);
    const expectedCompressed = deflateSync(indexed);

    const out = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload: STRING_VALUE_HELLO_BODY,
      compressionType: "ZLIB",
      protobuf: {
        schemaText: WRAPPERS_PROTO,
        messageFullName: "google.protobuf.StringValue",
      },
    });

    expect(out[1]).toBe(COMPRESSION_BYTE_ZLIB);
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE).equals(expectedCompressed)).toBe(true);
  });

  it("emits a 20-byte output for a 2-byte NONE payload (header + payload)", () => {
    const payload = Buffer.from([0xaa, 0xbb]);
    const out = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload,
      compressionType: "NONE",
    });
    expect(out.length).toBe(WIRE_FORMAT_HEADER_SIZE + payload.length);
  });
});

describe("decodeMessage — inverts the encode order", () => {
  it("NONE + non-protobuf: returns the raw payload and NONE mode", () => {
    const payload = Buffer.from("plain-body");
    const wire = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload,
      compressionType: "NONE",
    });

    const decoded = decodeMessage(wire);

    expect(decoded.schemaVersionId).toBe(GOLDEN_UUID);
    expect(decoded.compressionType).toBe("NONE");
    expect(decoded.payload.equals(payload)).toBe(true);
    expect(decoded.messageIndex).toBeUndefined();
  });

  it("ZLIB + non-protobuf: decompresses and reports ZLIB mode", () => {
    const payload = Buffer.from("compressible-".repeat(3));
    const wire = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload,
      compressionType: "ZLIB",
    });

    const decoded = decodeMessage(wire);

    expect(decoded.compressionType).toBe("ZLIB");
    expect(decoded.payload.equals(payload)).toBe(true);
  });

  it("protobuf + NONE: strips the varint and returns messageIndex", () => {
    const wire = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload: STRING_VALUE_HELLO_BODY,
      compressionType: "NONE",
      protobuf: {
        schemaText: WRAPPERS_PROTO,
        messageFullName: "google.protobuf.StringValue",
      },
    });

    const decoded = decodeMessage(wire, { protobuf: true });

    expect(decoded.compressionType).toBe("NONE");
    expect(decoded.messageIndex).toBe(6);
    expect(decoded.payload.equals(STRING_VALUE_HELLO_BODY)).toBe(true);
  });

  it("protobuf + ZLIB: round-trip reconstructs the original body", () => {
    const wire = encodeMessage({
      schemaVersionId: GOLDEN_UUID,
      payload: STRING_VALUE_HELLO_BODY,
      compressionType: "ZLIB",
      protobuf: {
        schemaText: WRAPPERS_PROTO,
        messageFullName: "google.protobuf.StringValue",
      },
    });

    const decoded = decodeMessage(wire, { protobuf: true });

    expect(decoded.schemaVersionId).toBe(GOLDEN_UUID);
    expect(decoded.compressionType).toBe("ZLIB");
    expect(decoded.messageIndex).toBe(6);
    expect(decoded.payload.equals(STRING_VALUE_HELLO_BODY)).toBe(true);
  });

  it("propagates GsrIncompatibleDataError from the header on a short buffer", () => {
    expect(() => decodeMessage(Buffer.alloc(5))).toThrow(GsrIncompatibleDataError);
  });
});

describe("decodeMessage — uniform negative-path error surface", () => {
  // A valid 18-byte header whose compression byte says ZLIB, followed by a
  // body of 0xff bytes that pako.inflate cannot decode. Without the codec
  // wrap this leaks pako 1.x's bare-string throw ("invalid block type")
  // straight out of decodeMessage; the wrap converts every such throw to
  // GsrIncompatibleDataError.
  const CORRUPT_ZLIB_BODY = Buffer.from([
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
  ]);
  const CORRUPT_ZLIB_WIRE = encodeWireHeader(
    GOLDEN_UUID,
    COMPRESSION_BYTE_ZLIB,
    CORRUPT_ZLIB_BODY,
  );

  it("wraps a corrupt-ZLIB body as GsrIncompatibleDataError (not a bare string)", () => {
    let caught: unknown;
    try {
      decodeMessage(CORRUPT_ZLIB_WIRE);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
    // The direct test that a bare string does NOT escape: without the wrap,
    // pako 1.x's bare-string throw would land as a plain string here.
    expect(typeof caught).not.toBe("string");
  });

  it("passes GsrIncompatibleDataError through unchanged (no double-wrap)", () => {
    // A version byte other than 0x03 makes decodeWireHeader throw
    // GsrIncompatibleDataError. The catch in decodeMessage must rethrow the
    // same instance rather than wrapping it as its own cause.
    const badVersion = Buffer.alloc(WIRE_FORMAT_HEADER_SIZE);
    badVersion[0] = 0x00;
    badVersion[1] = COMPRESSION_BYTE_NONE;

    let caught: unknown;
    try {
      decodeMessage(badVersion);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
    // If we had wrapped, the message would start with "failed to decode…".
    expect((caught as Error).message).toContain("wire-format version byte");
    expect((caught as { cause?: unknown }).cause).toBeUndefined();
  });

  it("wraps a decode-time throw whose value is a bare string as GsrIncompatibleDataError", () => {
    // Belt-and-braces for the decodeMessage wrap itself: even if a future
    // decode-pipeline function throws a bare string (bypassing every
    // intermediate wrap layer), decodeMessage must still convert it to a
    // GsrIncompatibleDataError with the string on cause. Simulate that by
    // monkey-patching Buffer.prototype.subarray for the duration of one
    // call so a downstream slice throws a bare string.
    const originalSubarray = Buffer.prototype.subarray;
    Buffer.prototype.subarray = function (
      this: Buffer,
      start?: number,
      end?: number,
    ): Buffer {
      // Trigger only for the exact post-header slice (offset 18) on the
      // valid-header buffer below — otherwise the header decoder itself
      // fails on the internal 8-byte UUID slice and the wrap never fires.
      if (start === WIRE_FORMAT_HEADER_SIZE) {
        throw "synthetic bare-string throw";
      }
      return originalSubarray.call(this, start, end) as Buffer;
    };

    let caught: unknown;
    try {
      const wire = encodeWireHeader(
        GOLDEN_UUID,
        COMPRESSION_BYTE_NONE,
        Buffer.from([0x00]),
      );
      decodeMessage(wire);
    } catch (err) {
      caught = err;
    } finally {
      Buffer.prototype.subarray = originalSubarray;
    }

    expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
    expect(typeof caught).not.toBe("string");
    expect((caught as Error).message).toContain(
      "failed to decode wire-format message",
    );
    expect((caught as { cause?: unknown }).cause).toBe(
      "synthetic bare-string throw",
    );
  });

  it("wraps a message-index strip failure on a corrupt protobuf payload", () => {
    // A valid NONE header whose payload is a 10-byte run of 0x80 — every
    // byte has continuation bit set, so the varint never terminates and
    // stripMessageIndex throws GsrIncompatibleDataError. Proves the strip
    // stage IS in the try/catch surface (the class passes through unwrapped
    // by design, but the assertion below is on the class, not the wrap).
    const unterminatedVarint = Buffer.alloc(10, 0x80);
    const wire = encodeWireHeader(
      GOLDEN_UUID,
      COMPRESSION_BYTE_NONE,
      unterminatedVarint,
    );

    expect(() => decodeMessage(wire, { protobuf: true })).toThrow(
      GsrIncompatibleDataError,
    );
  });

  it("surfaces every decode failure as GsrIncompatibleDataError (no bare-string leak)", () => {
    // Cover the top-level contract: for every corrupt input in this suite,
    // the caught value is an Error subclass — never a raw string.
    const cases = [
      Buffer.alloc(5),                    // short buffer
      CORRUPT_ZLIB_WIRE,                  // valid header + un-inflatable body
      encodeWireHeader(                   // truncated protobuf varint
        GOLDEN_UUID,
        COMPRESSION_BYTE_NONE,
        Buffer.alloc(10, 0x80),
      ),
    ];
    for (const wire of cases) {
      let caught: unknown;
      try {
        decodeMessage(wire, { protobuf: true });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(Error);
      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      expect(typeof caught).not.toBe("string");
    }
  });
});

describe("core barrel exposes the wire surface", () => {
  it("re-exports encodeMessage/decodeMessage through the core src barrel", async () => {
    const barrel = await import("../index.js");
    expect(typeof barrel.encodeMessage).toBe("function");
    expect(typeof barrel.decodeMessage).toBe("function");
    expect(typeof barrel.encodeWireHeader).toBe("function");
    expect(typeof barrel.decodeWireHeader).toBe("function");
    expect(typeof barrel.compressZlib).toBe("function");
    expect(typeof barrel.decompressZlib).toBe("function");
    expect(typeof barrel.computeMessageIndex).toBe("function");
    expect(typeof barrel.prependMessageIndex).toBe("function");
    expect(typeof barrel.stripMessageIndex).toBe("function");
    expect(barrel.WIRE_FORMAT_HEADER_SIZE).toBe(WIRE_FORMAT_HEADER_SIZE);
  });
});

describe("core stays transport-agnostic", () => {
  const thisFileDir = dirname(fileURLToPath(import.meta.url));
  const coreSrcDir = resolve(thisFileDir, "..");

  function walk(dir: string): string[] {
    const out: string[] = [];
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        out.push(...walk(full));
      } else if (entry.isFile() && entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")) {
        out.push(full);
      }
    }
    return out;
  }

  it("no Kafka import lives under packages/core/src", () => {
    const offenders: string[] = [];
    for (const file of walk(coreSrcDir)) {
      const source = readFileSync(file, "utf8");
      if (/kafkajs|node-rdkafka|kafka-node|@confluentinc\/kafka-javascript/i.test(source)) {
        offenders.push(file);
      }
    }
    expect(offenders).toEqual([]);
  });
});
