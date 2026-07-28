import { describe, expect, it } from "vitest";
import {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_ZLIB,
  WIRE_FORMAT_HEADER_SIZE,
} from "./constants.js";
import { GsrIncompatibleDataError } from "./errors.js";
import { decodeWireHeader, encodeWireHeader } from "./wire-format.js";

// Fixed UUID recorded in every golden-vector descriptor and in the
// oracle's PROVENANCE.md. The 16 bytes 01 02 03 … 0f 10 make MSB/LSB
// big-endian layout trivially auditable.
const GOLDEN_UUID = "01020304-0506-0708-090a-0b0c0d0e0f10";
const GOLDEN_UUID_BYTES = Buffer.from([
  0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // MSB big-endian
  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, // LSB big-endian
]);

// A second UUID with easily-distinguished halves — locks endianness down.
const ENDIAN_UUID = "11223344-5566-7788-99aa-bbccddeeff00";
const ENDIAN_UUID_BYTES = Buffer.from([
  0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
  0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
]);

describe("wire/wire-format encodeWireHeader", () => {
  it("reproduces the golden header prefix for the fixed UUID + NONE + empty payload", () => {
    const out = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_NONE, Buffer.alloc(0));
    expect(out).toHaveLength(WIRE_FORMAT_HEADER_SIZE);

    const expected = Buffer.concat([
      Buffer.from([0x03, 0x00]),
      GOLDEN_UUID_BYTES,
    ]);
    expect(out.equals(expected)).toBe(true);

    // Explicit byte-by-byte spot check on the leading nibble the spec calls out.
    expect(out.subarray(0, 4).toString("hex")).toBe("03000102");
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE - 2, WIRE_FORMAT_HEADER_SIZE).toString("hex")).toBe(
      "0f10",
    );
  });

  it("lays out UUID bytes MSB-then-LSB big-endian", () => {
    const out = encodeWireHeader(ENDIAN_UUID, COMPRESSION_BYTE_NONE, Buffer.alloc(0));
    expect(out.subarray(2, WIRE_FORMAT_HEADER_SIZE).equals(ENDIAN_UUID_BYTES)).toBe(true);
  });

  it("writes 0x00 for NONE and 0x05 for ZLIB in the second header byte", () => {
    const noneOut = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_NONE, Buffer.alloc(0));
    const zlibOut = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_ZLIB, Buffer.alloc(0));
    expect(noneOut[1]).toBe(0x00);
    expect(zlibOut[1]).toBe(0x05);
  });

  it("appends the caller-supplied payload verbatim after the 18-byte header", () => {
    const payload = Buffer.from("hello", "utf8");
    const out = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_NONE, payload);
    expect(out).toHaveLength(WIRE_FORMAT_HEADER_SIZE + payload.length);
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE).equals(payload)).toBe(true);
  });

  it("does not treat ZLIB compression as a signal to compress — payload is passed through", () => {
    // Bytes chosen to look nothing like real zlib output.
    const rawPayload = Buffer.from([0xaa, 0xbb, 0xcc]);
    const out = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_ZLIB, rawPayload);
    expect(out.subarray(WIRE_FORMAT_HEADER_SIZE).equals(rawPayload)).toBe(true);
  });

  it("throws GsrIncompatibleDataError for an out-of-set compression byte", () => {
    expect(() =>
      encodeWireHeader(GOLDEN_UUID, 0x01, Buffer.alloc(0)),
    ).toThrow(GsrIncompatibleDataError);
    expect(() =>
      encodeWireHeader(GOLDEN_UUID, 0xff, Buffer.alloc(0)),
    ).toThrow(GsrIncompatibleDataError);
  });

  it("throws GsrIncompatibleDataError for a malformed UUID string", () => {
    expect(() =>
      encodeWireHeader("not-a-uuid", COMPRESSION_BYTE_NONE, Buffer.alloc(0)),
    ).toThrow(GsrIncompatibleDataError);
    // Right length, wrong format (missing dashes).
    expect(() =>
      encodeWireHeader(
        "0102030405060708090a0b0c0d0e0f10",
        COMPRESSION_BYTE_NONE,
        Buffer.alloc(0),
      ),
    ).toThrow(GsrIncompatibleDataError);
    // Right dashes, non-hex char.
    expect(() =>
      encodeWireHeader(
        "0102030z-0506-0708-090a-0b0c0d0e0f10",
        COMPRESSION_BYTE_NONE,
        Buffer.alloc(0),
      ),
    ).toThrow(GsrIncompatibleDataError);
  });
});

describe("wire/wire-format decodeWireHeader", () => {
  it("round-trips inputs: decode(encode(x)) reproduces uuid, compression byte, and payload", () => {
    const payload = Buffer.from("round-trip payload", "utf8");
    const encoded = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_NONE, payload);

    const decoded = decodeWireHeader(encoded);
    expect(decoded.schemaVersionId).toBe(GOLDEN_UUID);
    expect(decoded.compressionByte).toBe(COMPRESSION_BYTE_NONE);
    expect(decoded.payload.equals(payload)).toBe(true);
  });

  it("round-trips a ZLIB-flagged buffer without touching the payload bytes", () => {
    const payload = Buffer.from([0x78, 0x9c, 0x00, 0xde, 0xad]);
    const encoded = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_ZLIB, payload);

    const decoded = decodeWireHeader(encoded);
    expect(decoded.schemaVersionId).toBe(GOLDEN_UUID);
    expect(decoded.compressionByte).toBe(COMPRESSION_BYTE_ZLIB);
    expect(decoded.payload.equals(payload)).toBe(true);
  });

  it("accepts a header-only buffer with no payload", () => {
    const encoded = encodeWireHeader(GOLDEN_UUID, COMPRESSION_BYTE_NONE, Buffer.alloc(0));
    expect(encoded).toHaveLength(WIRE_FORMAT_HEADER_SIZE);

    const decoded = decodeWireHeader(encoded);
    expect(decoded.payload).toHaveLength(0);
  });

  it("reads the UUID bytes MSB-then-LSB big-endian from a hand-built buffer", () => {
    const manual = Buffer.concat([
      Buffer.from([0x03, 0x00]),
      ENDIAN_UUID_BYTES,
      Buffer.from("payload", "utf8"),
    ]);

    const decoded = decodeWireHeader(manual);
    expect(decoded.schemaVersionId).toBe(ENDIAN_UUID);
    expect(decoded.compressionByte).toBe(0x00);
    expect(decoded.payload.equals(Buffer.from("payload", "utf8"))).toBe(true);
  });

  it("throws GsrIncompatibleDataError for data shorter than the 18-byte header", () => {
    const short = Buffer.alloc(WIRE_FORMAT_HEADER_SIZE - 1);
    short[0] = 0x03;
    expect(() => decodeWireHeader(short)).toThrow(GsrIncompatibleDataError);
    expect(() => decodeWireHeader(Buffer.alloc(0))).toThrow(GsrIncompatibleDataError);
  });

  it("throws GsrIncompatibleDataError for a version byte other than 0x03", () => {
    const buf = Buffer.alloc(WIRE_FORMAT_HEADER_SIZE + 1);
    buf[0] = 0x02;
    buf[1] = COMPRESSION_BYTE_NONE;
    expect(() => decodeWireHeader(buf)).toThrow(GsrIncompatibleDataError);
  });

  it("throws GsrIncompatibleDataError for a compression byte outside {0x00, 0x05}", () => {
    const buf = Buffer.alloc(WIRE_FORMAT_HEADER_SIZE + 1);
    buf[0] = 0x03;
    buf[1] = 0x01; // boolean-shaped value, deliberately rejected
    expect(() => decodeWireHeader(buf)).toThrow(GsrIncompatibleDataError);

    buf[1] = 0xff;
    expect(() => decodeWireHeader(buf)).toThrow(GsrIncompatibleDataError);
  });
});
