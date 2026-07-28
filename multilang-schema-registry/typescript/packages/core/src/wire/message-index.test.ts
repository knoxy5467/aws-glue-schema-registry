import { describe, expect, it } from "vitest";
import {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "./errors.js";
import {
  computeMessageIndex,
  prependMessageIndex,
  stripMessageIndex,
} from "./message-index.js";

/**
 * The relevant portion of `google/protobuf/wrappers.proto`. The full file is
 * shipped by every protoc distribution — the schema below is byte-for-byte
 * the message declarations in that file (comments and options elided so this
 * test stays independent of any external `.proto` fixture).
 *
 * The sorted-lex order of the nine wrapper messages places `StringValue` at
 * index 6 — this is the canonical example the Java reference uses.
 */
const WRAPPERS_PROTO = `
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
 * The synthetic example from the Java MessageIndexFinder reference:
 * `message B { message C {} message A { message D {} } }` sorted-lex yields
 * `B=0, B.A=1, B.A.D=2, B.C=3`.
 */
const SYNTHETIC_PROTO = `
syntax = "proto3";
message B {
  message C {}
  message A {
    message D {}
  }
}
`;

describe("wire/message-index", () => {
  describe("computeMessageIndex", () => {
    it("returns 6 for google.protobuf.StringValue in wrappers.proto", () => {
      expect(
        computeMessageIndex(WRAPPERS_PROTO, "google.protobuf.StringValue"),
      ).toBe(6);
    });

    it("also accepts a leading-dot fully-qualified name", () => {
      expect(
        computeMessageIndex(WRAPPERS_PROTO, ".google.protobuf.StringValue"),
      ).toBe(6);
    });

    it.each([
      ["google.protobuf.BoolValue", 0],
      ["google.protobuf.BytesValue", 1],
      ["google.protobuf.DoubleValue", 2],
      ["google.protobuf.FloatValue", 3],
      ["google.protobuf.Int32Value", 4],
      ["google.protobuf.Int64Value", 5],
      ["google.protobuf.StringValue", 6],
      ["google.protobuf.UInt32Value", 7],
      ["google.protobuf.UInt64Value", 8],
    ])("returns the sorted-lex index for wrappers.proto: %s → %i", (name, expected) => {
      expect(computeMessageIndex(WRAPPERS_PROTO, name)).toBe(expected);
    });

    it("reproduces the Java MessageIndexFinder example: B=0, B.A=1, B.A.D=2, B.C=3", () => {
      expect(computeMessageIndex(SYNTHETIC_PROTO, "B")).toBe(0);
      expect(computeMessageIndex(SYNTHETIC_PROTO, "B.A")).toBe(1);
      expect(computeMessageIndex(SYNTHETIC_PROTO, "B.A.D")).toBe(2);
      expect(computeMessageIndex(SYNTHETIC_PROTO, "B.C")).toBe(3);
    });

    it("throws GsrMessageTypeNotFoundError for a name absent from the schema", () => {
      expect(() =>
        computeMessageIndex(WRAPPERS_PROTO, "google.protobuf.NotAMessage"),
      ).toThrow(GsrMessageTypeNotFoundError);
    });

    it("includes the target name in the thrown error message", () => {
      let caught: unknown;
      try {
        computeMessageIndex(WRAPPERS_PROTO, "google.protobuf.NotAMessage");
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(GsrMessageTypeNotFoundError);
      expect((caught as Error).message).toContain(
        "google.protobuf.NotAMessage",
      );
    });
  });

  describe("prependMessageIndex + stripMessageIndex", () => {
    const payload = Buffer.from([0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f]);

    it("round-trips index 0 (1-byte varint 0x00)", () => {
      const prefixed = prependMessageIndex(payload, 0);
      expect(prefixed[0]).toBe(0x00);
      expect(prefixed.length).toBe(payload.length + 1);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(0);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("round-trips index 6 (1-byte varint 0x06) — the StringValue case", () => {
      const prefixed = prependMessageIndex(payload, 6);
      expect(prefixed[0]).toBe(0x06);
      expect(prefixed.length).toBe(payload.length + 1);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(6);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("round-trips index 127 — the largest 1-byte varint value", () => {
      const prefixed = prependMessageIndex(payload, 127);
      expect(prefixed[0]).toBe(0x7f);
      expect(prefixed.length).toBe(payload.length + 1);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(127);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("round-trips index 128 — the smallest 2-byte varint value", () => {
      // 128 → 0x80 0x01 (continuation bit set on first byte, second byte holds high bits)
      const prefixed = prependMessageIndex(payload, 128);
      expect(prefixed[0]).toBe(0x80);
      expect(prefixed[1]).toBe(0x01);
      expect(prefixed.length).toBe(payload.length + 2);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(128);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("round-trips index 16383 — the largest 2-byte varint value", () => {
      // 16383 → 0xff 0x7f
      const prefixed = prependMessageIndex(payload, 16383);
      expect(prefixed[0]).toBe(0xff);
      expect(prefixed[1]).toBe(0x7f);
      expect(prefixed.length).toBe(payload.length + 2);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(16383);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("round-trips index 300 (2-byte varint 0xac 0x02)", () => {
      const prefixed = prependMessageIndex(payload, 300);
      expect(prefixed[0]).toBe(0xac);
      expect(prefixed[1]).toBe(0x02);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(300);
      expect(stripped.equals(payload)).toBe(true);
    });

    it("preserves an empty payload across the round trip", () => {
      const empty = Buffer.alloc(0);
      const prefixed = prependMessageIndex(empty, 42);
      const { messageIndex, payload: stripped } = stripMessageIndex(prefixed);
      expect(messageIndex).toBe(42);
      expect(stripped.length).toBe(0);
    });

    it("rejects negative indices", () => {
      expect(() => prependMessageIndex(payload, -1)).toThrow(
        GsrIncompatibleDataError,
      );
    });

    it("rejects non-integer indices", () => {
      expect(() => prependMessageIndex(payload, 1.5)).toThrow(
        GsrIncompatibleDataError,
      );
    });
  });

  describe("stripMessageIndex — error paths", () => {
    it("throws GsrIncompatibleDataError on empty input", () => {
      expect(() => stripMessageIndex(Buffer.alloc(0))).toThrow(
        GsrIncompatibleDataError,
      );
    });

    it("throws GsrIncompatibleDataError when the varint exceeds 5 bytes", () => {
      // Six bytes all with the continuation bit set → never terminates within
      // the 5-byte varint32 budget.
      const bad = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
      let caught: unknown;
      try {
        stripMessageIndex(bad);
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      expect((caught as Error).message).toContain("5 bytes");
    });

    it("throws GsrIncompatibleDataError when the continuation bit runs off the end", () => {
      // A single byte with the continuation bit set and no follow-up.
      const bad = Buffer.from([0x80]);
      let caught: unknown;
      try {
        stripMessageIndex(bad);
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(GsrIncompatibleDataError);
      expect((caught as Error).message).toContain("continuation");
    });
  });
});
