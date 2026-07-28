import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import {
  decodeProtobuf,
  encodeProtobuf,
  protobufHazardRecords,
  WRAPPERS_PROTO_TEXT,
} from "./encode-slice.js";
import {
  GOLDEN_JAVA_DIR,
  referenceRootExists,
} from "../test-support/reference-root.js";

/**
 * The 18-byte wire header prefix in each golden `.bin`. Kept as a local
 * constant so this test file has no cross-package import — the header codec
 * lives in `@gsr/core` and is exercised by the byte-identity gate suite; the
 * slice only needs to skip the fixed-size prefix to reach the payload region.
 */
const WIRE_FORMAT_HEADER_SIZE = 18;

const STRINGVALUE_HELLO_RECORD = { value: "hello" };
const STRINGVALUE_FULL_NAME = "google.protobuf.StringValue";

/**
 * The uncompressed protobuf payload region (bytes 18..) of the NONE-compressed
 * dynamic golden vector: `06 0a 05 68 65 6c 6c 6f` — varint 0x06 (StringValue
 * lives at index 6 in the sorted `wrappers.proto` message set), then the
 * protobuf body (`tag=field 1 LEN`, len 5, `"hello"`). Kept as a literal so
 * assertions stay legible on failure.
 */
const STRINGVALUE_HELLO_PAYLOAD = Buffer.from([
  0x06, 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
]);

describe("serde/protobuf/encode-slice", () => {
  describe("protobufHazardRecords", () => {
    it("exposes the stringvalue-hello fixture record with the StringValue shape", () => {
      expect(protobufHazardRecords).toHaveLength(1);
      const entry = protobufHazardRecords[0];
      expect(entry).toBeDefined();
      expect(entry?.tag).toBe("stringvalue-hello");
      expect(entry?.messageFullName).toBe(STRINGVALUE_FULL_NAME);
      expect(entry?.record).toEqual(STRINGVALUE_HELLO_RECORD);
      expect(entry?.protoSchemaText).toBe(WRAPPERS_PROTO_TEXT);
    });
  });

  describe("encodeProtobuf", () => {
    it.skipIf(!referenceRootExists)("produces the payload region of the NONE-compressed dynamic Java golden vector", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "protobuf__dynamic__comp-NONE__stringvalue-hello.bin",
      );
      const golden = readFileSync(goldenFile);
      const goldenPayload = golden.subarray(WIRE_FORMAT_HEADER_SIZE);

      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      expect(encoded.equals(goldenPayload)).toBe(true);
    });

    it("matches the recorded byte literal — varint 0x06 then StringValue body for {value:'hello'}", () => {
      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      expect(encoded.equals(STRINGVALUE_HELLO_PAYLOAD)).toBe(true);
    });

    it.skipIf(!referenceRootExists)("also matches the CONCRETE record-type golden payload (equal SHA-256)", () => {
      // PROVENANCE.md records dynamic and concrete stringvalue-hello cells
      // with identical SHA-256s, so one dynamic encode covers both golden
      // record-type cells.
      const concreteGolden = readFileSync(
        resolve(
          GOLDEN_JAVA_DIR,
          "protobuf__concrete__comp-NONE__stringvalue-hello.bin",
        ),
      ).subarray(WIRE_FORMAT_HEADER_SIZE);

      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );

      expect(encoded.equals(concreteGolden)).toBe(true);
    });

    it("prefixes the varint message-index (6) before the protobuf body", () => {
      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      // First byte is the unsigned varint for index 6 — a 1-byte varint 0x06.
      expect(encoded[0]).toBe(0x06);
      // Followed immediately by the tag-1-LEN prefix of the message body.
      expect(encoded[1]).toBe(0x0a);
    });

    it("accepts a leading-dot fully-qualified message name", () => {
      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        ".google.protobuf.StringValue",
        STRINGVALUE_HELLO_RECORD,
      );
      expect(encoded.equals(STRINGVALUE_HELLO_PAYLOAD)).toBe(true);
    });
  });

  describe("decodeProtobuf", () => {
    it.skipIf(!referenceRootExists)("reconstructs {value:'hello'} from the golden payload region", () => {
      const goldenFile = resolve(
        GOLDEN_JAVA_DIR,
        "protobuf__dynamic__comp-NONE__stringvalue-hello.bin",
      );
      const goldenPayload = readFileSync(goldenFile).subarray(
        WIRE_FORMAT_HEADER_SIZE,
      );

      const decoded = decodeProtobuf(
        goldenPayload,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
      );

      expect(decoded).toEqual(STRINGVALUE_HELLO_RECORD);
    });

    it("round-trips encodeProtobuf output back to the original record", () => {
      const encoded = encodeProtobuf(
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
        STRINGVALUE_HELLO_RECORD,
      );
      const decoded = decodeProtobuf(
        encoded,
        WRAPPERS_PROTO_TEXT,
        STRINGVALUE_FULL_NAME,
      );

      expect(decoded).toEqual(STRINGVALUE_HELLO_RECORD);
    });

    it("throws when the payload is too short to hold a message-index varint", () => {
      const emptyPayload = Buffer.alloc(0);
      expect(() =>
        decodeProtobuf(emptyPayload, WRAPPERS_PROTO_TEXT, STRINGVALUE_FULL_NAME),
      ).toThrow(/payload too short/);
    });

    it("throws when the varint is missing its continuation terminator (all bytes have high bit set)", () => {
      // Two bytes, each with high bit set — never signals termination but
      // does not exceed the 5-byte cap. Exercises the missing-terminator
      // branch specifically.
      const truncatedVarint = Buffer.from([0x81, 0x82]);
      expect(() =>
        decodeProtobuf(
          truncatedVarint,
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
        ),
      ).toThrow(/missing continuation terminator/);
    });

    it("throws when the varint exceeds 5 bytes (overflow guard)", () => {
      // Six bytes all with high bit set — walks past the 5-byte cap without
      // ever terminating. Exercises the overflow branch specifically.
      const overflowVarint = Buffer.from([0x81, 0x82, 0x83, 0x84, 0x85, 0x86]);
      expect(() =>
        decodeProtobuf(
          overflowVarint,
          WRAPPERS_PROTO_TEXT,
          STRINGVALUE_FULL_NAME,
        ),
      ).toThrow(/exceeds 5 bytes/);
    });

    it("round-trips a multi-byte varint index (exercises the varint-encode continuation loop)", () => {
      // A schema with 130 lex-sorted messages puts the target at index 128,
      // which requires a two-byte varint (0x80 0x01). This exercises the
      // continuation-byte encode path in encodeUnsignedVarint32 (value >=
      // 0x80) and the corresponding continuation decode branch.
      const messageCount = 130;
      const targetIndex = 128;
      const names = Array.from({ length: messageCount }, (_, i) =>
        // Zero-pad so lexicographic sort matches numeric order.
        `M${String(i).padStart(3, "0")}`,
      );
      const schemaText =
        'syntax = "proto3";\npackage test;\n' +
        names.map((n) => `message ${n} { string value = 1; }`).join("\n") +
        "\n";
      const targetName = `test.${names[targetIndex]}`;
      const record = { value: "hello" };

      const encoded = encodeProtobuf(schemaText, targetName, record);

      // First varint byte is the low 7 bits with the continuation flag,
      // followed by the high byte terminating the varint (index 128 → 0x80 0x01).
      expect(encoded[0]).toBe(0x80);
      expect(encoded[1]).toBe(0x01);

      const decoded = decodeProtobuf(encoded, schemaText, targetName);
      expect(decoded).toEqual(record);
    });
  });

  describe("encodeProtobuf error paths", () => {
    it("throws when the message name is not present in the schema", () => {
      // Unknown message name surfaces as an error from the protobufjs lookup
      // before the BFS index computation runs. The exact message text comes
      // from protobufjs; here we just assert the failure surfaces (rather
      // than silently returning an invalid payload) so callers see the miss.
      const schemaText = 'syntax = "proto3";\npackage test;\nmessage Only { string value = 1; }';
      expect(() =>
        encodeProtobuf(schemaText, "test.Missing", { value: "hi" }),
      ).toThrow(/test\.Missing/);
    });

    it("finds messages nested inside another message (BFS traversal covers nestedArray)", () => {
      // Two top-level messages, one with a nested type — the BFS walker
      // must enqueue the nested Inner and include it in the lex-sorted
      // candidate list. `test.Outer.Inner` sorts after `test.Outer` and
      // `test.Sibling`, so its computed index is > 0 and encoding proves
      // the nested type was reachable.
      const schemaText = `
syntax = "proto3";
package test;

message Sibling { string a = 1; }
message Outer {
  message Inner { string a = 1; }
  string b = 1;
}
`;
      const encoded = encodeProtobuf(schemaText, "test.Outer.Inner", {
        a: "x",
      });
      const decoded = decodeProtobuf(encoded, schemaText, "test.Outer.Inner");
      expect(decoded).toEqual({ a: "x" });
    });
  });
});
