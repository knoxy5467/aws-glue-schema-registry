import { describe, expect, it } from "vitest";
import {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_SIZE,
  COMPRESSION_BYTE_ZLIB,
  HEADER_VERSION_BYTE_SIZE,
  SCHEMA_VERSION_ID_SIZE,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_VERSION_BYTE,
} from "./constants.js";

/**
 * The constants module has no logic — these tests exist to pin the exact
 * numeric values against drift from the Java canonical and Go reference.
 */
describe("wire/constants", () => {
  it("uses 0x03 as the wire-format version byte", () => {
    expect(WIRE_FORMAT_VERSION_BYTE).toBe(0x03);
  });

  it("uses 0x00 for the NONE compression byte (not a boolean)", () => {
    expect(COMPRESSION_BYTE_NONE).toBe(0x00);
  });

  it("uses 0x05 for the ZLIB compression byte (not a boolean)", () => {
    expect(COMPRESSION_BYTE_ZLIB).toBe(0x05);
  });

  it("reports the header version byte as one byte", () => {
    expect(HEADER_VERSION_BYTE_SIZE).toBe(1);
  });

  it("reports the compression byte as one byte", () => {
    expect(COMPRESSION_BYTE_SIZE).toBe(1);
  });

  it("reports the schema-version UUID as 16 bytes", () => {
    expect(SCHEMA_VERSION_ID_SIZE).toBe(16);
  });

  it("computes the total header size as version + compression + UUID (18 bytes)", () => {
    expect(WIRE_FORMAT_HEADER_SIZE).toBe(18);
    expect(WIRE_FORMAT_HEADER_SIZE).toBe(
      HEADER_VERSION_BYTE_SIZE + COMPRESSION_BYTE_SIZE + SCHEMA_VERSION_ID_SIZE,
    );
  });

  it("keeps the two compression bytes distinct", () => {
    expect(COMPRESSION_BYTE_NONE).not.toBe(COMPRESSION_BYTE_ZLIB);
  });
});
