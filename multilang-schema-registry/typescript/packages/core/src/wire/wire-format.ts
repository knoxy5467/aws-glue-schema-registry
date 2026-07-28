/**
 * 18-byte wire-format header encode/decode.
 *
 * Layout (Java canonical `SerializationDataEncoder.write`, mirrored in Go
 * `core/wire_format.go`):
 *   byte  0:       0x03                                       (version)
 *   byte  1:       0x00 (none) | 0x05 (zlib)                  (compression)
 *   bytes 2..9:    schema-version UUID MSB, big-endian        (8 bytes)
 *   bytes 10..17:  schema-version UUID LSB, big-endian        (8 bytes)
 *   bytes 18..:    payload (already-compressed if byte 1 == 0x05)
 *
 * The MSB-then-LSB big-endian layout is byte-identical to a UUID's natural
 * network-order representation, so parsing the 16 bytes as hex groups
 * 8-4-4-4-12 reproduces the canonical UUID string form.
 *
 * The payload is transparent to this module — compression, protobuf
 * message-index prefixing, and any other transforms happen in the modules
 * that own those concerns and hand the assembled payload to this codec.
 */

import {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_ZLIB,
  SCHEMA_VERSION_ID_SIZE,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_VERSION_BYTE,
} from "./constants.js";
import { GsrIncompatibleDataError } from "./errors.js";

/** Decoded 18-byte header result. */
export interface DecodedWire {
  /** Canonical lowercase UUID string, e.g. `01020304-0506-0708-090a-0b0c0d0e0f10`. */
  schemaVersionId: string;
  /** Raw compression byte from the header (0x00 or 0x05). */
  compressionByte: number;
  /** Payload bytes; left compressed if `compressionByte === 0x05`. */
  payload: Buffer;
}

// Canonical UUID string: 8-4-4-4-12 hex groups, case-insensitive.
const UUID_STRING_REGEX =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

function uuidStringToBytes(schemaVersionId: string): Buffer {
  if (typeof schemaVersionId !== "string" || !UUID_STRING_REGEX.test(schemaVersionId)) {
    throw new GsrIncompatibleDataError(
      `schema version id is not a valid UUID: ${String(schemaVersionId)}`,
    );
  }
  const hex = schemaVersionId.replace(/-/g, "");
  const bytes = Buffer.from(hex, "hex");
  // Buffer.from(hex, "hex") silently drops non-hex chars; the regex above
  // already rejects those, so this is a belt-and-braces guard.
  if (bytes.length !== SCHEMA_VERSION_ID_SIZE) {
    throw new GsrIncompatibleDataError(
      `schema version id did not decode to ${SCHEMA_VERSION_ID_SIZE} bytes: ${schemaVersionId}`,
    );
  }
  return bytes;
}

function uuidBytesToString(data: Buffer, offset: number): string {
  const hex = data.subarray(offset, offset + SCHEMA_VERSION_ID_SIZE).toString("hex");
  return (
    hex.slice(0, 8) +
    "-" +
    hex.slice(8, 12) +
    "-" +
    hex.slice(12, 16) +
    "-" +
    hex.slice(16, 20) +
    "-" +
    hex.slice(20, 32)
  );
}

function assertKnownCompressionByte(compressionByte: number): void {
  if (
    compressionByte !== COMPRESSION_BYTE_NONE &&
    compressionByte !== COMPRESSION_BYTE_ZLIB
  ) {
    throw new GsrIncompatibleDataError(
      `unsupported compression byte 0x${compressionByte.toString(16).padStart(2, "0")}`,
    );
  }
}

/**
 * Assemble the 18-byte wire-format header followed by `payload`.
 *
 * The caller is responsible for producing `payload` in its final form —
 * this module does not compress and does not prepend a protobuf
 * message-index. `schemaVersionId` is caller-supplied so a fixed UUID can
 * be reproduced offline against golden vectors without any live registry
 * interaction.
 *
 * Throws {@link GsrIncompatibleDataError} when the compression byte is not
 * `0x00` or `0x05`, or when the UUID string is malformed.
 */
export function encodeWireHeader(
  schemaVersionId: string,
  compressionByte: number,
  payload: Buffer,
): Buffer {
  assertKnownCompressionByte(compressionByte);
  const uuidBytes = uuidStringToBytes(schemaVersionId);

  const out = Buffer.allocUnsafe(WIRE_FORMAT_HEADER_SIZE + payload.length);
  out[0] = WIRE_FORMAT_VERSION_BYTE;
  out[1] = compressionByte;
  uuidBytes.copy(out, 2);
  payload.copy(out, WIRE_FORMAT_HEADER_SIZE);
  return out;
}

/**
 * Parse the 18-byte wire-format header from `data` and return its fields
 * plus the raw payload slice (payload is left compressed if the header
 * says so; the caller decides whether to decompress).
 *
 * Throws {@link GsrIncompatibleDataError} for data shorter than 18 bytes,
 * a version byte other than `0x03`, or a compression byte not in
 * `{0x00, 0x05}`.
 */
export function decodeWireHeader(data: Buffer): DecodedWire {
  if (data.length < WIRE_FORMAT_HEADER_SIZE) {
    throw new GsrIncompatibleDataError(
      `data length ${data.length} is below minimum header size ${WIRE_FORMAT_HEADER_SIZE}`,
    );
  }

  const versionByte = data[0];
  if (versionByte !== WIRE_FORMAT_VERSION_BYTE) {
    throw new GsrIncompatibleDataError(
      `unsupported wire-format version byte 0x${(versionByte ?? 0).toString(16).padStart(2, "0")}`,
    );
  }

  const compressionByte = data[1] as number;
  assertKnownCompressionByte(compressionByte);

  const schemaVersionId = uuidBytesToString(data, 2);
  const payload = data.subarray(WIRE_FORMAT_HEADER_SIZE);

  return { schemaVersionId, compressionByte, payload };
}
