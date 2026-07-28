/**
 * Zlib compression handler and compression-mode-selection helpers for the
 * AWS Glue Schema Registry wire protocol.
 *
 * The Java canonical (`SerializationDataEncoder.compressData` /
 * `DeserializationDataDecoder.decompressData`) uses a bare
 * `java.util.zip.Deflater()` on the whole payload with default settings —
 * level 6, default strategy, window bits 15, memLevel 8, no dictionary — and
 * the resulting bytes always begin with the zlib header `78 9c`.
 *
 * We deflate through pako (a pure-JS port of stock zlib), NOT through Node's
 * bundled `zlib.deflateSync`. Node ships a Chromium fork of zlib whose
 * deflate output diverges from stock zlib / Java `Deflater` on payloads
 * larger than a handful of bytes — the produced bytes are valid DEFLATE
 * (they decompress to the same plaintext) but they are NOT byte-identical
 * to the Java canonical, which breaks the wire-format byte-identity
 * conformance guarantee. pako 1.x tracks stock zlib bit-for-bit, matching
 * both the Java Deflater and Python's `zlib.compress` output. This module
 * MUST NOT emulate the Go reference's sync-flush framing
 * (`04 00 00 ff ff` / `00 00 ff ff`), which diverges from Java bytes.
 *
 * Mode selection is caller-forced by config, mirroring Java and Go: `NONE`
 * skips compression and emits the `0x00` header byte; `ZLIB` always compresses
 * and emits `0x05`. There is no adaptive below-threshold heuristic.
 */

import pako from "pako";

import {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_ZLIB,
} from "./constants.js";
import { GsrIncompatibleDataError } from "./errors.js";

/**
 * The compression modes supported on the wire. `NONE` leaves the payload
 * as-is; `ZLIB` deflates the payload with default zlib parameters.
 */
export type CompressionType = "NONE" | "ZLIB";

/**
 * Compress `data` with pako deflate at level 6 — the stock-zlib defaults
 * that reproduce Java `new Deflater()` bytes: level 6, default strategy,
 * window bits 15, memLevel 8, no dictionary. The output begins with the
 * zlib header `78 9c` and never carries sync-flush framing.
 */
export function compressZlib(data: Buffer): Buffer {
  return Buffer.from(pako.deflate(data, { level: 6 }));
}

/**
 * Decompress `data` produced by `compressZlib` (or by the Java / Go
 * reference clients) with pako inflate. Kept on pako (not Node zlib) so
 * both directions run on the same pure-JS engine and share test coverage.
 *
 * Any throw from `pako.inflate` — including pako 1.x's bare-string throw
 * on corrupt ZLIB input ("invalid block type", "unknown compression
 * method", "buffer error", etc.) — is wrapped in a
 * `GsrIncompatibleDataError` so the wire-format error taxonomy stays
 * uniform. The original throw is preserved on `cause` for diagnostics.
 */
export function decompressZlib(data: Buffer): Buffer {
  try {
    return Buffer.from(pako.inflate(data));
  } catch (cause) {
    const reason =
      cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to decompress zlib payload: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }
}

/**
 * Map a caller-selected compression mode to the byte written into the
 * wire-format header. `NONE` → `0x00`, `ZLIB` → `0x05`. The result is
 * never a boolean `0`/`1`.
 */
export function compressionByteForType(type: CompressionType): number {
  switch (type) {
    case "NONE":
      return COMPRESSION_BYTE_NONE;
    case "ZLIB":
      return COMPRESSION_BYTE_ZLIB;
    default: {
      // Exhaustiveness guard for TS callers that widen the type at runtime.
      const unknown: never = type;
      throw new GsrIncompatibleDataError(
        `unknown compression type: ${String(unknown)}`,
      );
    }
  }
}

/**
 * Map the compression byte read from the wire-format header back to a
 * compression mode. Throws `GsrIncompatibleDataError` for any byte outside
 * the supported set `{0x00, 0x05}`.
 */
export function compressionTypeForByte(byte: number): CompressionType {
  if (byte === COMPRESSION_BYTE_NONE) {
    return "NONE";
  }
  if (byte === COMPRESSION_BYTE_ZLIB) {
    return "ZLIB";
  }
  throw new GsrIncompatibleDataError(
    `unknown compression byte: 0x${byte.toString(16).padStart(2, "0")}`,
  );
}
