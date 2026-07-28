/**
 * Transport-agnostic wire codec composition.
 *
 * `encodeMessage` and `decodeMessage` compose the wire primitives in the exact
 * order the Java canonical (`SerializationDataEncoder.write` /
 * `DeserializationDataDecoder`) and the Go reference (`encoder.Encode` /
 * `decoder.Decode`) use, so callers cannot get the ordering wrong:
 *
 *   encode: (optional) prepend protobuf message-index
 *        → (optional) zlib-compress
 *        → prepend 18-byte header
 *   decode: parse 18-byte header
 *        → (if header says zlib) decompress
 *        → (if caller flags protobuf) strip message-index
 *
 * This module has no Kafka / transport dependency — the wire core stays
 * independently testable and framework-free.
 */

import {
  compressZlib,
  compressionByteForType,
  compressionTypeForByte,
  decompressZlib,
  type CompressionType,
} from "./compression.js";
import { COMPRESSION_BYTE_ZLIB } from "./constants.js";
import {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "./errors.js";
import {
  computeMessageIndex,
  prependMessageIndex,
  stripMessageIndex,
} from "./message-index.js";
import {
  decodeWireHeader,
  encodeWireHeader,
} from "./wire-format.js";

/** Protobuf-specific inputs supplied to {@link encodeMessage}. */
export interface EncodeProtobufOptions {
  /** Full `.proto` source text, parsed to resolve the message index. */
  schemaText: string;
  /** Fully-qualified message name, e.g. `"google.protobuf.StringValue"`. */
  messageFullName: string;
}

/** Parameters accepted by {@link encodeMessage}. */
export interface EncodeParams {
  /** Canonical UUID string identifying the schema version. */
  schemaVersionId: string;
  /** Format-serialized record bytes (pre-index, pre-compression). */
  payload: Buffer;
  /** Selected compression mode; caller-forced by config. */
  compressionType: CompressionType;
  /** Present only for the PROTOBUF format. */
  protobuf?: EncodeProtobufOptions;
}

/** Assembled result returned by {@link decodeMessage}. */
export interface DecodeResult {
  /** Canonical UUID string parsed from the header. */
  schemaVersionId: string;
  /** Resolved compression mode from the header's compression byte. */
  compressionType: CompressionType;
  /**
   * Decoded record payload — decompressed if the header indicated ZLIB, and
   * with the protobuf message-index stripped when the caller flagged
   * `opts.protobuf`.
   */
  payload: Buffer;
  /**
   * Parsed protobuf message-index; present only when the caller flagged
   * `opts.protobuf`.
   */
  messageIndex?: number;
}

/** Optional flags accepted by {@link decodeMessage}. */
export interface DecodeOptions {
  /**
   * Set when the payload carries a protobuf message-index varint. Non-protobuf
   * formats leave this unset and receive the raw decompressed payload.
   */
  protobuf?: boolean;
}

/**
 * Assemble a full wire-format message from a serialized record payload.
 *
 * The composition order is fixed by the reference clients: for a protobuf
 * message the caller-supplied message-index is prepended first, then the
 * payload is compressed if `compressionType === "ZLIB"`, then the 18-byte
 * header is prepended.
 */
export function encodeMessage(params: EncodeParams): Buffer {
  let payload = params.payload;

  if (params.protobuf) {
    const index = computeMessageIndex(
      params.protobuf.schemaText,
      params.protobuf.messageFullName,
    );
    payload = prependMessageIndex(payload, index);
  }

  if (params.compressionType === "ZLIB") {
    payload = compressZlib(payload);
  }

  const compressionByte = compressionByteForType(params.compressionType);
  return encodeWireHeader(params.schemaVersionId, compressionByte, payload);
}

/**
 * Parse a full wire-format message into its components. The header is decoded
 * first; if the header indicates ZLIB the payload is decompressed; if the
 * caller flags protobuf the leading varint message-index is stripped from the
 * payload and returned separately.
 *
 * The full decode pipeline (header parse → optional decompress → optional
 * message-index strip) is wrapped so any failure surfaces as one uniform
 * wire-format error type. `GsrIncompatibleDataError` and
 * `GsrMessageTypeNotFoundError` pass through unchanged; every other throw —
 * including a non-`Error` value such as pako's bare-string throw on corrupt
 * input — is wrapped as `GsrIncompatibleDataError`, preserving the original
 * throw on `cause` for diagnostics.
 */
export function decodeMessage(
  data: Buffer,
  opts?: DecodeOptions,
): DecodeResult {
  try {
    const { schemaVersionId, compressionByte, payload: headerPayload } =
      decodeWireHeader(data);

    let payload = headerPayload;
    if (compressionByte === COMPRESSION_BYTE_ZLIB) {
      payload = decompressZlib(payload);
    }

    const compressionType = compressionTypeForByte(compressionByte);

    if (opts?.protobuf) {
      const { messageIndex, payload: stripped } = stripMessageIndex(payload);
      return {
        schemaVersionId,
        compressionType,
        payload: stripped,
        messageIndex,
      };
    }

    return { schemaVersionId, compressionType, payload };
  } catch (cause) {
    if (
      cause instanceof GsrIncompatibleDataError ||
      cause instanceof GsrMessageTypeNotFoundError
    ) {
      throw cause;
    }
    const reason = cause instanceof Error ? cause.message : String(cause);
    const wrapped = new GsrIncompatibleDataError(
      `failed to decode wire-format message: ${reason}`,
    );
    wrapped.cause = cause;
    throw wrapped;
  }
}
