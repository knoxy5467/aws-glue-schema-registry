/**
 * Public wire-format surface for `@gsr/core`. Consumers import the primitives
 * (header codec, compression handler, message-index codec) and the composed
 * message codec from this barrel; the individual module files are internal.
 */

export {
  COMPRESSION_BYTE_NONE,
  COMPRESSION_BYTE_SIZE,
  COMPRESSION_BYTE_ZLIB,
  HEADER_VERSION_BYTE_SIZE,
  SCHEMA_VERSION_ID_SIZE,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_VERSION_BYTE,
} from "./constants.js";

export {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "./errors.js";

export {
  decodeWireHeader,
  encodeWireHeader,
  type DecodedWire,
} from "./wire-format.js";

export {
  compressZlib,
  compressionByteForType,
  compressionTypeForByte,
  decompressZlib,
  type CompressionType,
} from "./compression.js";

export {
  computeMessageIndex,
  prependMessageIndex,
  stripMessageIndex,
  type StrippedIndex,
} from "./message-index.js";

export {
  decodeMessage,
  encodeMessage,
  type DecodeOptions,
  type DecodeResult,
  type EncodeParams,
  type EncodeProtobufOptions,
} from "./codec.js";
