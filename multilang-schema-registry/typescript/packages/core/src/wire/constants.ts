/**
 * Wire-format constants for the AWS Glue Schema Registry wire protocol.
 *
 * Values are anchored to the Java canonical (`AWSSchemaRegistryConstants`)
 * and mirrored in the Go reference (`core/wire_format.go`). The header layout
 * is: [version byte] [compression byte] [16-byte schema-version UUID] = 18 bytes.
 */

/** Wire-format version byte written as byte 0 of every message. */
export const WIRE_FORMAT_VERSION_BYTE = 0x03;

/** Compression byte indicating an uncompressed payload. */
export const COMPRESSION_BYTE_NONE = 0x00;

/** Compression byte indicating a zlib-deflated payload. */
export const COMPRESSION_BYTE_ZLIB = 0x05;

/** Size in bytes of the header version byte. */
export const HEADER_VERSION_BYTE_SIZE = 1;

/** Size in bytes of the compression byte. */
export const COMPRESSION_BYTE_SIZE = 1;

/** Size in bytes of the schema-version UUID carried in the header. */
export const SCHEMA_VERSION_ID_SIZE = 16;

/** Total wire-format header size: version (1) + compression (1) + UUID (16). */
export const WIRE_FORMAT_HEADER_SIZE = 18;
