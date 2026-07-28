/**
 * Error types thrown by the wire-format core.
 *
 * Mirrors the parity error surface from the Java canonical
 * (`GlueSchemaRegistryIncompatibleDataException`, etc.) and Go
 * reference (`core/errors.go`). These are the minimal error classes
 * needed by the wire-format primitives; full negative-path breadth is
 * carried in later work.
 */

/**
 * Thrown when the wire bytes cannot be interpreted as a valid
 * schema-registry message: buffer too short, unknown version byte,
 * unknown compression byte, or a malformed protobuf message-index varint.
 */
export class GsrIncompatibleDataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GsrIncompatibleDataError";
  }
}

/**
 * Thrown when a protobuf message full-name lookup does not resolve to
 * an index within the schema's sorted message set.
 */
export class GsrMessageTypeNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GsrMessageTypeNotFoundError";
  }
}
