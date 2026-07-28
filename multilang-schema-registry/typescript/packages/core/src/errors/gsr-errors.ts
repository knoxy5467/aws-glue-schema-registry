/**
 * Glue-integration error taxonomy for the `@gsr/core` control-plane surface.
 *
 * Mirrors the Glue-integration slice of the Go reference (`core/errors.go`,
 * `core/encoder.go`) onto TypeScript. `GsrError` is the umbrella base
 * (anchored to Java `AWSSchemaRegistryException` / Go `ErrGSR`); every
 * class in this module extends it. The wire-format errors in `wire/errors.ts`
 * are intentionally NOT retrofitted onto this base — that hierarchy is
 * frozen, and unifying it is a tracked, deferred parity refinement.
 *
 * `classifyGlueError` gives the registrar a discriminant over the two Glue
 * typed exceptions it needs to branch on. Grounded in Go's `errors.As`
 * checks against `*EntityNotFoundException` and `*AlreadyExistsException`
 * on the register-fall-through path.
 */

/**
 * Umbrella base for every GSR-originated failure raised outside the frozen
 * wire path.
 */
export class GsrError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "GsrError";
  }
}

/**
 * Thrown when the encoder hits an unknown schema and
 * `schemaAutoRegistrationEnabled` is false. Anchored to Go
 * `ErrSchemaAutoRegistrationDisabled`.
 */
export class GsrAutoRegistrationDisabledError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrAutoRegistrationDisabledError";
  }
}

/**
 * Config-validation error for an out-of-set `compression` value. Anchored
 * to Go's compression-type validation sentinel in `core/config.go`.
 */
export class GsrInvalidCompressionTypeError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidCompressionTypeError";
  }
}

/**
 * Config-validation error for a `compatibility` value outside the case-exact
 * 8-mode set (`NONE, DISABLED, BACKWARD, BACKWARD_ALL, FORWARD, FORWARD_ALL,
 * FULL, FULL_ALL`). Anchored to Go's `validateCompatibility`.
 */
export class GsrInvalidCompatibilityError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidCompatibilityError";
  }
}

/**
 * Config-validation error for a non-integer `timeToLiveMillis`.
 */
export class GsrInvalidCacheTtlError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidCacheTtlError";
  }
}

/**
 * Config-validation error for a non-integer `cacheSize`.
 */
export class GsrInvalidCacheSizeError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidCacheSizeError";
  }
}

/**
 * Config-validation error for an `avroRecordType` outside
 * `{GENERIC_RECORD, SPECIFIC_RECORD}`. Anchored to Go's
 * `validateAvroRecordType`.
 */
export class GsrInvalidAvroRecordTypeError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidAvroRecordTypeError";
  }
}

/**
 * Config-validation error for a `protobufMessageType` outside
 * `{POJO, DYNAMIC_MESSAGE}`. Anchored to Go's `validateProtobufMessageType`.
 */
export class GsrInvalidProtobufMessageTypeError extends GsrError {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidProtobufMessageTypeError";
  }
}

/**
 * Registration / poll-path failure wrapping an underlying Glue SDK error, a
 * poll timeout, or an unexpected schema-version status. Preserves the
 * original error on the standard `.cause` property (Node native, ES2022).
 * Anchored to Go's `SerializationError` wrapping `ErrGSR` on the register
 * path and the poll-loop `ErrGSR`-wrapped returns (`encoder.go`
 * `waitForSchemaEvolutionCheck`).
 */
export class GsrRegistrationError extends GsrError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "GsrRegistrationError";
  }
}

/**
 * Discriminant returned by {@link classifyGlueError}. The registrar branches
 * on this to decide whether an error means "not yet registered" (fall
 * through to `CreateSchema`), "concurrent producer race" (fall back to
 * `RegisterSchemaVersion`), or anything else (propagate as
 * `GsrRegistrationError`).
 */
export type GlueErrorKind = "entity-not-found" | "already-exists" | "other";

/**
 * Classify a caught `@aws-sdk/client-glue` error for the registration flow.
 *
 * Grounded in Go `encoder.go`, which uses `errors.As` against the typed
 * `*EntityNotFoundException` and `*AlreadyExistsException` structs. AWS SDK
 * v3 for JavaScript sets `.name` on every service-error instance to the
 * exact exception name (matching the Smithy shape name), so the JS-idiomatic
 * discriminant here is a `.name` string comparison rather than an
 * `instanceof` check — this keeps the errors module independent of the SDK
 * (which is not a dependency of the errors module) while remaining faithful
 * to the same wire-level discriminant. Non-`Error` inputs (null, undefined,
 * strings, plain objects) fall through to `"other"`.
 */
export function classifyGlueError(err: unknown): GlueErrorKind {
  if (err === null || typeof err !== "object") {
    return "other";
  }
  const name = (err as { name?: unknown }).name;
  if (name === "EntityNotFoundException") {
    return "entity-not-found";
  }
  if (name === "AlreadyExistsException") {
    return "already-exists";
  }
  return "other";
}
