/**
 * Full-breadth Protobuf serde.
 *
 * `serializeProtobufBody` and `deserializeProtobufBody` accept an arbitrary
 * `.proto` schema (parsed via `protobufjs.parse`) and a fully-qualified
 * message name, and encode / decode the corresponding message body only.
 *
 * The wire message-index varint is deliberately NOT part of the surface: it
 * is a `@gsr/core` concern (pre-compression, in-payload) and is prepended /
 * stripped by `@gsr/core.encodeMessage` / `decodeMessage` when the caller
 * passes the protobuf schema and message name. This serde produces and
 * consumes exactly the bytes that sit after the message-index — the
 * `protobufjs`-encoded message body — so the composition
 *
 *     encodeMessage({
 *       schemaVersionId,
 *       payload: serializeProtobufBody(schemaText, messageFullName, record),
 *       compressionType,
 *       protobuf: { schemaText, messageFullName },
 *     })
 *
 * reproduces the Java canonical wire bytes: the core prepends the message
 * index it computes via BFS+lex, deflates when requested, and prepends the
 * 18-byte header.
 *
 * Two decode shapes are supported via {@link ProtobufSerdeOptions.messageType}:
 *
 *   - `DYNAMIC` (default) — the decoded record is materialized to a plain
 *     JS object via `Type.toObject` with defaults off. Mirrors the Java
 *     `DYNAMIC_MESSAGE` record type and Go `ProtobufMessageType.DYNAMIC_MESSAGE`:
 *     the caller sees a plain `{ ... }` shape.
 *   - `POJO` — the decoded record is the concrete `protobufjs` message
 *     instance returned by `Type.decode`, without the plain-object
 *     conversion. Mirrors the Java `POJO` record type and Go
 *     `ProtobufMessageType.POJO`: the caller sees a typed message instance
 *     — `instance instanceof Type.ctor` holds and field access goes through
 *     the type's accessors.
 *
 * Every library throw is wrapped into the wire-format error taxonomy so no
 * bare `protobufjs` error or bare string escapes the public decode surface:
 *
 *   - An unknown `messageFullName` (missing from the parsed schema) throws
 *     `GsrMessageTypeNotFoundError`.
 *   - Any other failure (parse error on `schemaText`, truncated / malformed
 *     body, verification failure) throws `GsrIncompatibleDataError`,
 *     preserving the original throw on `cause`.
 */

import protobuf from "protobufjs";

import {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "@gsr/core";

/** Decode return-shape selector. */
export type ProtobufMessageType = "DYNAMIC" | "POJO";

/** Optional flags accepted by {@link deserializeProtobufBody}. */
export interface ProtobufSerdeOptions {
  /**
   * Selects the decode return shape. Defaults to `"DYNAMIC"` — a plain JS
   * object via `Type.toObject`. `"POJO"` returns the concrete `protobufjs`
   * message instance (mirrors Go `ProtobufMessageType.POJO`).
   */
  messageType?: ProtobufMessageType;
}

/**
 * Encode a message record as the raw `protobufjs` body — the bytes that sit
 * AFTER the message-index varint in the wire payload region.
 *
 * The message-index is intentionally NOT prepended; the caller composes this
 * body with the core codec (`encodeMessage(..., { protobuf: {...} })`), which
 * prepends the index pre-compression and assembles the header.
 *
 * Both plain objects and existing `protobufjs` message instances are accepted
 * as `message`: plain objects flow through `Type.fromObject` so nested
 * message / enum / bytes handling matches the library's canonical path;
 * message instances are encoded directly.
 *
 * @throws {GsrMessageTypeNotFoundError} If `messageFullName` is absent from
 *   the parsed `schemaText`.
 * @throws {GsrIncompatibleDataError} If `schemaText` fails to parse or the
 *   record fails `protobufjs` verification.
 */
export function serializeProtobufBody(
  schemaText: string,
  messageFullName: string,
  message: unknown,
): Buffer {
  const target = stripLeadingDot(messageFullName);
  const type = lookupMessageType(schemaText, target);

  try {
    const record = normalizeInputRecord(type, message);
    return Buffer.from(type.encode(record).finish());
  } catch (cause) {
    throw wrapAsIncompatibleData(
      `failed to encode protobuf body for "${target}"`,
      cause,
    );
  }
}

/**
 * Decode a raw `protobufjs` message body back into a record. `body` is the
 * post-index, post-decompression payload region — the caller-side
 * responsibility of stripping the message-index and inflating ZLIB payloads
 * lives in `@gsr/core.decodeMessage`.
 *
 * Selects the decode return shape via `opts.messageType`; see the module
 * docstring for the DYNAMIC / POJO contract.
 *
 * @throws {GsrMessageTypeNotFoundError} If `messageFullName` is absent from
 *   the parsed `schemaText`.
 * @throws {GsrIncompatibleDataError} If `schemaText` fails to parse or the
 *   body is truncated / malformed.
 */
export function deserializeProtobufBody(
  body: Buffer,
  schemaText: string,
  messageFullName: string,
  opts?: ProtobufSerdeOptions,
): unknown {
  const target = stripLeadingDot(messageFullName);
  const type = lookupMessageType(schemaText, target);
  const messageType: ProtobufMessageType = opts?.messageType ?? "DYNAMIC";

  let decoded: protobuf.Message<object>;
  try {
    decoded = type.decode(body);
  } catch (cause) {
    throw wrapAsIncompatibleData(
      `failed to decode protobuf body for "${target}"`,
      cause,
    );
  }

  if (messageType === "POJO") {
    return decoded;
  }
  return type.toObject(decoded, { defaults: false });
}

/**
 * Parse `schemaText` and resolve `target` to a `protobufjs.Type`. Parse
 * failures surface as `GsrIncompatibleDataError`; a name miss surfaces as
 * `GsrMessageTypeNotFoundError` (mirrors the core codec's error taxonomy).
 */
function lookupMessageType(schemaText: string, target: string): protobuf.Type {
  let root: protobuf.Root;
  try {
    root = protobuf.parse(schemaText, { keepCase: true }).root;
  } catch (cause) {
    throw wrapAsIncompatibleData(
      `failed to parse protobuf schema for "${target}"`,
      cause,
    );
  }

  const looked = root.lookup(target);
  if (!(looked instanceof protobuf.Type)) {
    throw new GsrMessageTypeNotFoundError(
      `protobuf message type not found: "${target}"`,
    );
  }
  return looked;
}

/**
 * Coerce an arbitrary caller input to a `protobufjs` message ready for
 * `Type.encode`. Plain objects pass through `Type.fromObject` so nested
 * message / enum / bytes handling matches the library's canonical path;
 * existing message instances (`instance instanceof type.ctor`) are returned
 * unchanged. `Type.verify` runs first so a shape mismatch surfaces a readable
 * error at the encode boundary rather than at a distant offset inside the
 * produced wire bytes.
 */
function normalizeInputRecord(
  type: protobuf.Type,
  message: unknown,
): protobuf.Message<object> {
  if (message === null || typeof message !== "object") {
    throw new GsrIncompatibleDataError(
      `protobuf message must be an object (got ${typeof message})`,
    );
  }

  if (message instanceof type.ctor) {
    return message as protobuf.Message<object>;
  }

  const asObject = message as { [k: string]: unknown };
  const verifyError = type.verify(asObject);
  if (verifyError !== null) {
    throw new GsrIncompatibleDataError(
      `protobuf verify failed for "${type.fullName}": ${verifyError}`,
    );
  }
  return type.fromObject(asObject);
}

/**
 * Wrap an arbitrary throw as `GsrIncompatibleDataError`, preserving the
 * original throw (including a non-`Error` bare string) on `cause`. Mirrors
 * the `@gsr/core` decode-path wrap so the same failure surfaces the same
 * class from either layer. An already-`GsrIncompatibleDataError` is passed
 * through untouched to avoid double-wrapping.
 */
function wrapAsIncompatibleData(
  prefix: string,
  cause: unknown,
): GsrIncompatibleDataError {
  if (cause instanceof GsrIncompatibleDataError) {
    return cause;
  }
  const reason = cause instanceof Error ? cause.message : String(cause);
  const wrapped = new GsrIncompatibleDataError(`${prefix}: ${reason}`);
  wrapped.cause = cause;
  return wrapped;
}

function stripLeadingDot(name: string): string {
  return name.startsWith(".") ? name.slice(1) : name;
}
