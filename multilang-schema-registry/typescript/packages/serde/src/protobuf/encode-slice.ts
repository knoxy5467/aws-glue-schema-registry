/**
 * Minimal Protobuf encode/decode slice.
 *
 * The slice's sole job is to reproduce the Java-canonical Protobuf payload
 * bytes (the region that follows the 18-byte wire header) for
 * `StringValue{value:"hello"}` from `google/protobuf/wrappers.proto`, and to
 * decode that same payload back to the canonical record. The payload region
 * is `messageIndex-varint || protobuf-body` — the message-index sits inside
 * the payload (pre-compression), never in the outer wire header.
 *
 * Full Protobuf breadth — concrete generated-class dispatch, base64
 * `FileDescriptorProto` schema input, nested message registries — is out of
 * scope for this slice.
 *
 * The wire header, compression, and codec composition live in `@gsr/core`;
 * this module deliberately produces only the format-specific payload bytes
 * so the gate test can compose them with the core codec and compare against
 * every golden cell.
 */

import protobuf from "protobufjs";

/**
 * The nine wrapper messages from `google/protobuf/wrappers.proto`, inlined so
 * this slice has exactly one runtime dependency — `protobufjs` — and needs no
 * cross-repo file access to reproduce the payload. The declarations are
 * byte-for-byte the message set in the upstream `wrappers.proto`; comments
 * and options are elided so this constant stays independent of any external
 * `.proto` fixture.
 *
 * The lexicographic order of the nine wrapper messages places
 * `google.protobuf.StringValue` at index 6 — the canonical message-index > 0
 * example the Java reference uses.
 */
export const WRAPPERS_PROTO_TEXT = `
syntax = "proto3";
package google.protobuf;

message DoubleValue { double value = 1; }
message FloatValue  { float  value = 1; }
message Int64Value  { int64  value = 1; }
message UInt64Value { uint64 value = 1; }
message Int32Value  { int32  value = 1; }
message UInt32Value { uint32 value = 1; }
message BoolValue   { bool   value = 1; }
message StringValue { string value = 1; }
message BytesValue  { bytes  value = 1; }
`;

/**
 * Canonical record exposed by the Protobuf slice. Each entry pairs a
 * golden-vector `payloadTag` with the inline `.proto` schema text, the
 * fully-qualified message name to look up, and the record the encode slice
 * serializes.
 */
export interface ProtobufHazardRecord {
  /** Matches the `payloadTag` field of the Java-golden `.json` descriptor. */
  readonly tag: string;
  /** Inline `.proto` schema text (parsed via `protobufjs`). */
  readonly protoSchemaText: string;
  /** Fully-qualified message name, e.g. `"google.protobuf.StringValue"`. */
  readonly messageFullName: string;
  /** The canonical record the encode slice serializes. */
  readonly record: Record<string, unknown>;
}

/**
 * The slice exposes exactly one record — `StringValue{value:"hello"}` from
 * `google/protobuf/wrappers.proto` — because the Java-golden dynamic and
 * concrete cells for this tag share identical payload bytes (equal SHA-256 in
 * `PROVENANCE.md`), so a single protobufjs dynamic encode/decode pair covers
 * both record-type golden cells for the gate.
 */
export const protobufHazardRecords: readonly ProtobufHazardRecord[] = [
  {
    tag: "stringvalue-hello",
    protoSchemaText: WRAPPERS_PROTO_TEXT,
    messageFullName: "google.protobuf.StringValue",
    record: { value: "hello" },
  },
];

/** Maximum length of an unsigned varint32 in bytes. */
const MAX_VARINT32_BYTES = 5;

/**
 * Encode a record against the given `.proto` schema and return the payload
 * bytes that would sit after the 18-byte wire header:
 * `messageIndex-varint || protobuf-body`.
 *
 * The message-index is computed via BFS-then-lex-sort of fully-qualified
 * message names in the schema (Java `MessageIndexFinder` parity) and
 * prepended as an unsigned varint (not zig-zag). The message body is encoded
 * dynamically via `protobufjs` — no generated concrete class is required, so
 * new schemas plug in without codegen.
 *
 * The caller composes this payload with the wire header (and optional ZLIB
 * compression) via the `@gsr/core` codec — this function is header-agnostic
 * on purpose.
 */
export function encodeProtobuf(
  protoSchemaText: string,
  messageFullName: string,
  record: object,
): Buffer {
  const target = stripLeadingDot(messageFullName);
  const parsed = protobuf.parse(protoSchemaText, { keepCase: true });
  const type = parsed.root.lookupType(target);

  const messageIndex = computeMessageIndexFromRoot(parsed.root, target);

  const body = Buffer.from(type.encode(type.create(record)).finish());
  const varint = encodeUnsignedVarint32(messageIndex);
  return Buffer.concat([varint, body], varint.length + body.length);
}

/**
 * Decode a Protobuf payload (bytes AFTER the 18-byte wire header, already
 * decompressed if the transport was ZLIB) back into the canonical record.
 *
 * The input must be `messageIndex-varint || protobuf-body`; the varint is
 * consumed first and then the body is deserialized via `protobufjs`.
 */
export function decodeProtobuf(
  payload: Buffer,
  protoSchemaText: string,
  messageFullName: string,
): unknown {
  const target = stripLeadingDot(messageFullName);
  const parsed = protobuf.parse(protoSchemaText, { keepCase: true });
  const type = parsed.root.lookupType(target);

  const { payload: body } = readUnsignedVarint32(payload);
  const decoded = type.decode(body);
  return type.toObject(decoded, { defaults: false });
}

/**
 * BFS-then-lex-sort computation of `target`'s wire message-index within
 * `root`. Mirrors the Java `MessageIndexFinder` algorithm and the Go
 * reference `getMessageIndexFromProtoDefinition`: BFS (level-order) over
 * top-level then nested messages, collect fully-qualified names, sort
 * lexicographically, and return the target's position.
 *
 * Duplicated here (rather than importing from `@gsr/core`) so the slice
 * does not need a workspace dep on the core package — `protobufjs` is the
 * only external dependency this slice pulls in.
 */
function computeMessageIndexFromRoot(
  root: protobuf.Root,
  target: string,
): number {
  const topLevel: protobuf.Type[] = [];
  const namespaces: protobuf.NamespaceBase[] = [root];
  while (namespaces.length > 0) {
    const ns = namespaces.pop() as protobuf.NamespaceBase;
    for (const child of ns.nestedArray ?? []) {
      if (child instanceof protobuf.Type) {
        topLevel.push(child);
      } else if (child instanceof protobuf.Namespace) {
        namespaces.push(child);
      }
    }
  }

  const collected: string[] = [];
  const queue: protobuf.Type[] = [...topLevel];
  while (queue.length > 0) {
    const t = queue.shift() as protobuf.Type;
    collected.push(stripLeadingDot(t.fullName));
    for (const nested of t.nestedArray ?? []) {
      if (nested instanceof protobuf.Type) {
        queue.push(nested);
      }
    }
  }

  const sorted = collected.slice().sort();
  const index = sorted.indexOf(target);
  if (index < 0) {
    throw new Error(
      `protobuf message type not found: "${target}" (sorted candidates: ${JSON.stringify(sorted)})`,
    );
  }
  return index;
}

/** Encode a non-negative integer as an unsigned varint (not zig-zag). */
function encodeUnsignedVarint32(value: number): Buffer {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(
      `message index must be a non-negative integer (got ${value})`,
    );
  }
  const bytes: number[] = [];
  let v = value >>> 0;
  while (v >= 0x80) {
    bytes.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  bytes.push(v & 0x7f);
  return Buffer.from(bytes);
}

/**
 * Read an unsigned varint from the start of `data` and return the parsed
 * value paired with the remaining bytes. Throws on malformed input (empty,
 * exceeds 5 bytes, or missing a continuation terminator).
 */
function readUnsignedVarint32(data: Buffer): {
  messageIndex: number;
  payload: Buffer;
} {
  if (data.length < 1) {
    throw new Error(
      "protobuf payload too short to contain message-index varint",
    );
  }

  let index = 0;
  let shift = 0;
  let pos = 0;
  let terminated = false;

  while (pos < data.length && pos < MAX_VARINT32_BYTES) {
    const b = data[pos] as number;
    pos++;

    index |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) {
      terminated = true;
      break;
    }
    shift += 7;
  }

  if (!terminated) {
    if (pos >= MAX_VARINT32_BYTES) {
      throw new Error(
        "malformed protobuf message-index varint: exceeds 5 bytes",
      );
    }
    throw new Error(
      "malformed protobuf message-index varint: missing continuation terminator",
    );
  }

  return { messageIndex: index >>> 0, payload: data.subarray(pos) };
}

function stripLeadingDot(name: string): string {
  return name.startsWith(".") ? name.slice(1) : name;
}
