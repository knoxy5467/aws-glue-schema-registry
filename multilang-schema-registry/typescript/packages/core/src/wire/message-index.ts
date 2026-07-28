/**
 * Protobuf message-index codec.
 *
 * The message-index is a protobuf-format concern that lives inside the payload
 * (not the outer wire header). Encoders prepend it pre-compression; decoders
 * strip it post-decompression. It is an unsigned varint (equivalent to Java
 * `CodedOutputStream.writeUInt32NoTag`) — never zig-zag.
 *
 * Index computation mirrors the Java `MessageIndexFinder` algorithm and the
 * Go reference `getMessageIndexFromProtoDefinition`:
 *
 *   1. BFS (level-order) over the schema's top-level messages, enqueueing each
 *      message's nested messages at the end of the queue.
 *   2. Collect fully-qualified message names in traversal order.
 *   3. Sort lexicographically.
 *   4. The target's position in the sorted list is its wire message-index.
 *
 * Example from Java `MessageIndexFinder` — the schema
 * `message B { message C {} message A { message D {} } }` yields
 * `B=0, B.A=1, B.A.D=2, B.C=3`.
 *
 * Parsing accepts `.proto` text. Base64 `FileDescriptorProto` input is out of
 * scope here and lives with the schema-registry integration.
 */

import protobuf from "protobufjs";
import {
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "./errors.js";

/** Maximum length of an unsigned varint32 in bytes. */
const MAX_VARINT32_BYTES = 5;

/**
 * Compute the wire message-index of a message type within a `.proto` schema.
 *
 * @param protoSchemaText   The full `.proto` source text.
 * @param messageFullName   Fully-qualified message name, e.g.
 *                          `"google.protobuf.StringValue"`. May optionally
 *                          include a leading `.`; both forms are accepted.
 * @throws {GsrMessageTypeNotFoundError} If `messageFullName` is absent from
 *   the schema's sorted message set.
 */
export function computeMessageIndex(
  protoSchemaText: string,
  messageFullName: string,
): number {
  const sorted = sortedMessageFullNames(protoSchemaText);
  const target = stripLeadingDot(messageFullName);
  const index = sorted.indexOf(target);
  if (index < 0) {
    throw new GsrMessageTypeNotFoundError(
      `protobuf message type not found: "${target}" (sorted candidates: ${JSON.stringify(sorted)})`,
    );
  }
  return index;
}

/**
 * Prepend the message-index as an unsigned varint (not zig-zag) to the payload.
 *
 * @param payload         The pre-index payload bytes.
 * @param messageIndex    Non-negative integer index (typically <= 2^31 - 1).
 */
export function prependMessageIndex(
  payload: Buffer,
  messageIndex: number,
): Buffer {
  if (!Number.isInteger(messageIndex) || messageIndex < 0) {
    throw new GsrIncompatibleDataError(
      `message index must be a non-negative integer (got ${messageIndex})`,
    );
  }

  const varintBytes: number[] = [];
  let value = messageIndex >>> 0;
  while (value >= 0x80) {
    varintBytes.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  varintBytes.push(value & 0x7f);

  const prefix = Buffer.from(varintBytes);
  return Buffer.concat([prefix, payload], prefix.length + payload.length);
}

/** Result of {@link stripMessageIndex}. */
export interface StrippedIndex {
  /** The parsed unsigned-varint message index. */
  messageIndex: number;
  /** The remaining bytes after the consumed varint prefix. */
  payload: Buffer;
}

/**
 * Read the unsigned-varint message-index prefix from `data` and return the
 * index paired with the remaining payload bytes.
 *
 * @throws {GsrIncompatibleDataError} If the varint is malformed: empty input,
 *   more than 5 bytes, or missing a continuation-bit terminator.
 */
export function stripMessageIndex(data: Buffer): StrippedIndex {
  if (data.length < 1) {
    throw new GsrIncompatibleDataError(
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
      throw new GsrIncompatibleDataError(
        "malformed protobuf message-index varint: exceeds 5 bytes",
      );
    }
    throw new GsrIncompatibleDataError(
      "malformed protobuf message-index varint: missing continuation terminator",
    );
  }

  return {
    messageIndex: index >>> 0,
    payload: data.subarray(pos),
  };
}

/**
 * Parse `.proto` text and return the schema's fully-qualified message names,
 * sorted lexicographically. Traversal is BFS (level-order): top-level messages
 * of the file first, then each message's nested messages at the end of the
 * queue. Sorting is stable and matches Java `Comparator.naturalOrder()` on
 * `Descriptor::getFullName()`.
 */
function sortedMessageFullNames(protoSchemaText: string): string[] {
  const parsed = protobuf.parse(protoSchemaText, { keepCase: true });

  const topLevel = findTopLevelMessages(parsed.root);

  const collected: string[] = [];
  const queue: protobuf.Type[] = [...topLevel];
  while (queue.length > 0) {
    const t = queue.shift() as protobuf.Type;
    collected.push(stripLeadingDot(t.fullName));
    for (const nested of nestedMessages(t)) {
      queue.push(nested);
    }
  }

  return collected.sort();
}

/**
 * Return every top-level (file-scope) message reachable from `root`, i.e.
 * every {@link protobuf.Type} that is a descendant of `root` through
 * {@link protobuf.Namespace} nodes only (never through another
 * {@link protobuf.Type}).
 *
 * `.proto` `package` declarations become intermediate {@link protobuf.Namespace}
 * nodes in the protobufjs tree, so a `package google.protobuf` file's
 * `message`s sit inside those namespace nodes rather than directly under
 * `root`.
 */
function findTopLevelMessages(root: protobuf.Root): protobuf.Type[] {
  const result: protobuf.Type[] = [];
  const stack: protobuf.NamespaceBase[] = [root];
  while (stack.length > 0) {
    const ns = stack.pop() as protobuf.NamespaceBase;
    for (const child of ns.nestedArray ?? []) {
      if (child instanceof protobuf.Type) {
        result.push(child);
      } else if (child instanceof protobuf.Namespace) {
        stack.push(child);
      }
    }
  }
  return result;
}

/** Return every immediately-nested message of `t`. */
function nestedMessages(t: protobuf.Type): protobuf.Type[] {
  return (t.nestedArray ?? []).filter(
    (c): c is protobuf.Type => c instanceof protobuf.Type,
  );
}

function stripLeadingDot(name: string): string {
  return name.startsWith(".") ? name.slice(1) : name;
}
