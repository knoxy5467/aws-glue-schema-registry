/**
 * Common deserializer facade.
 *
 * `GsrDeserializer` reverses {@link GsrSerializer}: it hands the full wire
 * message to `@gsr/core.decodeMessage` (which parses the 18-byte header,
 * decompresses when the header says ZLIB, and — for PROTOBUF — strips the
 * leading message-index varint), then routes the recovered payload to the
 * matching per-format serde's decode. Neither the header, ZLIB, nor the
 * message-index leaves `@gsr/core`; the facade only routes.
 *
 * The public decode surface has a single wire-format error type: every
 * failure — corrupt header, unknown compression byte, malformed varint,
 * pako's bare-string throw on corrupt ZLIB input, truncated format payload,
 * schema-mismatched instance — surfaces as `GsrIncompatibleDataError`
 * (`GsrMessageTypeNotFoundError` for a protobuf message name that is
 * missing from the parsed schema; `JsonSchemaValidationError` for a
 * schema-mismatched JSON instance). No raw `avsc` / `protobufjs` / `ajv` /
 * `pako` throw escapes this surface.
 */

import {
  decodeMessage,
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
} from "@gsr/core";

import { deserializeAvro, type AvroSerdeOptions } from "../avro/serde.js";
import { projectAvroPayload } from "../evolution/avro-projection.js";
import {
  deserializeJson,
  JsonSchemaValidationError,
} from "../jsonschema/serde.js";
import {
  deserializeProtobufBody,
  type ProtobufSerdeOptions,
} from "../protobuf/serde.js";

import { DataFormat } from "./types.js";

/**
 * Normalize any throw from a per-format serde into the facade's public error
 * taxonomy. The three spec-sanctioned error types
 * (`GsrIncompatibleDataError`, `GsrMessageTypeNotFoundError`,
 * `JsonSchemaValidationError`) pass through unchanged; any other error —
 * a raw `SyntaxError` from `JSON.parse` on an unparseable Avro schema, a
 * raw `avsc` / `protobufjs` throw that escaped the format-serde's own
 * try/catch, etc. — is wrapped as `GsrIncompatibleDataError` with the
 * original preserved on `.cause` for diagnostics. Guarantees the facade's
 * "no raw library error escapes" contract.
 */
function normalizeSerdeThrow(cause: unknown, context: string): never {
  if (
    cause instanceof GsrIncompatibleDataError ||
    cause instanceof GsrMessageTypeNotFoundError ||
    cause instanceof JsonSchemaValidationError
  ) {
    throw cause;
  }
  const reason = cause instanceof Error ? cause.message : String(cause);
  const wrapped = new GsrIncompatibleDataError(`${context}: ${reason}`);
  wrapped.cause = cause;
  throw wrapped;
}

/** Per-call inputs accepted by {@link GsrDeserializer.deserialize}. */
export interface DeserializeRequest {
  /** Format tag routed on by the facade. */
  format: DataFormat;
  /** Full wire-format message (18-byte header + payload region). */
  data: Buffer;
  /**
   * Format-specific schema. For AVRO this is an avsc-parseable schema; for
   * PROTOBUF this is the full `.proto` source text; for JSON this is a
   * Draft-07 JSON Schema.
   */
  schema: unknown;
  /**
   * Fully-qualified protobuf message name, e.g. `"example.Order"`. Required
   * when `format === DataFormat.PROTOBUF`; ignored otherwise.
   */
  messageFullName?: string;
  /** Optional Avro decode-shape selector (GENERIC vs SPECIFIC). */
  avro?: AvroSerdeOptions;
  /** Optional Protobuf decode-shape selector (DYNAMIC vs POJO). */
  protobuf?: ProtobufSerdeOptions;
  /**
   * AVRO only. When present, the payload recovered from the wire is
   * projected from the writer schema (`schema`) into this reader
   * schema's shape — dropping fields the reader omits, filling reader
   * defaults for fields the writer never wrote, and applying
   * avsc-supported numeric promotions. When absent, the AVRO decode
   * path is writer-only.
   * Ignored for PROTOBUF (compatibility is field-number intrinsic) and
   * for JSON (documents are self-describing).
   */
  readerSchema?: string | object;
}

/**
 * Common deserializer facade. Stateless — the class exists to match the
 * serializer's shape and to give call sites a stable seam for future
 * cached schema lookups.
 *
 * @example
 * Decode a GSR wire message back into a record. Excerpted from
 * `examples/quick-start.ts` (type-checked by
 * `npm run docs:examples:typecheck`).
 * ```ts
 * import {
 *   DataFormat,
 *   GsrDeserializer,
 *   type DeserializeRequest,
 * } from "@gsr/serde";
 *
 * interface OrderRecord {
 *   id: string;
 *   amount: number;
 *   quantity: number;
 *   status: "OPEN" | "PAID" | "CANCELLED";
 * }
 *
 * const orderSchema = {
 *   type: "record",
 *   name: "Order",
 *   namespace: "example.orders",
 *   fields: [
 *     { name: "id", type: "string" },
 *     { name: "amount", type: "double" },
 *     { name: "quantity", type: "int" },
 *     {
 *       name: "status",
 *       type: {
 *         type: "enum",
 *         name: "Status",
 *         symbols: ["OPEN", "PAID", "CANCELLED"],
 *       },
 *     },
 *   ],
 * };
 *
 * // `wireMessage` came from a previous `GsrSerializer.serialize` call.
 * declare const wireMessage: Buffer;
 *
 * const deserializer = new GsrDeserializer();
 *
 * const deserializeRequest: DeserializeRequest = {
 *   format: DataFormat.AVRO,
 *   data: wireMessage,
 *   schema: orderSchema,
 * };
 *
 * const recovered = deserializer.deserialize(deserializeRequest) as OrderRecord;
 * ```
 */
export class GsrDeserializer {
  /**
   * Decode a full wire-format message back into a record. The wire header,
   * decompression, and (for protobuf) message-index stripping are handled
   * by `@gsr/core.decodeMessage`; the recovered payload is then routed to
   * the matching per-format serde. The facade never re-implements any of
   * the wire-layer logic.
   */
  deserialize(req: DeserializeRequest): unknown {
    const isProtobuf = req.format === DataFormat.PROTOBUF;

    // Core decode: parses header → decompresses if needed → strips protobuf
    // message-index when opts.protobuf is set. Every failure here is
    // already wrapped as GsrIncompatibleDataError by the core layer.
    const { payload } = decodeMessage(req.data, { protobuf: isProtobuf });

    switch (req.format) {
      case DataFormat.AVRO:
        return this.deserializeAvroFormat(payload, req);
      case DataFormat.PROTOBUF:
        return this.deserializeProtobufFormat(payload, req);
      case DataFormat.JSON:
        return this.deserializeJsonFormat(payload, req);
      default: {
        const value = req.format as string;
        throw new GsrIncompatibleDataError(
          `unsupported DataFormat: "${value}"`,
        );
      }
    }
  }

  private deserializeAvroFormat(
    payload: Buffer,
    req: DeserializeRequest,
  ): unknown {
    // When a reader schema is supplied, project the writer-encoded
    // payload into the reader's shape (field drop / default fill /
    // numeric promotion). When it is absent, keep the writer-only
    // decode path so an existing caller sees no behavior change.
    if (req.readerSchema !== undefined) {
      try {
        return projectAvroPayload(
          payload,
          req.schema as Parameters<typeof projectAvroPayload>[1],
          req.readerSchema,
          req.avro,
        );
      } catch (cause) {
        normalizeSerdeThrow(
          cause,
          "failed to project Avro payload into reader schema",
        );
      }
    }
    try {
      return deserializeAvro(
        payload,
        req.schema as Parameters<typeof deserializeAvro>[1],
        req.avro,
      );
    } catch (cause) {
      // avsc compiles the schema outside its own try/catch — a
      // JSON.parse throw on an unparseable schema escapes as a raw
      // SyntaxError. Normalize here so every failure at either layer
      // surfaces as GsrIncompatibleDataError.
      normalizeSerdeThrow(cause, "failed to deserialize Avro payload");
    }
  }

  private deserializeProtobufFormat(
    payload: Buffer,
    req: DeserializeRequest,
  ): unknown {
    if (typeof req.schema !== "string") {
      throw new GsrIncompatibleDataError(
        "PROTOBUF deserialize requires req.schema to be the .proto source text",
      );
    }
    if (
      req.messageFullName === undefined ||
      req.messageFullName === null ||
      req.messageFullName.length === 0
    ) {
      throw new GsrMessageTypeNotFoundError(
        "PROTOBUF deserialize requires req.messageFullName",
      );
    }
    try {
      return deserializeProtobufBody(
        payload,
        req.schema,
        req.messageFullName,
        req.protobuf,
      );
    } catch (cause) {
      normalizeSerdeThrow(cause, "failed to deserialize Protobuf payload");
    }
  }

  private deserializeJsonFormat(
    payload: Buffer,
    req: DeserializeRequest,
  ): unknown {
    try {
      return deserializeJson(
        payload,
        req.schema as Parameters<typeof deserializeJson>[1],
      );
    } catch (cause) {
      normalizeSerdeThrow(cause, "failed to deserialize JSON-Schema payload");
    }
  }
}
