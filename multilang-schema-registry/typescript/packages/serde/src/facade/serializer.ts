/**
 * Common serializer facade.
 *
 * `GsrSerializer` is the customer-facing entry point that wires a
 * {@link DataFormat} to the matching per-format serde and composes the
 * format-produced payload with the byte-proven `@gsr/core` codec into a full
 * wire-format message. The facade never touches the wire header, ZLIB
 * compression, or the protobuf message-index directly — those live in
 * `@gsr/core` and are enforced there by construction. For protobuf the
 * facade hands the `.proto` schema text and message name to the core codec,
 * which computes the message-index (BFS+lex over the parsed schema),
 * prepends it pre-compression, and assembles the header. This preserves the
 * "index stays a core concern" invariant across every format.
 *
 * The facade is transport-agnostic — it does not import Kafka or any
 * transport client and does not perform a Glue registration call. The
 * caller supplies `schemaVersionId` directly (live Glue registration is a
 * documented later capability). A schema-name strategy is configurable so
 * callers can mirror the Java/Go `SchemaNameStrategy` seam without pulling
 * in the Glue client surface.
 */

import {
  encodeMessage,
  GsrIncompatibleDataError,
  GsrMessageTypeNotFoundError,
  type CompressionType,
} from "@gsr/core";

import { serializeAvro, type AvroSerdeOptions } from "../avro/serde.js";
import {
  JsonSchemaValidationError,
  serializeJson,
} from "../jsonschema/serde.js";
import { serializeProtobufBody } from "../protobuf/serde.js";

import {
  DefaultSchemaNameStrategy,
  type SchemaNameStrategy,
} from "./schema-name-strategy.js";
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

/** Configuration accepted by {@link GsrSerializer}'s constructor. */
export interface SerializerConfig {
  /**
   * Selected compression mode applied to every message this serializer
   * produces. Defaults to `"NONE"`. `"ZLIB"` delegates the deflate step to
   * `@gsr/core.encodeMessage`, which uses the pako-based, Java-parity
   * settings proven in the wire-core gate.
   */
  compression?: CompressionType;
  /**
   * Strategy used by {@link GsrSerializer.schemaNameFor} to derive the GSR
   * schema name from the record and the transport topic. Defaults to
   * {@link DefaultSchemaNameStrategy}, which returns the topic verbatim
   * (matches the Java and Go references' default).
   */
  schemaNameStrategy?: SchemaNameStrategy;
}

/** Per-call inputs accepted by {@link GsrSerializer.serialize}. */
export interface SerializeRequest {
  /** Format tag routed on by the facade. */
  format: DataFormat;
  /**
   * Caller-supplied schema version identifier (UUID string). Written into
   * the 18-byte wire header verbatim; live Glue registration is a documented
   * later capability.
   */
  schemaVersionId: string;
  /** Transport topic — passed through to the schema-name strategy. */
  topic: string;
  /**
   * Format-specific schema. For AVRO this is an avsc-parseable schema
   * (parsed object, JSON string, or pre-compiled `avsc.Type`); for
   * PROTOBUF this is the full `.proto` source text; for JSON this is a
   * Draft-07 JSON Schema (parsed object or JSON string).
   */
  schema: unknown;
  /**
   * The record to serialize. Shape depends on the format: Avro datum
   * (Generic or Specific), protobufjs message (plain object or message
   * instance), or arbitrary JSON instance.
   */
  data: unknown;
  /**
   * Fully-qualified protobuf message name, e.g. `"example.Order"`. Required
   * only when `format === DataFormat.PROTOBUF`; ignored otherwise.
   */
  messageFullName?: string;
  /** Optional Avro decode-shape hint reserved for symmetry with the deserializer. */
  avro?: AvroSerdeOptions;
}

/**
 * Common serializer facade. Instances are cheap; callers are expected to
 * hold one per pipeline stage rather than one per message. The class is
 * stateless beyond the injected {@link SerializerConfig}.
 *
 * @example
 * Encode an Avro record into a GSR wire message. Excerpted from
 * `examples/quick-start.ts` (type-checked by
 * `npm run docs:examples:typecheck`).
 * ```ts
 * import { DataFormat, GsrSerializer, type SerializeRequest } from "@gsr/serde";
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
 * interface OrderRecord {
 *   id: string;
 *   amount: number;
 *   quantity: number;
 *   status: "OPEN" | "PAID" | "CANCELLED";
 * }
 *
 * const originalRecord: OrderRecord = {
 *   id: "ord-42",
 *   amount: 19.99,
 *   quantity: 3,
 *   status: "PAID",
 * };
 *
 * // 3. Serialize. `schemaVersionId` is the Glue `SchemaVersionId` UUID; the
 * //    client writes it verbatim into the 18-byte wire header.
 * const serializer = new GsrSerializer({ compression: "NONE" });
 *
 * const serializeRequest: SerializeRequest = {
 *   format: DataFormat.AVRO,
 *   schemaVersionId: "01020304-0506-0708-090a-0b0c0d0e0f10",
 *   topic: "orders",
 *   schema: orderSchema,
 *   data: originalRecord,
 * };
 *
 * const wireMessage: Buffer = serializer.serialize(serializeRequest);
 * ```
 */
export class GsrSerializer {
  private readonly compression: CompressionType;
  private readonly schemaNameStrategy: SchemaNameStrategy;

  constructor(config?: SerializerConfig) {
    this.compression = config?.compression ?? "NONE";
    this.schemaNameStrategy =
      config?.schemaNameStrategy ?? new DefaultSchemaNameStrategy();
  }

  /**
   * Produce a full wire-format message (18-byte header + payload region)
   * from a format-agnostic {@link SerializeRequest}.
   *
   * The composition order is fixed: per-format serde produces the format
   * payload bytes; `@gsr/core.encodeMessage` prepends the protobuf
   * message-index (when applicable), applies ZLIB compression (when
   * configured), and prepends the header. The facade never re-implements
   * any of that logic — it only routes the format-specific inputs into the
   * core codec.
   *
   * Any failure — schema parse error, protobuf verify miss, JSON-Schema
   * validation failure, missing protobuf message name — surfaces as one
   * of the wire-format error types (`GsrIncompatibleDataError` /
   * `GsrMessageTypeNotFoundError` from `@gsr/core`, or
   * `JsonSchemaValidationError` from the JSON-Schema serde). No raw
   * `avsc` / `protobufjs` / `ajv` error escapes this surface.
   */
  serialize(req: SerializeRequest): Buffer {
    switch (req.format) {
      case DataFormat.AVRO:
        return this.serializeAvroFormat(req);
      case DataFormat.PROTOBUF:
        return this.serializeProtobufFormat(req);
      case DataFormat.JSON:
        return this.serializeJsonFormat(req);
      default: {
        // Exhaustiveness guard — `DataFormat` is a closed enum so this
        // path is unreachable for well-typed callers, but a JS caller
        // could pass a runtime-invalid value. Surface it as an
        // incompatible-data error rather than a bare `Error`.
        const value = req.format as string;
        throw new GsrIncompatibleDataError(
          `unsupported DataFormat: "${value}"`,
        );
      }
    }
  }

  /**
   * Apply the configured {@link SchemaNameStrategy} to derive the GSR
   * schema name for a request. The default strategy returns
   * `req.topic` verbatim — this method exists so callers can invoke the
   * naming seam without owning the strategy instance directly.
   */
  schemaNameFor(req: SerializeRequest): string {
    return this.schemaNameStrategy.getSchemaName(req.data, req.topic);
  }

  private serializeAvroFormat(req: SerializeRequest): Buffer {
    let payload: Buffer;
    try {
      payload = serializeAvro(
        req.schema as Parameters<typeof serializeAvro>[0],
        req.data,
        req.avro,
      );
    } catch (cause) {
      // avsc compiles the schema outside its own try/catch — a
      // JSON.parse throw on an unparseable schema escapes as a raw
      // SyntaxError. Normalize here to keep the facade's error
      // taxonomy uniform.
      normalizeSerdeThrow(cause, "failed to serialize Avro payload");
    }
    return encodeMessage({
      schemaVersionId: req.schemaVersionId,
      payload,
      compressionType: this.compression,
    });
  }

  private serializeProtobufFormat(req: SerializeRequest): Buffer {
    if (typeof req.schema !== "string") {
      throw new GsrIncompatibleDataError(
        "PROTOBUF serialize requires req.schema to be the .proto source text",
      );
    }
    if (
      req.messageFullName === undefined ||
      req.messageFullName === null ||
      req.messageFullName.length === 0
    ) {
      throw new GsrMessageTypeNotFoundError(
        "PROTOBUF serialize requires req.messageFullName",
      );
    }

    const schemaText = req.schema;
    const messageFullName = req.messageFullName;
    let payload: Buffer;
    try {
      payload = serializeProtobufBody(schemaText, messageFullName, req.data);
    } catch (cause) {
      normalizeSerdeThrow(cause, "failed to serialize Protobuf payload");
    }
    return encodeMessage({
      schemaVersionId: req.schemaVersionId,
      payload,
      compressionType: this.compression,
      protobuf: { schemaText, messageFullName },
    });
  }

  private serializeJsonFormat(req: SerializeRequest): Buffer {
    const schema = req.schema as Parameters<typeof serializeJson>[0];
    let payload: Buffer;
    try {
      payload = serializeJson(schema, req.data);
    } catch (cause) {
      normalizeSerdeThrow(cause, "failed to serialize JSON-Schema payload");
    }
    return encodeMessage({
      schemaVersionId: req.schemaVersionId,
      payload,
      compressionType: this.compression,
    });
  }
}
