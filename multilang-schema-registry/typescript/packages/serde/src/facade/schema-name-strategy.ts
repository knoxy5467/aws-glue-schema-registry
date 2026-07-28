/**
 * Transport-agnostic schema-name strategy seam.
 *
 * Mirrors the Java `AWSSchemaNamingStrategy` contract and the Go
 * `common.SchemaNameStrategy` interface: given the record and the transport
 * topic, produce the GSR schema name to register or look up against. The
 * seam is a pure naming function — no Glue calls, no I/O, no config surface
 * beyond the strategy instance itself. Registration (whether against a real
 * Glue endpoint or a fake) is handled where the facade is wired to a Glue
 * client; this module is deliberately independent of that.
 *
 * Two strategies ship here — the same pair the Go reference exposes on
 * `common.DefaultSchemaNameStrategy` plus a Kafka-flavored `RecordName`
 * variant that composes the transport topic with a format-appropriate
 * record identifier. `RecordNameStrategy` is best-effort: JavaScript
 * runtime objects have no universal record-name channel, so the strategy
 * inspects the record for the identifiers each format library exposes —
 * `protobufjs` dynamic messages carry `$type.fullName`, JSON Schema
 * documents may declare `$id` or `title`, and Avro specific-record shapes
 * carry a `getSchema().name`. If no identifier is present the strategy
 * degrades to the topic name so callers never see an empty schema name.
 */

/**
 * Naming contract implemented by strategies passed to the serializer. The
 * default strategy ignores `data` entirely and returns the topic — that
 * matches the Go reference and the Java default. Alternate strategies use
 * `data` to derive a per-record name.
 */
export interface SchemaNameStrategy {
  /**
   * Produce the GSR schema name from the transport topic and (optionally)
   * the record being serialized.
   */
  getSchemaName(data: unknown, topic: string): string;
}

/**
 * Names the schema with the transport topic verbatim, ignoring the record.
 * Matches Go `DefaultSchemaNameStrategy.GetSchemaName` and Java's
 * `AWSSchemaNamingStrategyDefaultImpl` — the widest-compatibility choice
 * and the default across the reference clients.
 */
export class DefaultSchemaNameStrategy implements SchemaNameStrategy {
  getSchemaName(_data: unknown, topic: string): string {
    return topic;
  }
}

/**
 * Separator between the topic and the derived record name. The Kafka
 * `TopicRecordNameStrategy` from the Confluent stack uses `-`, so we
 * match that here — schemas registered by this strategy read as
 * `<topic>-<record>` in the Glue console.
 */
const TOPIC_RECORD_SEPARATOR = "-";

/**
 * Composes the transport topic with a best-effort record name derived from
 * the record's format-specific identifier:
 *
 * - Protobuf: `data.$type.fullName` — `protobufjs` decorates messages
 *   created via `Type.create` / `Type.decode` with a non-enumerable
 *   `$type` back-reference to the `Type`, whose `fullName` is the
 *   dotted fully-qualified name (e.g. `.google.protobuf.StringValue`).
 * - JSON Schema: `data.$id` or `data.title` — the standard Draft-07
 *   identifier fields; useful when callers pass a schema-like object or
 *   annotate their instances.
 * - Avro Specific: `data.getSchema().name` or `.fullName` — mirrors the
 *   Java `SpecificRecord` accessor. Best-effort because JS objects can
 *   expose that method however the caller likes.
 * - Otherwise: `data.constructor.name` when it is not the generic
 *   `Object` / `Array` name — a last-ditch hint for POJOs whose class
 *   name carries meaning.
 *
 * When none of the above yield a usable name (null, primitives, plain
 * `Object` literals, arrays without a class name), the strategy falls
 * back to the topic alone. Consumers that need a strictly-required record
 * name should validate up front rather than relying on the fallback.
 */
export class RecordNameStrategy implements SchemaNameStrategy {
  getSchemaName(data: unknown, topic: string): string {
    const recordName = deriveRecordName(data);
    if (recordName === undefined) return topic;
    return `${topic}${TOPIC_RECORD_SEPARATOR}${recordName}`;
  }
}

/**
 * Try each format-specific identifier in the order documented on
 * `RecordNameStrategy`. Returns `undefined` when the record carries no
 * usable name; callers translate that into a topic-only fallback.
 */
function deriveRecordName(data: unknown): string | undefined {
  if (data === null || data === undefined) return undefined;
  if (typeof data !== "object") return undefined;

  const asRecord = data as Record<string, unknown>;

  const protobufName = readProtobufFullName(asRecord);
  if (protobufName !== undefined) return protobufName;

  const jsonName = readJsonSchemaIdentifier(asRecord);
  if (jsonName !== undefined) return jsonName;

  const avroName = readAvroSpecificName(asRecord);
  if (avroName !== undefined) return avroName;

  return readConstructorName(data);
}

function readProtobufFullName(
  data: Record<string, unknown>,
): string | undefined {
  const dollarType = data.$type;
  if (dollarType === null || dollarType === undefined) return undefined;
  if (typeof dollarType !== "object") return undefined;

  const fullName = (dollarType as Record<string, unknown>).fullName;
  if (typeof fullName !== "string" || fullName.length === 0) return undefined;

  return stripLeadingDot(fullName);
}

function readJsonSchemaIdentifier(
  data: Record<string, unknown>,
): string | undefined {
  const dollarId = data.$id;
  if (typeof dollarId === "string" && dollarId.length > 0) return dollarId;

  const title = data.title;
  if (typeof title === "string" && title.length > 0) return title;

  return undefined;
}

function readAvroSpecificName(
  data: Record<string, unknown>,
): string | undefined {
  const getSchema = data.getSchema;
  if (typeof getSchema !== "function") return undefined;

  let schema: unknown;
  try {
    schema = (getSchema as () => unknown).call(data);
  } catch {
    return undefined;
  }
  if (schema === null || schema === undefined) return undefined;
  if (typeof schema !== "object") return undefined;

  const schemaRecord = schema as Record<string, unknown>;
  const fullName = schemaRecord.fullName;
  if (typeof fullName === "string" && fullName.length > 0) return fullName;

  const name = schemaRecord.name;
  if (typeof name === "string" && name.length > 0) return name;

  return undefined;
}

function readConstructorName(data: object): string | undefined {
  const ctor = (data as { constructor?: { name?: unknown } }).constructor;
  if (ctor === undefined || ctor === null) return undefined;
  const name = ctor.name;
  if (typeof name !== "string" || name.length === 0) return undefined;
  if (name === "Object" || name === "Array") return undefined;
  return name;
}

function stripLeadingDot(name: string): string {
  return name.startsWith(".") ? name.slice(1) : name;
}
