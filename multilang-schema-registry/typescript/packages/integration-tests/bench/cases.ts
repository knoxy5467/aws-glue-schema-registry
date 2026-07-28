/**
 * Benchmark case matrix and per-case builders.
 *
 * The benchmark exercises the pure encode/decode paths of the `@gsr/serde`
 * facade over a matrix of realistic combinations:
 *
 *     format-variant  × compression × payload-size × direction
 *     (4 variants)      (2 modes)     (3 sizes)      (encode | decode)
 *     =  24 cells per direction, 48 total.
 *
 * The four format-variants are:
 *
 *   - `avro-generic`   — Avro encoded from a plain datum, decoded to a plain
 *                        object (the default `recordType: "GENERIC"`).
 *   - `avro-specific`  — Avro encoded from a plain datum, decoded to an
 *                        avsc typed-record instance (`recordType: "SPECIFIC"`).
 *                        Encode-side wire bytes are byte-identical to
 *                        `avro-generic`; the flag only reshapes the decoded
 *                        object.
 *   - `protobuf`       — proto3, single `string blob = 1` message.
 *   - `json`           — Draft-07 JSON-Schema with a single `blob: string`
 *                        property.
 *
 * The two compression modes are `NONE` and `ZLIB`; the three payload sizes
 * are 100 bytes, 10 KiB, and 1 MiB.
 *
 * The case matrix mirrors the Go reference's benchmark matrix so the two
 * sides' MB/s numbers compare apples-to-apples — same payload sizes, same
 * compression modes, same single-`blob`-field schema shape.
 *
 * Every case builder pre-constructs a `GsrSerializer` (or `GsrDeserializer`)
 * plus the request object once, so the measurement loop hits a steady-state
 * encode/decode rather than first-call schema-compile cost. Schema parse
 * (JSON.parse of the Avro / JSON schemas) is done once at builder time and
 * the parsed object is passed into the request; the protobuf serde requires
 * its `.proto` text as a string per the facade's contract, so it is passed
 * as text (protobufjs' parse cache handles steady-state warm behavior).
 *
 * The generator, alphabet, and fixture schemas from the sibling
 * `payloads` module are the only source of test data — the payloads are
 * byte-identical to the reference generator (asserted by the pinned
 * SHA-256 digests in `payloads.test.ts`), and every payload byte falls in
 * the 64-char printable-ASCII alphabet, so `Buffer.toString()` losslessly
 * round-trips the payload as a JavaScript string through all three format
 * serdes.
 */

import {
  DataFormat,
  GsrDeserializer,
  GsrSerializer,
  type DeserializeRequest,
  type SerializeRequest,
} from "@gsr/serde";

import {
  PERF_AVRO_SCHEMA,
  PERF_JSON_SCHEMA,
  PERF_PAYLOAD_MESSAGE_FULL_NAME,
  PERF_PROTO_TEXT_SCHEMA,
  perfPayload,
} from "./payloads.js";

/**
 * Fixed schema-version identifier written into the wire header for every
 * bench case. The value has no runtime semantics beyond being a valid UUID
 * — the benchmark exercises the pure encode/decode paths and never resolves
 * this against a live Glue registry.
 */
export const BENCH_SCHEMA_VERSION_ID = "01020304-0506-0708-090a-0b0c0d0e0f10";

/** Transport topic passed to the schema-name strategy for every bench case. */
export const BENCH_TOPIC = "perf";

/**
 * Compression modes covered by the case matrix.
 *
 * Mirrors the Go reference's `benchSerCompressionModes`. `NONE` measures the
 * pure encode/decode path; `ZLIB` measures the same path plus the codec's
 * pako-based deflate/inflate step.
 */
export const BENCH_COMPRESSIONS = ["NONE", "ZLIB"] as const;
export type BenchCompression = (typeof BENCH_COMPRESSIONS)[number];

/**
 * Format variants covered by the case matrix.
 *
 * The two Avro entries share encode-side wire bytes (avsc has no shape
 * variance on encode); they diverge only on the decoded object shape. Both
 * variants are still measured on the encode side so the matrix has the same
 * cardinality in both directions and the comparison report can render a
 * uniform grid.
 */
export const BENCH_FORMAT_VARIANTS = [
  "avro-generic",
  "avro-specific",
  "protobuf",
  "json",
] as const;
export type BenchFormatVariant = (typeof BENCH_FORMAT_VARIANTS)[number];

/**
 * Payload sizes covered by the case matrix. Mirrors the Go reference's
 * `benchSerPayloadSizes`.
 */
export interface BenchSize {
  /** Short human-legible tag used in the case name and report tables. */
  readonly label: string;
  /** Payload size in bytes. */
  readonly bytes: number;
}
export const BENCH_SIZES: readonly BenchSize[] = [
  { label: "small_100B", bytes: 100 },
  { label: "medium_10KB", bytes: 10240 },
  { label: "large_1MB", bytes: 1048576 },
];

/**
 * Direction under measurement. `encode` measures
 * `GsrSerializer.serialize`; `decode` measures `GsrDeserializer.deserialize`
 * on a pre-encoded buffer built once at case-build time.
 */
export type BenchDirection = "encode" | "decode";

/**
 * One row of the bench case matrix — a typed, self-describing descriptor
 * that both the case builders (below) and the comparison report generator
 * consume. The `name` field is the canonical string used to name the bench
 * inside a vitest-bench suite and is what the report parses back into cell
 * coordinates.
 */
export interface BenchCaseDescriptor {
  readonly formatVariant: BenchFormatVariant;
  readonly format: DataFormat;
  readonly compression: BenchCompression;
  readonly size: BenchSize;
  readonly direction: BenchDirection;
  /**
   * Canonical case name, e.g. `avro-generic/comp-NONE/small_100B/encode`.
   * Structurally stable — the report generator splits on `/` and treats
   * `comp-` as the prefix marker for the compression segment.
   */
  readonly name: string;
}

/**
 * Map a format-variant to the `DataFormat` enum value it routes on.
 */
function formatFor(variant: BenchFormatVariant): DataFormat {
  switch (variant) {
    case "avro-generic":
    case "avro-specific":
      return DataFormat.AVRO;
    case "protobuf":
      return DataFormat.PROTOBUF;
    case "json":
      return DataFormat.JSON;
  }
}

/**
 * Compose the canonical case name from a descriptor's coordinates.
 */
function nameFor(
  variant: BenchFormatVariant,
  compression: BenchCompression,
  size: BenchSize,
  direction: BenchDirection,
): string {
  return `${variant}/comp-${compression}/${size.label}/${direction}`;
}

/**
 * Enumerate every case in the matrix for a single direction. The iteration
 * order is:
 *
 *   for each format-variant, for each compression, for each size
 *
 * so the emitted array's `name` field is monotonically stable across runs.
 */
function descriptorsFor(direction: BenchDirection): BenchCaseDescriptor[] {
  const out: BenchCaseDescriptor[] = [];
  for (const variant of BENCH_FORMAT_VARIANTS) {
    for (const compression of BENCH_COMPRESSIONS) {
      for (const size of BENCH_SIZES) {
        out.push({
          formatVariant: variant,
          format: formatFor(variant),
          compression,
          size,
          direction,
          name: nameFor(variant, compression, size, direction),
        });
      }
    }
  }
  return out;
}

/** The 24 encode-direction case descriptors. */
export const ENCODE_CASES: readonly BenchCaseDescriptor[] =
  descriptorsFor("encode");

/** The 24 decode-direction case descriptors. */
export const DECODE_CASES: readonly BenchCaseDescriptor[] =
  descriptorsFor("decode");

/**
 * A parsed Avro schema object (from `JSON.parse` of `PERF_AVRO_SCHEMA`).
 * Parsed once at module load so every case built at runtime reuses the same
 * object instance. The `serializeAvro` implementation accepts an already-
 * parsed schema object and skips the `JSON.parse` step; `avsc.Type.forSchema`
 * still runs per call, which is the honest steady-state cost the benchmark
 * measures.
 */
const AVRO_SCHEMA_OBJECT: object = JSON.parse(PERF_AVRO_SCHEMA);

/**
 * A parsed JSON-Schema object (from `JSON.parse` of `PERF_JSON_SCHEMA`).
 * Same rationale as `AVRO_SCHEMA_OBJECT`. The JSON-Schema serde also caches
 * compiled ajv validators internally, so after the first call the schema
 * compile is a cache hit.
 */
const JSON_SCHEMA_OBJECT: object = JSON.parse(PERF_JSON_SCHEMA);

/**
 * Build the payload record for a given size. The record is
 * `{ blob: <utf8 string of the payload bytes> }`. Because every generator
 * output byte lies in the 64-char printable-ASCII alphabet, `.toString()`
 * (default UTF-8) round-trips the bytes losslessly as a JavaScript string
 * through Avro's `string` field, protobuf's `string blob = 1`, and JSON.
 */
function buildRecord(sizeBytes: number): { blob: string } {
  const payload = perfPayload(sizeBytes);
  return { blob: payload.toString() };
}

/**
 * Build the `SerializeRequest` for a descriptor. Schema is pre-parsed for
 * Avro and JSON so the request object is reusable across the measurement
 * loop without re-parsing the schema string on every iteration.
 */
function buildSerializeRequest(
  descriptor: BenchCaseDescriptor,
): SerializeRequest {
  const data = buildRecord(descriptor.size.bytes);
  switch (descriptor.formatVariant) {
    case "avro-generic":
    case "avro-specific":
      return {
        format: DataFormat.AVRO,
        schemaVersionId: BENCH_SCHEMA_VERSION_ID,
        topic: BENCH_TOPIC,
        schema: AVRO_SCHEMA_OBJECT,
        data,
      };
    case "protobuf":
      return {
        format: DataFormat.PROTOBUF,
        schemaVersionId: BENCH_SCHEMA_VERSION_ID,
        topic: BENCH_TOPIC,
        schema: PERF_PROTO_TEXT_SCHEMA,
        data,
        messageFullName: PERF_PAYLOAD_MESSAGE_FULL_NAME,
      };
    case "json":
      return {
        format: DataFormat.JSON,
        schemaVersionId: BENCH_SCHEMA_VERSION_ID,
        topic: BENCH_TOPIC,
        schema: JSON_SCHEMA_OBJECT,
        data,
      };
  }
}

/**
 * Build the `DeserializeRequest` for a descriptor. Mirrors
 * `buildSerializeRequest` but with the `readerSchema`-less shape and the
 * variant-specific decode-shape flag (Avro Generic vs. Specific) that
 * `avro-specific` needs.
 *
 * `data` is left off here — the caller sets it to a pre-encoded buffer
 * (produced by {@link buildDecodeCase}).
 */
function buildDeserializeRequestBase(
  descriptor: BenchCaseDescriptor,
): Omit<DeserializeRequest, "data"> {
  switch (descriptor.formatVariant) {
    case "avro-generic":
      return {
        format: DataFormat.AVRO,
        schema: AVRO_SCHEMA_OBJECT,
        avro: { recordType: "GENERIC" },
      };
    case "avro-specific":
      return {
        format: DataFormat.AVRO,
        schema: AVRO_SCHEMA_OBJECT,
        avro: { recordType: "SPECIFIC" },
      };
    case "protobuf":
      return {
        format: DataFormat.PROTOBUF,
        schema: PERF_PROTO_TEXT_SCHEMA,
        messageFullName: PERF_PAYLOAD_MESSAGE_FULL_NAME,
      };
    case "json":
      return {
        format: DataFormat.JSON,
        schema: JSON_SCHEMA_OBJECT,
      };
  }
}

/**
 * A ready-to-measure encode case. The bench suite calls
 * `serializer.serialize(request)` in its hot loop; the return is the full
 * wire message (18-byte header + payload). The bench does not read the
 * output beyond forcing side-effect evaluation.
 */
export interface EncodeCase {
  readonly descriptor: BenchCaseDescriptor;
  readonly serializer: GsrSerializer;
  readonly request: SerializeRequest;
}

/**
 * A ready-to-measure decode case. The pre-encoded buffer was produced
 * once at case-build time by a paired `GsrSerializer` on the same
 * compression mode; the bench suite calls
 * `deserializer.deserialize({...request, data: encodedBuffer})` (or a
 * pre-materialized clone) in its hot loop.
 */
export interface DecodeCase {
  readonly descriptor: BenchCaseDescriptor;
  readonly deserializer: GsrDeserializer;
  readonly request: DeserializeRequest;
  readonly encodedBuffer: Buffer;
}

/**
 * Build a ready-to-measure encode case from a descriptor. The serializer's
 * compression mode is fixed to the descriptor's compression, so the whole
 * measurement stream over this case uses the correct compression setting.
 */
export function buildEncodeCase(descriptor: BenchCaseDescriptor): EncodeCase {
  if (descriptor.direction !== "encode") {
    throw new Error(
      `buildEncodeCase: descriptor.direction must be "encode", got "${descriptor.direction}"`,
    );
  }
  const serializer = new GsrSerializer({ compression: descriptor.compression });
  const request = buildSerializeRequest(descriptor);
  return { descriptor, serializer, request };
}

/**
 * Build a ready-to-measure decode case from a descriptor. The pre-encoded
 * buffer is produced once at build time by a paired `GsrSerializer` on the
 * matching compression mode; the bench then hands that same buffer to
 * `GsrDeserializer.deserialize` in a hot loop.
 */
export function buildDecodeCase(descriptor: BenchCaseDescriptor): DecodeCase {
  if (descriptor.direction !== "decode") {
    throw new Error(
      `buildDecodeCase: descriptor.direction must be "decode", got "${descriptor.direction}"`,
    );
  }
  // The paired serializer only exists to produce the fixture buffer once;
  // it is not the object the bench measures.
  const pairedSerializer = new GsrSerializer({
    compression: descriptor.compression,
  });
  const serializeRequest = buildSerializeRequest({
    ...descriptor,
    direction: "encode",
    name: nameFor(
      descriptor.formatVariant,
      descriptor.compression,
      descriptor.size,
      "encode",
    ),
  });
  const encodedBuffer = pairedSerializer.serialize(serializeRequest);

  const deserializer = new GsrDeserializer();
  const request: DeserializeRequest = {
    ...buildDeserializeRequestBase(descriptor),
    data: encodedBuffer,
  };

  return { descriptor, deserializer, request, encodedBuffer };
}
