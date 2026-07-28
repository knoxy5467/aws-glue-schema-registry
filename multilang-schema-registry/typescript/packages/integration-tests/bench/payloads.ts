/**
 * Deterministic benchmark payload generator and single-`blob`-field fixture
 * schemas.
 *
 * The reference Go client ships a benchmark matrix whose MB/s numbers only
 * line up with the TypeScript numbers when both sides consume byte-identical
 * input payloads. That is what this module exists to guarantee.
 * `perfPayload(size)` produces the same bytes for the same size as the
 * reference `PerfPayload(size)` implementation, using the same xorshift64
 * seed, the same 64-character alphabet, and the same middle-bits mapping.
 * The pinned SHA-256 digests in the co-located unit test are the falsifiable
 * proof.
 *
 * Why xorshift64 rather than `alphabet[i % alphabet.length]`:
 *   - The modulo pattern is a 63-byte cycle. LZ77 compresses that to about
 *     1 % of the input size, so a ZLIB throughput number ends up measuring
 *     compression against a near-empty deflate stream instead of realistic
 *     data (roughly 10-30 % compression ratio for JSON/Avro text).
 *   - xorshift64 has period 2^64 - 1. The output has no detectable
 *     periodicity at any practical benchmark buffer size, so zlib hits
 *     realistic dictionary turnover.
 *   - It is deterministic from a fixed seed: same bytes every run means
 *     benchstat-style regression detection reflects code changes, not
 *     input drift.
 *   - Every output byte lands in a printable-ASCII window (the 64-char
 *     alphabet), so JSON-encoding the payload as a `string` field stays
 *     lossless (no UTF-8 replacement, no escape blow-up).
 *
 * The Java benchmark and the Go benchmark ship an identical xorshift64
 * implementation with the same seed and the same alphabet literal. The
 * three implementations are load-bearing in lockstep — do not edit one
 * without editing the other two, and the pinned SHA-256 digests in
 * `payloads.test.ts` are the drift alarm on the TypeScript side.
 *
 * Fixture schemas mirror the reference's `test_helpers/perf_fixtures.go`
 * single-`blob`-field shapes so the encoded structure is equivalent
 * across languages. The proto uses `string blob = 1` here where the Go
 * reference uses `bytes blob = 1`; the *payload bytes* the generator
 * produces are identical, only the proto field wire-type differs so the
 * same printable-ASCII payload round-trips as a JavaScript string
 * through `protobufjs`. The benchmark report labels this the sole
 * sanctioned ecosystem-library difference and never silently normalizes
 * it.
 */

/**
 * xorshift64 starting state, matching the Go reference's
 * `PerfPayloadSeed`. The golden-ratio constant is arbitrary — any non-zero
 * 64-bit value would do — but this specific value is what the Go and
 * Java benchmarks pin, so it MUST NOT be edited on this side alone.
 */
export const PERF_PAYLOAD_SEED = 0x9e3779b97f4a7c15n;

/**
 * 64-character alphabet the xorshift64 output is mapped into. Identical
 * (character for character, in the same order) to the Go and Java
 * benchmarks. Every byte the generator emits is one of these characters,
 * which is why the SHA-256 digest pin in `payloads.test.ts` is a hex
 * digest and never a committed literal payload prefix.
 */
export const PERF_PAYLOAD_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .";

/**
 * Fully-qualified protobuf message name for the fixture schema. This
 * mirrors the reference's `PerfPayloadMessageName` and is the name the
 * `@gsr/serde` protobuf path resolves to via `messageFullName`.
 */
export const PERF_PAYLOAD_MESSAGE_FULL_NAME = "perf.Payload";

/**
 * Single-field Avro record schema used by both Avro benchmark variants
 * (Generic and Specific). Byte-identical to the Go reference's
 * `PerfAvroSchema` constant so the encoded record structure lines up
 * cross-language.
 */
export const PERF_AVRO_SCHEMA =
  '{"type":"record","name":"PerfRecord","namespace":"perf","fields":[{"name":"blob","type":"string"}]}';

/**
 * Single-field JSON-Schema object schema used by the JSON benchmark
 * variant. Byte-identical to the Go reference's `PerfJSONSchema`.
 */
export const PERF_JSON_SCHEMA =
  '{"type":"object","properties":{"blob":{"type":"string"}},"required":["blob"]}';

/**
 * proto3 source text for the protobuf fixture schema. Differs from the
 * Go reference's `PerfProtoTextSchema` only in the `blob` field type
 * (`string` here vs. `bytes` in Go) — the documented ecosystem-library
 * deviation surfaced in the benchmark report and never silently
 * normalized.
 */
export const PERF_PROTO_TEXT_SCHEMA =
  'syntax = "proto3"; package perf; message Payload { string blob = 1; }';

const UINT64_MASK = (1n << 64n) - 1n;

/**
 * Generate a deterministic, printable-ASCII payload of `size` bytes. The
 * output is byte-identical to the Go reference's `PerfPayload(size)` for
 * the same `size` — asserted by the pinned SHA-256 digests in
 * `payloads.test.ts`.
 *
 * Algorithm (Marsaglia 2003 xorshift64):
 *
 *   state = seed
 *   for i in 0..size-1:
 *     state ^= state << 13
 *     state ^= state >> 7
 *     state ^= state << 17
 *     out[i] = alphabet[(state >> 16) & 63]
 *
 * The middle-bit slice `(state >> 16) & 63` avoids the low-bit cyclic
 * structure some xorshift variants leak, and the mask matches the
 * alphabet length exactly (64) so no rejection sampling is required.
 *
 * Sizes at or below zero return an empty buffer (mirroring the
 * reference's `size <= 0` short-circuit).
 */
export function perfPayload(size: number): Buffer {
  if (!Number.isInteger(size)) {
    throw new TypeError(`perfPayload: size must be an integer, got ${size}`);
  }
  if (size <= 0) {
    return Buffer.alloc(0);
  }
  const out = Buffer.allocUnsafe(size);
  let state = PERF_PAYLOAD_SEED;
  for (let i = 0; i < size; i++) {
    state = (state ^ ((state << 13n) & UINT64_MASK)) & UINT64_MASK;
    state = (state ^ (state >> 7n)) & UINT64_MASK;
    state = (state ^ ((state << 17n) & UINT64_MASK)) & UINT64_MASK;
    const idx = Number((state >> 16n) & 63n);
    // Alphabet indexing is safe: `idx` is masked to `[0, 63]` and the
    // alphabet is exactly 64 characters. `charCodeAt` for a printable
    // ASCII char is guaranteed to fit in a single byte.
    out[i] = PERF_PAYLOAD_ALPHABET.charCodeAt(idx);
  }
  return out;
}
