/**
 * vitest-bench decode measurement suite.
 *
 * Iterates the decode direction of the shared case matrix
 * (`DECODE_CASES` from `./cases.ts` — 24 cases: 4 format-variants × 2
 * compression modes × 3 payload sizes) and measures
 * `GsrDeserializer.deserialize` throughput on a pre-encoded wire buffer.
 *
 * The measured hot loop is deliberately narrow:
 *
 *     bench(case.name, () => { case.deserializer.deserialize(case.request); })
 *
 * Everything that a first call would trigger — schema parse (Avro / JSON),
 * `.proto` text parse, ajv validator compile — happens ONCE at case-build
 * time inside `buildDecodeCase`, and the returned `request.data` is already
 * the encoded `Buffer` produced by a paired serializer on the correct
 * compression mode. The bench therefore measures steady-state decode over a
 * warm cache, mirroring the reference client's "warm cache" benchmark
 * framing so the two sides' ops/sec numbers compare apples-to-apples.
 *
 * File suffix contract: this file ends in `.bench.ts`. The default
 * `vitest.config.ts` glob matches `.test.ts` only, so `npm test` never
 * loads this file. It is discovered exclusively by `vitest.bench.config.ts`
 * (invoked via the root `bench` npm script).
 *
 * Correctness contract: the benchmark reports throughput; it MUST NOT
 * assert on it. Ecosystem differences between `avsc` / `protobufjs` / `ajv`
 * and the Go/Java reference libraries are expected and are surfaced by the
 * comparison report, not treated as a regression.
 */

import { bench, describe } from "vitest";

import { DECODE_CASES, buildDecodeCase, type DecodeCase } from "./cases.js";

// Materialize the ready-to-measure cases once at collect time. The encoded
// buffer, the deserializer, and the request object are all reused across
// every iteration of the measured loop — the loop body only calls
// `deserialize`, which is exactly the surface the benchmark exists to
// characterize.
//
// Materialization is done inside the describe body (rather than at module
// scope) so a discovery failure in any one case surfaces as a vitest
// failure attached to that case's name, not a module-load crash that
// disables the whole suite.
describe("decode", () => {
  const decodeCases: DecodeCase[] = DECODE_CASES.map((descriptor) =>
    buildDecodeCase(descriptor),
  );

  for (const decodeCase of decodeCases) {
    const { descriptor, deserializer, request } = decodeCase;
    bench(descriptor.name, () => {
      deserializer.deserialize(request);
    });
  }
});
