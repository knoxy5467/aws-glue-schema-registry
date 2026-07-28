/**
 * Encode-direction benchmark suite.
 *
 * This is a `vitest bench` suite (file suffix `.bench.ts`), collected ONLY by
 * the root `vitest.bench.config.ts` and never by the default `npm test` run.
 * Each iteration of the measured loop calls
 * `GsrSerializer.serialize(request)` for one preconstructed encode case —
 * exactly the "warm cache, format-layer + wire-layer" surface the Go
 * reference benchmark measures on its own side.
 *
 * The 24 cases enumerated by `ENCODE_CASES` are the full
 * format-variant × compression × payload-size grid:
 *
 *   - format-variants: `avro-generic`, `avro-specific`, `protobuf`, `json`
 *   - compression modes: `NONE`, `ZLIB`
 *   - payload sizes: 100 B, 10 KiB, 1 MiB
 *
 * Every `bench(name, ...)` uses the descriptor's canonical name
 * (`<format-variant>/comp-<compression>/<size-label>/encode`) so the JSON
 * results emitted by `vitest bench --outputJson` are self-describing and
 * map cleanly onto the report grid without any bespoke coupling to the
 * runner's internal shape.
 *
 * Warm setup (schema pre-parsed, serializer instance and request object
 * constructed once per case) happens at module load via `buildEncodeCase`,
 * so the measured hot loop is steady-state encode, not first-call schema
 * compile cost. The bench function performs no assertions on throughput
 * (throughput is informational only) and no I/O — the return value is
 * discarded so it cannot dead-code-eliminate, but the loop body is
 * intentionally minimal.
 */

import { bench, describe } from "vitest";

import { buildEncodeCase, ENCODE_CASES } from "./cases.js";

// Pre-build every encode case once at module load. Constructing the
// serializer + request outside the bench body is the whole point of the
// warm-cache framing: the measured loop performs the encode itself, not
// the object graph setup that a real caller would also do once per topic.
const preparedCases = ENCODE_CASES.map((descriptor) => buildEncodeCase(descriptor));

describe("encode benchmarks — GsrSerializer.serialize()", () => {
  for (const prepared of preparedCases) {
    bench(prepared.descriptor.name, () => {
      // Discard the return so a hypothetical dead-code-eliminator cannot
      // strip the call entirely; the encode side effect is fully realized
      // inside `serialize` regardless.
      prepared.serializer.serialize(prepared.request);
    });
  }
});
