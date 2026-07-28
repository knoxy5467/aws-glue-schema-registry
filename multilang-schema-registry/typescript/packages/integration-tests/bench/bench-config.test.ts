/**
 * Tier-1 offline unit tests for the bench-harness config surface.
 *
 * The two invariants under test are the load-bearing rules that keep the
 * benchmark suites and the default `npm test` run structurally disjoint,
 * while still allowing the bench directory's Tier-1 scaffolding tests to be
 * collected by the default run:
 *
 *   1. The bench-runner config (`vitest.bench.config.ts`) discovers ONLY the
 *      `.bench.ts` files under `packages/integration-tests/bench/`. It must
 *      not accidentally glob the surrounding `.test.ts` files as benches, and
 *      it must not reach outside the bench subtree.
 *
 *   2. The default config (`vitest.config.ts`) includes the additive
 *      `packages/*\/bench/**\/*.test.ts` line so bench-directory unit tests
 *      run under `npm test`, while continuing to exclude every
 *      `*.integ.test.ts` file (the offline-default guarantee) and while never
 *      including a `.bench.ts` glob (which would pull measurement suites into
 *      the default run and break offline).
 *
 * These assertions inspect the exported vitest config objects directly —
 * shape properties, not runtime behaviour — so the test is deterministic,
 * offline, and doesn't depend on file-system discovery.
 */

import { describe, expect, it } from "vitest";

import benchConfig from "../../../vitest.bench.config.js";
import defaultConfig from "../../../vitest.config.js";

// Both configs are the direct object shape produced by `defineConfig({...})`;
// we type them via a narrow structural assertion of only the fields we read.
type WithTestBlock = {
  test?: {
    include?: string[];
    exclude?: string[];
    benchmark?: {
      include?: string[];
      exclude?: string[];
    };
  };
};

const bench = benchConfig as WithTestBlock;
const def = defaultConfig as WithTestBlock;

const BENCH_INCLUDE_GLOB = "packages/integration-tests/bench/**/*.bench.ts";
const DEFAULT_BENCH_UNIT_TEST_GLOB = "packages/*/bench/**/*.test.ts";
const DEFAULT_INTEG_EXCLUDE_GLOB = "**/*.integ.test.ts";

describe("vitest.bench.config.ts (benchmark discovery)", () => {
  it("exposes a benchmark block on the test config", () => {
    expect(bench.test).toBeDefined();
    expect(bench.test?.benchmark).toBeDefined();
  });

  it("targets exactly the bench subtree with the .bench.ts suffix", () => {
    const includes = bench.test?.benchmark?.include ?? [];
    expect(includes).toEqual([BENCH_INCLUDE_GLOB]);
  });

  it("does not include any .test.ts glob (only .bench.ts is measured)", () => {
    const includes = bench.test?.benchmark?.include ?? [];
    for (const g of includes) {
      expect(g.endsWith(".bench.ts"), `bench include ${g} must end .bench.ts`).toBe(true);
      expect(g.includes(".test.ts"), `bench include ${g} must not glob .test.ts`).toBe(false);
    }
  });

  it("does not reach outside the bench subtree", () => {
    const includes = bench.test?.benchmark?.include ?? [];
    for (const g of includes) {
      expect(
        g.startsWith("packages/integration-tests/bench/"),
        `bench include ${g} must live under packages/integration-tests/bench/`,
      ).toBe(true);
    }
  });
});

describe("vitest.config.ts (default suite)", () => {
  it("includes the additive bench-unit-test glob", () => {
    const includes = def.test?.include ?? [];
    expect(includes).toContain(DEFAULT_BENCH_UNIT_TEST_GLOB);
  });

  it("preserves the *.integ.test.ts exclude (offline-default preserved)", () => {
    const excludes = def.test?.exclude ?? [];
    expect(excludes).toContain(DEFAULT_INTEG_EXCLUDE_GLOB);
  });

  it("never globs a .bench.ts file in the default include list", () => {
    const includes = def.test?.include ?? [];
    for (const g of includes) {
      expect(g.includes(".bench.ts"), `default include ${g} must not match .bench.ts`).toBe(false);
    }
  });

  it("preserves the pre-existing bench-adjacent include lines", () => {
    // The pre-existing include lines are the byte-identical baseline the
    // additive bench-unit-test glob extends. Locking them in this assertion
    // catches a regression where the additive edit accidentally rewrites or
    // drops a sibling include.
    const includes = def.test?.include ?? [];
    expect(includes).toContain("packages/*/test/**/*.test.ts");
    expect(includes).toContain("packages/*/src/**/*.test.ts");
    expect(includes).toContain("packages/*/demo/**/*.test.ts");
  });
});
