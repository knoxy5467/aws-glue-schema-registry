import { defineConfig } from "vitest/config";

// Benchmark project: the ONLY vitest config that globs `*.bench.ts` files.
// This is the sibling of the default `vitest.config.ts` and the integration
// `vitest.integration.config.ts`, and it is the compile/select-time gate for
// benchmark measurement suites. The default config's include list matches on
// the `.test.ts` suffix, so `.bench.ts` files are structurally invisible to
// `npm test`; they run only when this config is selected via
// `vitest bench --config vitest.bench.config.ts`.
//
// Discovery is deliberately narrow (a single subtree, `packages/integration-tests/bench/`)
// so bench measurement never leaks into an unrelated package's source or test tree.
export default defineConfig({
  test: {
    benchmark: {
      include: [
        "packages/integration-tests/bench/**/*.bench.ts",
      ],
      exclude: [
        "**/node_modules/**",
        "**/dist/**",
      ],
    },
    passWithNoTests: true,
    globals: false,
    environment: "node",
    reporters: ["default"],
  },
});
