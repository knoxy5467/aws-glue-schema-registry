import { defineConfig } from "vitest/config";

// Integration (Tier-2/3) vitest project: the ONLY config that globs
// `*.integ.test.ts` files. This is the sibling of the default `vitest.config.ts`
// and the compile/select-time gate — files matched here are invisible to
// `npm test` (which excludes the suffix). Runtime env gating (AWS_INTEGRATION,
// GSR_GLUE, credential checks) is added in each integration test file itself.
export default defineConfig({
  test: {
    include: [
      "packages/*/test/**/*.integ.test.ts",
      "packages/*/src/**/*.integ.test.ts",
    ],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
    ],
    passWithNoTests: true,
    globals: false,
    environment: "node",
    reporters: ["default"],
  },
});
