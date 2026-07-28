import { defineConfig } from "vitest/config";

// Default (Tier-1) vitest project: unit tests only. The `exclude` entry below is
// load-bearing: the include globs match `*.test.ts` by suffix, which also matches
// `*.integ.test.ts`. Without the explicit exclude, integration files (Tier-2/3)
// would leak into the default `npm test` run, breaking the offline guarantee.
// Integration files are selected exclusively by `vitest.integration.config.ts`.
export default defineConfig({
  test: {
    include: [
      "packages/*/test/**/*.test.ts",
      "packages/*/src/**/*.test.ts",
      "packages/*/demo/**/*.test.ts",
      "packages/*/bench/**/*.test.ts",
    ],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/*.integ.test.ts",
    ],
    passWithNoTests: true,
    globals: false,
    environment: "node",
    reporters: ["default"],
  },
});
