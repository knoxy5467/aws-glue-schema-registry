import { defineConfig } from "tsup";

export default defineConfig({
  // Public entry (`src/index.ts` → `dist/index.*`) plus a non-public
  // composition subpath (`src/internal.ts` → `dist/internal.*`) consumed
  // only by the offline byte-identity gate in `@gsr/integration-tests`.
  // Keeping fixture arrays on the internal subpath keeps `dist/index.d.ts`
  // free of test-fixture symbols in the public API surface.
  entry: ["src/index.ts", "src/internal.ts"],
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "node20",
  outDir: "dist",
  // @gsr/core is a peer/runtime dependency, not a bundled implementation
  // detail: keeping it external ensures a single shared class identity
  // across the package boundary so cross-package `instanceof` on
  // `@gsr/core` error classes holds against the built serde.
  external: ["@gsr/core"],
  outExtension({ format }) {
    return { js: format === "esm" ? ".mjs" : ".cjs" };
  },
  // Strip `sourcesContent` from generated `.map` files so shipped sourcemaps
  // never embed the full package source. See @gsr/core tsup.config.ts for
  // the equivalent hook and rationale; `verify-pack.mjs` asserts no shipped
  // .map contains `sourcesContent` so this cannot silently regress.
  esbuildOptions(options) {
    options.sourcesContent = false;
  },
});
