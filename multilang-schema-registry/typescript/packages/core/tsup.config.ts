import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "node20",
  outDir: "dist",
  outExtension({ format }) {
    return { js: format === "esm" ? ".mjs" : ".cjs" };
  },
  // Strip `sourcesContent` from generated `.map` files so shipped sourcemaps
  // never embed the full package source. Downstream consumers can still
  // symbolicate stack frames via the `sources` file references; the tarball
  // just stops carrying the source bytes twice (once in dist, once inlined
  // in every .map). `verify-pack.mjs` asserts no shipped .map contains
  // `sourcesContent` so this cannot silently regress.
  esbuildOptions(options) {
    options.sourcesContent = false;
  },
});
