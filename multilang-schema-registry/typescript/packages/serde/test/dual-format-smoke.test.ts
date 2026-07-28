import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const distDir = resolve(packageRoot, "dist");
const esmEntry = resolve(distDir, "index.mjs");
const cjsEntry = resolve(distDir, "index.cjs");
const dtsEntry = resolve(distDir, "index.d.ts");

describe("@gsr/serde dual-format build artifacts", () => {
  it("emits .mjs, .cjs and .d.ts under dist/", () => {
    expect(existsSync(esmEntry), `${esmEntry} should exist`).toBe(true);
    expect(existsSync(cjsEntry), `${cjsEntry} should exist`).toBe(true);
    expect(existsSync(dtsEntry), `${dtsEntry} should exist`).toBe(true);
  });

  it("loads the built package under ESM import()", async () => {
    const mod = await import(pathToFileURL(esmEntry).href);
    expect(mod).toBeTypeOf("object");
  });

  it("loads the built package under CJS require()", () => {
    const require = createRequire(import.meta.url);
    const mod = require(cjsEntry);
    expect(mod).toBeTypeOf("object");
  });
});
