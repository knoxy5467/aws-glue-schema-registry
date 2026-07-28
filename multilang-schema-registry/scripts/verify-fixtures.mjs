#!/usr/bin/env node
/**
 * verify-fixtures.mjs — monorepo-level fixture integrity checker.
 *
 * The TypeScript / Go / Java clients all consume the same shared fixture
 * trees (`shared/test/`) and Java-canonical golden wire vectors
 * (`testdata/golden-java/`). This script computes SHA-256 for every fixture
 * file and cross-checks against the committed manifest at
 * `multilang-schema-registry/fixtures.manifest.json`.
 *
 * Modes:
 *   • `--check`  (default) — read the committed manifest, recompute every
 *     hash, exit non-zero on any drift (missing file, extra file, or
 *     changed content). This is what CI runs.
 *   • `--write`  — recompute and OVERWRITE the manifest. Only run this
 *     when intentionally regenerating fixtures (typically alongside
 *     `golden-gen-java` output).
 *
 * The manifest is sorted so diffs are stable.
 *
 * Run from anywhere: paths are resolved relative to this script's location
 * (`multilang-schema-registry/scripts/`).
 */

import { createHash } from "node:crypto";
import { readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const MONOREPO_ROOT = resolve(SCRIPT_DIR, "..");
const MANIFEST_PATH = join(MONOREPO_ROOT, "fixtures.manifest.json");

/**
 * Root descriptors — each names a subtree we hash and the file kinds to
 * include (or `"*"` for every file). Kept as a constant so both `--check`
 * and `--write` agree on what "the manifest" covers.
 */
const ROOTS = [
  {
    root: "testdata/golden-java",
    // Golden wire vectors: the `.bin` bytes AND the sidecar `.json`
    // descriptors + the PROVENANCE.md contract. Any drift here would
    // silently invalidate the byte-identity gate.
    include: "*",
  },
  {
    root: "shared/test",
    // Every fixture file — source schemas (.avsc/.proto/.schema.json),
    // instance data (.data.json), and config properties files. Language
    // clients read all of them.
    include: "*",
  },
];

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function walkFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const info = statSync(full);
    if (info.isDirectory()) {
      out.push(...walkFiles(full));
    } else if (info.isFile()) {
      out.push(full);
    }
  }
  return out;
}

function toPosixRelative(rootAbs, absPath) {
  const rel = relative(rootAbs, absPath);
  return rel.split(sep).join("/");
}

/**
 * Compute the full manifest by hashing every file under the configured
 * roots. Returns entries sorted by `path` for stable diffs.
 */
function computeManifest() {
  const entries = [];
  for (const spec of ROOTS) {
    const rootAbs = resolve(MONOREPO_ROOT, spec.root);
    let files;
    try {
      files = walkFiles(rootAbs);
    } catch (err) {
      throw new Error(
        `cannot enumerate root "${spec.root}" at ${rootAbs}: ${err.message}`,
      );
    }
    for (const abs of files) {
      const rel = toPosixRelative(MONOREPO_ROOT, abs);
      const bytes = readFileSync(abs);
      entries.push({
        path: rel,
        size: bytes.length,
        sha256: sha256Hex(bytes),
      });
    }
  }
  entries.sort((a, b) => a.path.localeCompare(b.path));
  return {
    // A machine-readable pointer to the human-readable oracle. When
    // regenerating golden vectors, updating one without the other should
    // be caught by any downstream consumer that reads both.
    provenance: "testdata/golden-java/PROVENANCE.md",
    roots: ROOTS.map((r) => r.root),
    entries,
  };
}

function loadCommittedManifest() {
  try {
    const raw = readFileSync(MANIFEST_PATH, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    throw new Error(
      `cannot read manifest at ${MANIFEST_PATH}: ${err.message}\n` +
        `Run 'node scripts/verify-fixtures.mjs --write' to generate it.`,
    );
  }
}

function diffManifests(committed, computed) {
  const committedByPath = new Map(committed.entries.map((e) => [e.path, e]));
  const computedByPath = new Map(computed.entries.map((e) => [e.path, e]));
  const problems = [];
  for (const [p, c] of committedByPath) {
    const now = computedByPath.get(p);
    if (!now) {
      problems.push(`MISSING: ${p} (was ${c.size} bytes, sha ${c.sha256})`);
      continue;
    }
    if (now.sha256 !== c.sha256 || now.size !== c.size) {
      problems.push(
        `CHANGED: ${p} — committed ${c.size} bytes sha ${c.sha256}, now ${now.size} bytes sha ${now.sha256}`,
      );
    }
  }
  for (const [p, now] of computedByPath) {
    if (!committedByPath.has(p)) {
      problems.push(`EXTRA:   ${p} (${now.size} bytes, sha ${now.sha256})`);
    }
  }
  return problems;
}

function main() {
  const mode = process.argv.includes("--write") ? "write" : "check";
  const computed = computeManifest();

  if (mode === "write") {
    writeFileSync(
      MANIFEST_PATH,
      JSON.stringify(computed, null, 2) + "\n",
      "utf8",
    );
    console.log(
      `[verify-fixtures] wrote ${computed.entries.length} entries to ${relative(process.cwd(), MANIFEST_PATH)}`,
    );
    return;
  }

  const committed = loadCommittedManifest();
  const problems = diffManifests(committed, computed);
  if (problems.length === 0) {
    console.log(
      `[verify-fixtures] OK — ${computed.entries.length} fixture(s) match the committed manifest.`,
    );
    return;
  }
  console.error(
    `[verify-fixtures] FAIL — ${problems.length} drift(s) vs ${relative(process.cwd(), MANIFEST_PATH)}:`,
  );
  for (const problem of problems) {
    console.error(`  • ${problem}`);
  }
  console.error(
    `\nRegenerate with: node ${relative(process.cwd(), fileURLToPath(import.meta.url))} --write`,
  );
  process.exit(1);
}

main();
