#!/usr/bin/env node
// Offline publishable-manifest verifier for the AWS Glue Schema Registry TypeScript packages.
//
// For each published package (@gsr/core, @gsr/serde) this script:
//   1. Builds the package (so `dist/` is fresh).
//   2. Runs `npm pack --dry-run --json` to obtain the exact tarball manifest.
//   3. Asserts a positive allowlist (LICENSE, README.md, package.json, dist/**) plus
//      the four dual-entry exports targets under `dist/`
//      (index.mjs, index.cjs, index.d.ts, index.d.cts).
//   4. Asserts a negative manifest — no forbidden files may appear in the tarball
//      (src/**, *.test.ts, tsconfig*, tsup.config.*, vitest.config.*, examples/**,
//      bench/**, demo/**).
//   5. Asserts every shipped `.map` file has `sourcesContent` stripped so
//      published sourcemaps cannot silently leak the full package source
//      (a bypass of the src/** allowlist above).
//
// It also verifies that `@gsr/integration-tests` refuses to publish because it is
// marked `private: true` in its package.json.
//
// This is a pure-offline gate. It does not run a live publish and it is not wired
// into `npm test`. Invoke it via `npm run verify:pack` from the repository root.

import { readFileSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..');

const PUBLISHED_PACKAGES = ['packages/core', 'packages/serde'];
const PRIVATE_PACKAGE = 'packages/integration-tests';

// Exactly one of these tokens must match every published tarball entry.
const ALLOWED_TOP_LEVEL_FILES = new Set(['LICENSE', 'README.md', 'package.json']);
const ALLOWED_DIRECTORY_PREFIX = 'dist/';

// Required exports targets under dist/ (dual ESM + CommonJS). Both entry
// points (`.` public + `./internal` non-public for the byte-identity gate
// in @gsr/integration-tests) ship in the tarball.
const REQUIRED_DIST_TARGETS_BY_PACKAGE = new Map([
  [
    'packages/core',
    ['dist/index.mjs', 'dist/index.cjs', 'dist/index.d.ts', 'dist/index.d.cts'],
  ],
  [
    'packages/serde',
    [
      'dist/index.mjs',
      'dist/index.cjs',
      'dist/index.d.ts',
      'dist/index.d.cts',
      'dist/internal.mjs',
      'dist/internal.cjs',
      'dist/internal.d.ts',
      'dist/internal.d.cts',
    ],
  ],
]);

// Any tarball entry matching one of these predicates is a packaging bug.
const FORBIDDEN_MATCHERS = [
  { label: 'src/**', test: (p) => p.startsWith('src/') },
  { label: '*.test.ts', test: (p) => /(^|\/)[^/]+\.test\.ts$/.test(p) },
  { label: 'tsconfig*', test: (p) => /(^|\/)tsconfig[^/]*$/.test(p) },
  { label: 'tsup.config.*', test: (p) => /(^|\/)tsup\.config\.[^/]+$/.test(p) },
  { label: 'vitest.config.*', test: (p) => /(^|\/)vitest\.config\.[^/]+$/.test(p) },
  { label: 'examples/**', test: (p) => p.startsWith('examples/') },
  { label: 'bench/**', test: (p) => p.startsWith('bench/') },
  { label: 'demo/**', test: (p) => p.startsWith('demo/') },
];

/** Run a command in the given cwd; throw on non-zero exit. Returns stdout as string. */
function run(cmd, args, cwd, { captureStdout = false } = {}) {
  const result = spawnSync(cmd, args, {
    cwd,
    stdio: captureStdout ? ['ignore', 'pipe', 'inherit'] : 'inherit',
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error(`\`${cmd} ${args.join(' ')}\` exited with status ${result.status} (cwd=${cwd})`);
  }
  return captureStdout ? result.stdout : '';
}

/** Run npm without inheriting stdio; return { status, stdout, stderr } for grep-style checks. */
function runQuiet(cmd, args, cwd) {
  const result = spawnSync(cmd, args, { cwd, encoding: 'utf8' });
  return { status: result.status ?? -1, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
}

function readPackageJson(pkgDir) {
  const p = join(pkgDir, 'package.json');
  return JSON.parse(readFileSync(p, 'utf8'));
}

/** Verify one published package. Returns { name, version, errors: string[] }. */
function verifyPublishedPackage(relPath) {
  const pkgDir = join(repoRoot, relPath);
  const errors = [];
  const manifest = readPackageJson(pkgDir);
  const { name, version } = manifest;

  if (manifest.private === true) {
    errors.push(`package.json has private:true (should be publishable)`);
  }
  if (version !== '1.0.0') {
    errors.push(`version=${version} (expected 1.0.0)`);
  }

  // Fresh build so dist/ reflects the current source.
  run('npm', ['run', 'build'], pkgDir);

  // Grab the manifest exactly as npm pack would ship it.
  const packJson = run('npm', ['pack', '--dry-run', '--json'], pkgDir, { captureStdout: true });
  const parsed = JSON.parse(packJson);
  if (!Array.isArray(parsed) || parsed.length !== 1) {
    errors.push(`npm pack --dry-run --json did not return a single-package array`);
    return { name, version, errors };
  }
  const entry = parsed[0];
  const paths = (entry.files ?? []).map((f) => f.path);

  // Positive allowlist: every path must be a permitted top-level file or under dist/.
  const stray = paths.filter(
    (p) => !ALLOWED_TOP_LEVEL_FILES.has(p) && !p.startsWith(ALLOWED_DIRECTORY_PREFIX),
  );
  for (const p of stray) {
    errors.push(`tarball contains unexpected path: ${p}`);
  }

  // Required top-level files.
  for (const required of ALLOWED_TOP_LEVEL_FILES) {
    if (!paths.includes(required)) {
      errors.push(`tarball missing required file: ${required}`);
    }
  }

  // Required dist/ exports targets.
  const requiredTargets = REQUIRED_DIST_TARGETS_BY_PACKAGE.get(relPath) ?? [];
  for (const target of requiredTargets) {
    if (!paths.includes(target)) {
      errors.push(`tarball missing required exports target: ${target}`);
    }
    if (!existsSync(join(pkgDir, target))) {
      errors.push(`build output missing on disk: ${target}`);
    }
  }

  // Negative manifest: no forbidden file may appear.
  for (const p of paths) {
    for (const { label, test } of FORBIDDEN_MATCHERS) {
      if (test(p)) {
        errors.push(`tarball contains forbidden path (${label}): ${p}`);
      }
    }
  }

  // Sourcemap-source-leak guard. `.map` files that ship with `sourcesContent`
  // populated embed the full package source and defeat the src/** allowlist
  // above. tsup is configured to emit maps with `sourcesContent: false`; this
  // check ensures a config regression can never quietly re-inline the source.
  const mapPaths = paths.filter((p) => p.endsWith('.map'));
  for (const rel of mapPaths) {
    const onDisk = join(pkgDir, rel);
    if (!existsSync(onDisk)) {
      errors.push(`sourcemap on disk missing for shipped entry: ${rel}`);
      continue;
    }
    let map;
    try {
      map = JSON.parse(readFileSync(onDisk, 'utf8'));
    } catch (err) {
      errors.push(`sourcemap ${rel} is not valid JSON: ${err?.message ?? err}`);
      continue;
    }
    const sc = map.sourcesContent;
    if (Array.isArray(sc) && sc.some((s) => typeof s === 'string' && s.length > 0)) {
      errors.push(
        `sourcemap ${rel} embeds sourcesContent (${sc.length} entries) — sources would leak in the published tarball`,
      );
    }
  }

  return { name, version, errors };
}

/** Confirm the integration-tests package is non-publishable. Returns { name, errors }. */
function verifyPrivatePackage(relPath) {
  const pkgDir = join(repoRoot, relPath);
  const errors = [];
  const manifest = readPackageJson(pkgDir);
  const { name } = manifest;

  if (manifest.private !== true) {
    errors.push(`package.json is missing private:true`);
  }
  if (manifest.publishConfig) {
    errors.push(`package.json unexpectedly declares publishConfig`);
  }
  if (Array.isArray(manifest.files)) {
    errors.push(`package.json unexpectedly declares a files allowlist`);
  }

  // Belt-and-braces: ask npm to publish it as a workspace and require it to refuse.
  // For a private package, `npm publish --dry-run` prints a "Skipping … marked as private"
  // warning and exits 0.
  const publishOutput = runQuiet('npm', ['publish', '--dry-run', '-w', name], repoRoot);
  const combined = `${publishOutput.stdout}\n${publishOutput.stderr}`;
  const refusedMessage = `Skipping workspace ${name}, marked as private`;
  if (!combined.includes(refusedMessage)) {
    errors.push(`npm did not refuse to publish ${name}: expected "${refusedMessage}" in output`);
  }

  return { name, errors };
}

function main() {
  const results = [];
  let failed = false;

  for (const rel of PUBLISHED_PACKAGES) {
    console.log(`\n>> verifying ${rel}`);
    let result;
    try {
      result = verifyPublishedPackage(rel);
    } catch (err) {
      result = { name: rel, version: '?', errors: [String(err?.message ?? err)] };
    }
    results.push({ rel, kind: 'published', ...result });
    if (result.errors.length > 0) {
      failed = true;
      console.error(`FAIL: ${result.name}@${result.version}`);
      for (const e of result.errors) console.error(`  - ${e}`);
    } else {
      console.log(`PASS: ${result.name}@${result.version}`);
    }
  }

  console.log(`\n>> verifying ${PRIVATE_PACKAGE} (must refuse to publish)`);
  let privateResult;
  try {
    privateResult = verifyPrivatePackage(PRIVATE_PACKAGE);
  } catch (err) {
    privateResult = { name: PRIVATE_PACKAGE, errors: [String(err?.message ?? err)] };
  }
  results.push({ rel: PRIVATE_PACKAGE, kind: 'private', ...privateResult });
  if (privateResult.errors.length > 0) {
    failed = true;
    console.error(`FAIL: ${privateResult.name} (private)`);
    for (const e of privateResult.errors) console.error(`  - ${e}`);
  } else {
    console.log(`PASS: ${privateResult.name} (private, non-publishable)`);
  }

  console.log('');
  if (failed) {
    console.error('verify-pack: FAILED');
    process.exit(1);
  }
  console.log('verify-pack: OK');
}

main();
