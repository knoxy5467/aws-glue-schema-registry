#!/usr/bin/env node
// Regression guardrail: fail the build on any planning/review process token
// in shipped TypeScript source, docs, tests, examples, or committed configs.
//
// This repository maintains a strict discipline of keeping planning /
// review artifacts (per-item planning identifiers, definition-of-done
// jargon, milestone tokens, spec-constraint IDs, review-round labels,
// "hazard record" prose as English, bare "N/N" round counts, section
// symbols outside RFC / JLS contexts, spurious INV-* ids, etc.) OUT of
// the code and docs a consumer sees. The pattern set is enumerated
// below; each entry has an id, a case-insensitive regex, and an
// optional `ignore` predicate that whitelists legit uses of the same
// token (e.g. `INV-2026-*` invoice fixture ids, RFC section citations,
// legit `HazardRecord` PascalCase identifiers). The self-test at the
// bottom exercises both directions.
//
// Exit code: 0 on clean tree, 1 on any true hit. Prints every hit as
// `path:line:content` so a failing CI log points at the exact regression.
//
// Invoke from anywhere in the workspace via `npm run check:ai-artifacts` or
// directly:  node scripts/check-no-ai-artifacts.mjs [--test]

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const monorepoRoot = resolve(scriptDir, '..', '..');

// Scan roots relative to the monorepo. The TypeScript package tree is the
// primary shipping surface; `.github/workflows` catches CI configs that could
// contain review-round labels. The Go tree has its own conventions and is
// intentionally out of scope for this guard (per the task brief the token
// axis is TS-first; a Go equivalent lives with Go tooling if ever needed).
const SCAN_ROOTS = ['typescript', '..' + sep + '.github' + sep + 'workflows'];

// Directories we skip anywhere they appear in the walk.
const SKIP_DIRS = new Set([
  'node_modules',
  'dist',
  'build',
  'coverage',
  '.git',
  '.results',
  '.vitest',
  '.turbo',
  '.next',
]);

// File extensions we scan. Committed configs use these plus a couple of
// bare filenames handled below.
const SCAN_EXTENSIONS = new Set([
  '.ts',
  '.tsx',
  '.mts',
  '.cts',
  '.js',
  '.mjs',
  '.cjs',
  '.md',
  '.json',
  '.yml',
  '.yaml',
]);

// Only THIS script is excluded from the scan — every banned pattern is
// defined here, so scanning it would deterministically report each pattern
// as a hit. Everywhere else, including the CI workflow and the guard's
// own tests, MUST NOT name banned tokens (talk about the "pattern set" or
// "committed guard" instead).
const SELF_PATHS = new Set([
  'typescript' + sep + 'scripts' + sep + 'check-no-ai-artifacts.mjs',
]);

// Banned patterns. Each entry has:
//   id      — short label for the diagnostic line.
//   regex   — case- or case-insensitive matcher. NAMED groups are avoided so
//             logic stays lexical, not linguistic.
//   ignore  — optional predicate on the FULL matched substring; when it
//             returns true the hit is dropped. Reserved for exact-legit
//             literals like `INV-2026-*` fixture ids.
// A hit is a per-line match, not a per-file one — hits are reported with the
// substring so a reviewer can eyeball the false-positive rate.
const BANNED_PATTERNS = [
  {
    id: 'pbi-token',
    // `PBI-03`, `PBI 3`, `pbi3`, `pbis`.
    regex: /\bpbis?\b[- ]?\d*/gi,
  },
  {
    id: 'dod-acronym',
    regex: /\bDoD\b/g,
  },
  {
    id: 'definition-of-done',
    regex: /\bDefinition\s+of\s+Done\b/gi,
  },
  {
    id: 'milestone-code',
    // `M1`..`M99`, standalone.
    regex: /\bM\d{1,2}\b/g,
  },
  {
    id: 'board-jargon',
    // `board`, `re-board`, `board-fix`, `FD BOARD` etc. (not `boarding`,
    // `keyboard`, `dashboard` — `\b` guards a word-boundary each side).
    regex: /\bboard\b/gi,
  },
  {
    id: 'round-jargon',
    // `round 1`, `round-1`, `R1/R2`, `R1`. Guarded so `round-trip` and
    // `all-round` do NOT match (a letter continues the token).
    regex: /\bround[- ]\d+|\bR[0-9]\/R[0-9]\b|\bR[0-9]\b/g,
  },
  {
    id: 'constraint-id',
    // Spec-constraint IDs (`C-2`, `C-14`).
    regex: /\bC-\d+\b/g,
  },
  {
    id: 'rg-id',
    // Regression-group / rule-group IDs.
    regex: /\bRG-\d+/g,
  },
  {
    id: 'scenario-id',
    // Gherkin scenario IDs `S-1`..`S-9`.
    regex: /\bS-\d+\b/g,
  },
  {
    id: 'fd-id',
    // Final-decision / feature-decision IDs like `FD-3`.
    regex: /\bFD-\d+/g,
  },
  {
    id: 'gherkin',
    regex: /\bGherkin\b/gi,
  },
  {
    id: 'files-touched',
    regex: /\bfiles[- ]touched\b/gi,
  },
  {
    id: 'hazard-record-prose',
    // The literal review prose "hazard record" as two English words. The
    // CODE identifier `HazardRecord` (one token, PascalCase) is whitelisted
    // for legit fixture symbols; this pattern only matches the space-
    // separated English form.
    regex: /\bhazard\s+records?\b/gi,
  },
  {
    id: 'bare-round-count',
    // `1/3`, `2/5` — round-count style tokens with word-boundary on each
    // side. Excludes decimal fractions and dates because those never have
    // ` ` or `\b` transitions matching `\d/\d` alone. Excludes `Tier-N/N`
    // (legit test-tiering vocabulary) via the negative lookbehind.
    regex: /(?<![\d/]|Tier-)\b\d\/\d\b(?!\d)/g,
    ignore: (match, line) => {
      // Long numeric lists like "scenarios 3/4/7/8/11/12/13" are legit and
      // pass a longer form: if the surrounding text has 3+ slashes on the
      // same line, treat as a list token, not a round-count.
      const slashCount = (line.match(/\//g) ?? []).length;
      return slashCount >= 3;
    },
  },
  {
    id: 'section-symbol',
    // Section symbol used in AI review prose (`§ 8.1`, `§8`). RFC / Java
    // Language Spec / other standards citations that mention `RFC` or `JLS`
    // anywhere on the line are legit and whitelisted via `ignore`.
    regex: /§\s?\d+(?:\.\d+)*/g,
    ignore: (match, line) => /\bRFC\b|\bJLS\b|Java\s+Language\s+Specification/i.test(line),
  },
  {
    id: 'inv-id',
    // Any `INV-...` token. The 2026-invoice fixture ids used in the JSON-
    // Schema encode-slice are whitelisted via `ignore`.
    regex: /\bINV-\d+(?:-\d+)*/g,
    ignore: (match) => /^INV-2026/.test(match),
  },
  {
    id: 'scaffolding-vocab',
    // The word `scaffolding` (case-insensitive). Review prose has historically
    // used it as a synonym for structural planning work ("scaffolding items",
    // "scaffolding milestones", "scaffolding tasks"), which is exactly the
    // planning-artifact axis this guard closes. The bench harness legitimately
    // uses the same word as test-infrastructure vocabulary — an adjacent
    // `test(s)` or `verify the scaffolding` phrase is whitelisted the same
    // narrow, lexical way `RFC §` / `Tier-N` are. Planning-artifact usages
    // ("scaffolding items / tasks / milestones / rounds") do NOT match those
    // three phrases and therefore still fail the guard.
    regex: /\bscaffolding\b/gi,
    ignore: (match, line) =>
      /\bscaffolding\s+tests?\b|\btests?\s+scaffolding\b|\bverify\s+the\s+scaffolding\b/i.test(line),
  },
];

// Case-sensitive identifier whitelist. These specific PascalCase / camelCase
// SYMBOLS are legit code identifiers (the byte-identity gate fixture arrays
// and their record types); they can appear anywhere but never as English
// prose. The regex enforces they are a full identifier token.
const IDENTIFIER_WHITELIST = [
  /\bHazardRecord\b/,
  /\bhazardRecords\b/,
  /\b(?:Avro|Protobuf|JsonSchema)HazardRecord\b/,
  /\b(?:avro|protobuf|jsonSchema)HazardRecords\b/,
];

/**
 * Return every file path we should scan under `root`, recursively. Skips
 * any directory named in `SKIP_DIRS`; only returns files whose extension is
 * in `SCAN_EXTENSIONS`.
 */
function walk(root) {
  const out = [];
  let entries;
  try {
    entries = readdirSync(root, { withFileTypes: true });
  } catch (err) {
    if (err && err.code === 'ENOENT') return out;
    throw err;
  }
  for (const entry of entries) {
    const abs = join(root, entry.name);
    if (entry.isDirectory()) {
      if (SKIP_DIRS.has(entry.name)) continue;
      out.push(...walk(abs));
      continue;
    }
    if (!entry.isFile()) continue;
    const dot = entry.name.lastIndexOf('.');
    if (dot < 0) continue;
    const ext = entry.name.slice(dot);
    if (!SCAN_EXTENSIONS.has(ext)) continue;
    out.push(abs);
  }
  return out;
}

/** Determine whether a single match (substring + surrounding line) is a
 *  false-positive by checking against the identifier whitelist and the
 *  pattern-local `ignore` predicate. */
function isWhitelisted(pattern, match, line) {
  for (const idPattern of IDENTIFIER_WHITELIST) {
    if (idPattern.test(match)) return true;
  }
  if (typeof pattern.ignore === 'function' && pattern.ignore(match, line)) {
    return true;
  }
  return false;
}

/** Return every hit for a single file, one entry per (line, pattern, match)
 *  combination. Ordering is stable so CI logs read the same across runs. */
function scanFile(absPath) {
  const relPath = relative(monorepoRoot, absPath);
  const content = readFileSync(absPath, 'utf8');
  const lines = content.split(/\r?\n/);
  const hits = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line === undefined) continue;
    for (const pattern of BANNED_PATTERNS) {
      // Fresh regex state per line — `lastIndex` on a `/g` regex would
      // otherwise skip matches on shorter lines.
      pattern.regex.lastIndex = 0;
      let m;
      while ((m = pattern.regex.exec(line)) !== null) {
        const matched = m[0];
        if (isWhitelisted(pattern, matched, line)) continue;
        hits.push({
          file: relPath,
          line: i + 1,
          id: pattern.id,
          match: matched,
          content: line.trim().slice(0, 200),
        });
      }
    }
  }
  return hits;
}

function runScan() {
  const files = [];
  for (const rel of SCAN_ROOTS) {
    const rootAbs = resolve(monorepoRoot, rel);
    files.push(...walk(rootAbs));
  }
  const allHits = [];
  for (const abs of files) {
    const rel = relative(monorepoRoot, abs);
    if (SELF_PATHS.has(rel)) continue;
    allHits.push(...scanFile(abs));
  }
  return { files, hits: allHits };
}

function reportHits(hits) {
  if (hits.length === 0) return;
  const byFile = new Map();
  for (const h of hits) {
    if (!byFile.has(h.file)) byFile.set(h.file, []);
    byFile.get(h.file).push(h);
  }
  const files = [...byFile.keys()].sort();
  for (const file of files) {
    const fileHits = byFile.get(file);
    fileHits.sort((a, b) => a.line - b.line || a.id.localeCompare(b.id));
    for (const h of fileHits) {
      console.error(
        `${h.file}:${h.line}: [${h.id}] match="${h.match}"  |  ${h.content}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Self-test: `--test` runs a small in-process suite that exercises the
// banned-token detection and the whitelist. Runs off strings, not the real
// tree, so it stays deterministic and cheap. Invoked from
// `scripts/check-no-ai-artifacts.test.mjs` too.
// ---------------------------------------------------------------------------

function assertEqual(actual, expected, label) {
  const a = JSON.stringify(actual);
  const b = JSON.stringify(expected);
  if (a !== b) {
    console.error(`FAIL ${label}\n  actual:   ${a}\n  expected: ${b}`);
    return false;
  }
  console.log(`PASS ${label}`);
  return true;
}

function scanString(text, filename = 'inline.md') {
  const lines = text.split(/\r?\n/);
  const hits = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line === undefined) continue;
    for (const pattern of BANNED_PATTERNS) {
      pattern.regex.lastIndex = 0;
      let m;
      while ((m = pattern.regex.exec(line)) !== null) {
        const matched = m[0];
        if (isWhitelisted(pattern, matched, line)) continue;
        hits.push({ line: i + 1, id: pattern.id, match: matched });
      }
    }
  }
  return hits;
}

function runSelfTest() {
  let allOk = true;

  // Direction 1: banned tokens produce hits.
  const bannedCases = [
    { text: 'implement PBI-03 per spec', expectedIds: ['pbi-token'] },
    { text: 'PBI 5 blocks the release', expectedIds: ['pbi-token'] },
    { text: 'DoD closes with green build', expectedIds: ['dod-acronym'] },
    {
      text: 'Definition of Done requires green tests',
      expectedIds: ['definition-of-done'],
    },
    { text: 'M1 delivered; M2 outstanding', expectedIds: ['milestone-code', 'milestone-code'] },
    { text: 'board-fix-2 tip', expectedIds: ['board-jargon'] },
    { text: 'RE-BOARD round 3 verdict', expectedIds: ['board-jargon', 'round-jargon'] },
    // `R1/R2` matches once as a single round-jargon token.
    { text: 'CRITICAL-1 defense R1/R2', expectedIds: ['round-jargon'] },
    { text: 'constraint C-2 unmet', expectedIds: ['constraint-id'] },
    { text: 'see rule RG-7', expectedIds: ['rg-id'] },
    { text: 'Gherkin scenario S-3 fails', expectedIds: ['scenario-id', 'gherkin'] },
    { text: 'FD-4 disagrees', expectedIds: ['fd-id'] },
    { text: 'files-touched matrix', expectedIds: ['files-touched'] },
    { text: 'add a hazard record for the drift', expectedIds: ['hazard-record-prose'] },
    { text: 'round 1/3 done', expectedIds: ['round-jargon', 'bare-round-count'] },
    { text: 'per RFC §8.1 the ... ', expectedIds: [] },
    { text: 'per §8.1 the ...', expectedIds: ['section-symbol'] },
    { text: 'INV-9999-0001 fake', expectedIds: ['inv-id'] },
    { text: 'scaffolding PBIs still open', expectedIds: ['scaffolding-vocab', 'pbi-token'] },
    { text: 'add scaffolding milestone tasks', expectedIds: ['scaffolding-vocab'] },
    { text: 'stubbed scaffolding lands FD-4 residuals', expectedIds: ['scaffolding-vocab', 'fd-id'] },
  ];
  for (const c of bannedCases) {
    const hits = scanString(c.text);
    const ids = hits.map((h) => h.id).sort();
    const expected = [...c.expectedIds].sort();
    if (!assertEqual(ids, expected, `banned: "${c.text}"`)) allOk = false;
  }

  // Direction 2: whitelisted / legit tokens produce NO hits.
  const cleanCases = [
    'The gate exercises 789 tests across 42 files.',
    'quick-start.ts — the round-trip in this README, from the top.',
    'HazardRecord fixture arrays feed the byte-identity gate.',
    'avroHazardRecords[0] carries the test-v1 fixture.',
    'invoiceId: "INV-2026-0001"',
    'Tier-1 unit tests, Tier-2 offline gates, Tier-3 real-Glue.',
    'per RFC 8259 §8.1 the JSON canonicalization ...',
    'Scenario 20 asserts the flag is on; scenarios 3/4/7/8/11/12/13/15 need it.',
    'A round-trip between encode and decode matters.',
    'A dashboard or keyboard reference is a compound word.',
    // Bench harness's legit test-infra vocabulary — see the scaffolding-vocab
    // whitelist. All three phrasings that appear on the current tree.
    "the bench directory's Tier-1 scaffolding tests to be collected",
    'Tier-1 unit tests verify the scaffolding — matrix cardinality, payload determinism',
    'test scaffolding lives under bench/ for the offline gate',
  ];
  for (const c of cleanCases) {
    const hits = scanString(c);
    if (!assertEqual(hits, [], `clean:  "${c}"`)) allOk = false;
  }

  console.log('');
  if (!allOk) {
    console.error('check-no-ai-artifacts: SELF-TEST FAILED');
    process.exit(1);
  }
  console.log('check-no-ai-artifacts: SELF-TEST PASSED');
}

function main() {
  const argv = process.argv.slice(2);
  if (argv.includes('--test')) {
    runSelfTest();
    return;
  }
  const { files, hits } = runScan();
  if (hits.length > 0) {
    reportHits(hits);
    console.error('');
    console.error(
      `check-no-ai-artifacts: FAILED — ${hits.length} hit(s) across ${
        new Set(hits.map((h) => h.file)).size
      } file(s) (scanned ${files.length} files)`,
    );
    process.exit(1);
  }
  console.log(
    `check-no-ai-artifacts: OK — 0 hits (scanned ${files.length} files)`,
  );
}

main();
