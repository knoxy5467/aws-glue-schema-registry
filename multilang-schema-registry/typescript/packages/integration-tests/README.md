# @gsr/integration-tests

Tiered integration and interop test harness for the AWS Glue Schema Registry
TypeScript client. This workspace is **private** (never published) and hosts
everything above the pure unit-test tier: fake-backend integration scenarios,
real-AWS scenarios, cross-language interop, and fixture-sweep checks.

The Go reference client is the test-methodology oracle. Where the Go client
uses a compile-time build tag (`//go:build integration`) stacked with a
runtime env gate (`AWS_INTEGRATION=1`, `GSR_GLUE=real`), the TypeScript client
reproduces the same two-gate discipline with a filename-suffix gate
(`*.integ.test.ts`) plus a runtime env gate. The net effect is identical:
`npm test` is always fully offline; anything that could touch the network,
Docker, or AWS lives behind at least two independent switches.

For operator instructions to run against real Glue (`npm run test:integ:real`),
see [`README-REAL-AWS.md`](./README-REAL-AWS.md).

---

## The three tiers at a glance

| Tier | What it exercises | Selection glob | Runtime gate | Requires |
|------|-------------------|----------------|--------------|----------|
| Tier-1 | Pure unit + wire-format correctness. Runs on every `npm test`. | `*.test.ts` (excluding `*.integ.test.ts`) | none | node only |
| Tier-2 | Control-flow against an in-memory Glue fake and a `testcontainers`-managed Kafka broker. Never bills AWS. | `*.integ.test.ts` | `AWS_INTEGRATION=1` | node + Docker (Kafka scenarios) |
| Tier-3 | Round-trip against the real AWS Glue Schema Registry control plane. Bills AWS. Every schema is per-run namespaced and torn down on exit. | `*.integ.test.ts` | `AWS_INTEGRATION=1` + `GSR_GLUE=real` + resolvable AWS credentials | node + AWS credentials in `us-east-2` (or `AWS_REGION`) |

Tier-1 is the default. Tier-2 and Tier-3 are opt-in and require the operator
to explicitly enable them through env vars documented in the
[env-gate matrix](#env-gate-matrix) below.

---

## The two gates

### Gate 1 — the `*.integ.test.ts` filename suffix (compile/select-time)

Two vitest projects sit at the monorepo root and use disjoint file-selection
globs:

- `vitest.config.ts` — the **default** project. Selected by `npm test`
  (and by `npm run coverage`). Its `include` glob matches `*.test.ts` under
  every workspace's `test/` and `src/` trees; its `exclude` glob carries an
  explicit `**/*.integ.test.ts` entry so integration files are never picked
  up.
- `vitest.integration.config.ts` — the **integration** project. Selected by
  `npm run test:integ` and `npm run test:integ:real`. Its `include` glob is
  the mirror image: it selects **only** `*.integ.test.ts` files.

The explicit `**/*.integ.test.ts` entry in the default project's `exclude`
list is load-bearing: because the suffix `.integ.test.ts` ends in `.test.ts`,
a plain `**/*.test.ts` include would also match integration files if the
exclude were removed. A permanent selection-canary file at
`test/tier2/_selection-canary.integ.test.ts` throws at module-import time
so any regression in the default-project exclude fails `npm test` loudly:

```ts
throw new Error(
  "selection-canary: this file must never be imported by the default \`npm test\` run. ..."
);
```

If you ever see that error under `npm test`, the exclude has regressed.

### Gate 2 — the runtime env switch (`AWS_INTEGRATION`)

Even inside the integration project, every Tier-2 and Tier-3 suite reads a
runtime env gate before doing any Glue work. The helper module is
`src/env-gate.ts`; the important exports are:

- `describeIntegration(name, fn)` — wraps `vitest`'s `describe`. When
  `AWS_INTEGRATION !== "1"` it delegates to `describe.skip` **and** emits a
  single loud-skip line (`SKIP <suite> — AWS_INTEGRATION!=1`) so the skip is
  never mistaken for a pass.
- `requireAwsIntegration()` — a lower-level primitive that returns a
  discriminated-union result (`{skipped: false}` or
  `{skipped: true, reason}`). Use it when you need explicit control over
  where the skip announces itself.
- `isRealGlue()` — `true` when `GSR_GLUE === "real"`. Anything else
  (unset, `""`, `"fake"`, or a typo) resolves to `false` so a misconfigured
  env cannot silently bill AWS.
- `requireRealCreds()` — **throws** (not skips) when `GSR_GLUE=real` and
  neither `AWS_PROFILE` nor `AWS_ACCESS_KEY_ID` is set. This is the
  belt-and-suspenders check that runs after the shell-level guard inside
  `test:integ:real`.
- `resolveRegion()` — returns `AWS_REGION` or falls back to the default
  `us-east-2`.

The two gates are independent. A suite with the correct filename suffix but
no runtime gate would run against real AWS the moment someone flipped
`AWS_INTEGRATION=1`, which is why every Tier-2/3 file must wrap its
top-level suite in `describeIntegration` (or call `requireAwsIntegration()`
in a `beforeAll`).

---

## Env-gate matrix

The matrix below is the source of truth for what each npm script does in
each env configuration. Env var names are identical to the Go reference
where they overlap.

| Command | Tier | `AWS_INTEGRATION` | `GSR_GLUE` | Docker | AWS creds | Behavior |
|---------|------|-------------------|-----------|--------|-----------|----------|
| `npm test` | 1 | unset | unset | no | no | Runs Tier-1 only. Tier-2/3 files not selected. Never touches network/Docker/AWS. |
| `npm run coverage` | 1 | unset | unset | no | no | Tier-1 with coverage reporting. |
| `npm run test:integ` | 2 | `1` | unset/`fake` | yes | no | Fake Glue backend + testcontainers Kafka. No AWS billing. |
| `npm run test:integ` (no Docker) | 2 | `1` | unset | no | no | Kafka-requiring scenarios loud-skip; fake-Glue control-flow scenarios still run. |
| `npm run test:integ:real` | 3 | `1` | `real` | yes | **required** | Real Glue in `AWS_REGION` (default `us-east-2`). Hard-fails if no creds. Create-scoped cleanup on exit. |

### Env vars honored

- **`AWS_INTEGRATION`** — master switch. Only the literal string `"1"` is
  accepted; `"true"`, `"yes"`, `"on"`, and other values are deliberately
  rejected so the env matrix stays verbatim.
- **`GSR_GLUE`** — `"real"` selects the real Glue seam; anything else
  (unset, `""`, `"fake"`, or a typo) resolves to the fake. The safety
  property is that a misconfigured env never silently bills AWS.
- **`AWS_REGION`** — target region for Tier-3. Defaults to `us-east-2`
  when unset or empty.
- **`AWS_PROFILE`** / **`AWS_ACCESS_KEY_ID`** — either one is sufficient
  to satisfy the Tier-3 credentials guard. See
  [`README-REAL-AWS.md`](./README-REAL-AWS.md) for the full credentials
  section.
- **`KAFKA_BROKER`** — three-state, for the Tier-2 Kafka scenarios:
  - **Absent** — testcontainers starts a broker automatically for the
    duration of the run.
  - **Set but empty** (`KAFKA_BROKER=`) — the explicit no-Docker signal:
    testcontainers is NOT started and Kafka-requiring scenarios loud-skip.
  - **Set to a non-empty bootstrap** — the testcontainers startup is
    skipped and the external broker at that address is reused.
- **`GSR_REFERENCE_ROOT`** — override for the shared-fixture root when the
  Go reference checkout lives outside the default in-repo sibling path.
  Consumed by the fixture-root resolver (see
  [Fixture-sweep + manifest](#fixture-sweep--manifest) below).

---

## Command reference

Every command below is a root-workspace npm script. Run from
`multilang-schema-registry/typescript/`.

| Command | Selected files | What it does |
|---------|----------------|--------------|
| `npm test` | `*.test.ts` under `packages/*/test/**` and `packages/*/src/**`, excluding `*.integ.test.ts` | Runs the full Tier-1 unit suite via `vitest run`. Fully offline: no network, no Docker, no AWS. |
| `npm run coverage` | Same as `npm test` | Runs Tier-1 with the vitest `--coverage` flag (v8 provider). Emits `coverage/` artifacts. |
| `npm run test:integ` | `*.integ.test.ts` under `packages/*/test/**` and `packages/*/src/**` | Runs the integration project via `vitest run --config vitest.integration.config.ts`. Every Tier-2/3 suite short-circuits under `AWS_INTEGRATION!=1`. |
| `npm run test:integ:real` | Same as `test:integ` | Wraps `test:integ` with a shell-level credentials guard (hard-fails when neither `AWS_PROFILE` nor `AWS_ACCESS_KEY_ID` is set), a 5-second abort banner, and `GSR_GLUE=real AWS_INTEGRATION=1`. |

### Recipes

Run the offline Tier-1 default:

```
npm test
```

Run Tier-1 with coverage:

```
npm run coverage
```

Run the fake-backend Tier-2 suite (no AWS billing, no creds needed):

```
AWS_INTEGRATION=1 npm run test:integ
```

Run only the non-Kafka Tier-2 scenarios (no Docker required):

```
AWS_INTEGRATION=1 KAFKA_BROKER= npm run test:integ
```

Run the real-Glue Tier-3 suite (see
[`README-REAL-AWS.md`](./README-REAL-AWS.md) for the full runbook):

```
AWS_PROFILE=<your-beta-profile> AWS_REGION=us-east-2 npm run test:integ:real
```

---

## Directory layout

```
packages/integration-tests/
├── package.json                    # private workspace, devDeps only
├── README.md                       # this file
├── README-REAL-AWS.md              # operator runbook for test:integ:real
├── src/
│   ├── env-gate.ts                 # requireAwsIntegration / isRealGlue / requireRealCreds / describeIntegration
│   ├── fake-glue.ts                # in-memory GlueClient fake with Force* affordances
│   ├── real-glue.ts                # selectGlueBackend + CleanupTracker + realSchemaName
│   └── fixture-root.ts             # [forthcoming — lands in a companion change] shared-fixture resolver + manifest check
└── test/
    ├── gate/                       # offline wire-byte-identity gate
    ├── evolution/                  # projection-parity Tier-1 scenarios
    ├── tier2/                      # fake-backend scenarios (AWS_INTEGRATION=1)
    │   └── _selection-canary.integ.test.ts   # throws on import; falsifies suffix-exclusion
    ├── tier3/                      # real-AWS scenarios (GSR_GLUE=real)
    │   └── real-glue-roundtrip.integ.test.ts
    └── fixtures/                   # [forthcoming — lands in a companion change]
        ├── fixture-sweep.integ.test.ts       # [forthcoming] sweeps every shared/test schema
        └── fixture-manifest.json             # [forthcoming] SHA-256 manifest of every swept file

# Also forthcoming in the companion change (not yet in the tree):
#   scripts/regenerate-fixture-manifest.mjs   # regenerates fixture-manifest.json after a fixture edit
```

Tier-1 tests are colocated with production sources under
`packages/*/src/**/*.test.ts` (the pattern used by `@gsr/core` and
`@gsr/serde`); integration scenarios live under
`packages/integration-tests/test/tier{2,3}/`.

---

## Fixture-sweep + manifest

> **Note — forthcoming:** the fixture-sweep harness (`src/fixture-root.ts`,
> `test/fixtures/fixture-sweep.integ.test.ts`, `test/fixtures/fixture-manifest.json`,
> and `scripts/regenerate-fixture-manifest.mjs`) lands in a companion change
> and is **not yet present in this tree**. The section below documents the
> intended workflow so the contract is visible ahead of time; commands
> referencing those paths will fail with file-not-found until the companion
> change lands.

The Go reference client and the TypeScript client share the same corpus of
schema fixtures under `multilang-schema-registry/shared/test/`. The
fixture-sweep is the drift-detection guardrail for that corpus: it walks
every `.avsc`, `.proto`, and JSON-Schema file, asserts each parses/loads
under the corresponding serde, and (where a companion instance exists)
round-trips.

### The resolver

`src/fixture-root.ts` resolves the fixture root in this order:

1. The **in-repo sibling path** — `<repo>/multilang-schema-registry/shared/test`
   if present. This is the ratified default when the TS project ships as a
   sibling of the Go reference in the same monorepo.
2. The **`GSR_REFERENCE_ROOT` env override** — a local-checkout escape
   hatch used by the wire-byte-identity gate today. When set, the fixture
   root is resolved as `${GSR_REFERENCE_ROOT}/shared/test`.

`listFixtures(kind)` enumerates every fixture of a given kind under the
resolved root.

### The manifest

A `fixture-manifest.json` colocated with the sweep test records each swept
file's relative path and SHA-256. On every run the sweep asserts the on-disk
set matches the manifest exactly. If a fixture is added, removed, or its
bytes change without the manifest being updated in the same commit, the
sweep fails with the offending path.

### Workflow: adding or updating a fixture

1. Add or edit the fixture under `multilang-schema-registry/shared/test/`.
2. Run the fixture-sweep locally:

   ```
   AWS_INTEGRATION=1 npm run test:integ -- test/fixtures/fixture-sweep.integ.test.ts
   ```

3. The sweep will fail with the drift message. Regenerate the manifest with
   the provided helper (adjust path to your local invocation):

   ```
   node packages/integration-tests/scripts/regenerate-fixture-manifest.mjs
   ```

4. Commit the fixture change **and** the updated `fixture-manifest.json` in
   the same commit. A fixture change with a stale manifest is a bug the
   sweep will surface on the next run.

The sweep is a Tier-2 scenario (env-gated behind `AWS_INTEGRATION=1`)
because it exercises the serde parse/load paths against the full corpus,
which is heavier than a Tier-1 module test. It does not touch AWS or
Docker and is safe to run without credentials.

---

## Loud-skip contract

Every skipped Tier-2/3 suite and every skipped precondition-gated spike
emits a single line to the reporter naming the suite and the missing gate.
The shape is:

```
SKIP <suite-name> — <reason>
```

Concrete examples:

- `SKIP tier2/error-paths — AWS_INTEGRATION!=1`
- `SKIP tier3/real-glue-roundtrip — AWS_INTEGRATION!=1`
- `SKIP tier2/kafka-roundtrip — KAFKA_BROKER= (no-Docker signal)`

A bare `it.skip` or `describe.skip` with no reason line is forbidden. The
default `npm test` reporter prints a summary count of skipped suites; a
suite that skips silently is a defect.

---

## Cost, safety, and cleanup

- **`npm test`** — free. No network, no Docker, no AWS. Runs on every commit.
- **`npm run coverage`** — same as `npm test`, plus coverage report emission.
- **`npm run test:integ`** — free. Uses a `testcontainers`-managed Kafka
  broker (Docker) and an in-memory Glue fake. Does not bill AWS.
- **`npm run test:integ:real`** — **bills AWS Glue**. Rough per-run cost is
  ~4–13 Glue control-plane calls, comfortably inside the Glue free tier at
  the time of writing. Every schema created by the run is namespaced with
  the `gsr-ts-it-` prefix and torn down by the `CleanupTracker` on exit,
  regardless of test outcome. The registry (`default-registry`) is NEVER
  created or deleted by the suite. See
  [`README-REAL-AWS.md`](./README-REAL-AWS.md) for the full cost breakdown
  and the post-run leak-check command.

---

## Regression guardrails (invariants that MUST NOT break)

- `npm test` remains fully offline. No addition may add a network, Docker,
  or AWS dependency to the default target.
- No Tier-2/3 file skips silently. Every skip emits a one-line reason.
- The `*.integ.test.ts` suffix exclude in `vitest.config.ts` is
  load-bearing; the selection canary at
  `test/tier2/_selection-canary.integ.test.ts` falsifies any regression.
- No test tier ever creates or deletes a Glue registry. Every test schema
  lives inside the pre-existing `default-registry`.
- `packages/core` and `packages/serde` contain no `kafkajs` or
  `testcontainers` imports. Kafka and Docker are Tier-2 concerns and live
  only inside this workspace.

---

## Cross-references

- **Real-AWS operator runbook** — [`README-REAL-AWS.md`](./README-REAL-AWS.md).
  Prereqs, IAM permission list, `default-registry` precondition, invocation,
  expected cost, and the post-run leak-check.
- **Root TypeScript client README** — [`../../README.md`](../../README.md).
  Monorepo layout and getting-started instructions.
- **Go reference client** — the reference Go GSR client. The
  test-methodology oracle. Its real-AWS runbook is the source that
  [`README-REAL-AWS.md`](./README-REAL-AWS.md) is ported from.
