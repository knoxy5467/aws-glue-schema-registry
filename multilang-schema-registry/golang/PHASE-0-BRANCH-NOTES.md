# Phase 0 — Branch Alignment Findings

**Status:** read-only investigation. No remote action taken (no push, no PR
opened, no fork/upstream config changes). This file records what was found so
the project owner can decide the move-strategy off-line.

**Investigation date:** 2026-06-21

## What the plan needs

Plan §2.1, §6.3, §7-Phase 0: the work must eventually land on
`awslabs/aws-glue-schema-registry` under branch name
`native-schema-registry-golang`. The canary CDK
(`AWSSchemaRegistryClientCanaryInfraCDK/lib/stacks/githubOidcStack.ts`)
already trusts that exact `(repo, branch)` pair for OIDC. Any other branch
name on upstream requires a CDK change in lock-step.

## What exists today (verified 2026-06-21)

### Upstream `awslabs/aws-glue-schema-registry`

```
$ gh api repos/awslabs/aws-glue-schema-registry/branches/native-schema-registry-golang
{"message":"Branch not found", ... "status":"404"}
```

No `*golang*` branch on upstream at all. The `native-*` branches that *do*
exist:

- `native-schema-1.1.25-tests`
- `native-schema-registry-2025`
- `native-schema-registry-csharp`
- `native-schema-registry-release`
- `native-schema-trunk`

### Fork `knoxy5467/aws-glue-schema-registry`

Golang-relevant branches on the fork:

- `golang-mrknox` *(active — Phase 0 work now committed locally on top)*
- `golang-build-automation`
- `caching+perf-tests-golang`
- `negative-golang-tests`
- `integration-test-polygot`
- `polygot-integration-tests`

### Local repo state (post Phase 0 commits, pre Phase 1)

```
golang-mrknox (local, ahead of origin/golang-mrknox by 4 commits):
  d35843f  Phase 0: doc-comment Java parity refs in protobuf_utils; flag stub-tracking tests with TODO(phase 1)
  f72e859  Phase 0: add CONTRIBUTING.md
  a033b03  Phase 0: add DEVELOPMENT.md with corp-network Go setup
  094bf1d  Phase 0: gate integration tests behind build tag and env var
  179b2be  golang 1.20 go.mod files                          (origin/golang-mrknox tip)
```

## Implications

1. **Canary OIDC is blocked.** Until a `native-schema-registry-golang` branch
   exists on `awslabs/aws-glue-schema-registry`, the canary CodeBuild project
   cannot use OIDC to pull source. Phase 5 (Canary harness) is gated on this.
2. **No remote action taken from this thread.** Per the user's `no-pr-requests`
   directive and the explicit Phase 0+1 ground rule ("no git push, no
   gh pr create, no remote interaction"), nothing has been pushed, no PR has
   been drafted, no branch has been created on upstream.
3. **The 4 local commits on `golang-mrknox`** (and any future Phase 1
   commits) remain entirely local until the project owner decides the move strategy.

## Options for the project owner (NOT decided here)

These are the realistic paths forward. Listed for the user to choose; this
script does not pick one.

### Option A — push the fork branch + open an upstream PR

1. Push `golang-mrknox` to `origin/golang-mrknox` so the fork is current.
2. Open a PR from `knoxy5467:golang-mrknox` →
   `awslabs:native-schema-registry-golang` (target branch does not exist —
   the PR template will create it on merge, but GitHub may require an
   awslabs maintainer to seed the empty branch first; see Option B).
3. PR description references the plan (`GSR-Golang-Plan-revision.md`) and
   the four Phase 0 commits.

### Option B — ask an awslabs maintainer to seed `native-schema-registry-golang`

If upstream prefers an empty branch first, an awslabs maintainer with
push to `awslabs/aws-glue-schema-registry` can run:

```
git checkout -b native-schema-registry-golang upstream/master  # or wherever they want to base it
git push upstream native-schema-registry-golang
```

Once seeded, Option A's PR target exists and the canary OIDC begins to
trust it. The four local commits then go on top via PR (or direct push for
the maintainer).

### Option C — keep working on the fork; update CDK allowlist later

Plan §3.1 goal 6 says the work should land upstream "under the branch name
the canary CDK already trusts." If upstream is slow to accept Go
contributions, the CDK can instead be updated to allowlist
`knoxy5467:golang-mrknox`. That's a one-line edit to
`AWSSchemaRegistryClientCanaryInfraCDK/lib/stacks/githubOidcStack.ts` and
a separate CR there. This is the fallback if Option A/B are blocked.

### Decision NOT taken in this session

The user's instructions for this session were "local only — no push, no PR,
no remote interaction." Picking among A / B / C is left to the project owner. This
file is the paper trail.

## What to grep for when this file becomes stale

When the move actually happens, this file can be deleted in the same commit
that pushes `golang-mrknox` (or renames it on the fork) and opens the
upstream PR. Until then, it lives here as the single source of truth for
"why the branch alignment is still pending."
