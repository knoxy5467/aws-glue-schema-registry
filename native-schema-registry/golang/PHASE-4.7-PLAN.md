# Phase 4.7 — Real-AWS Glue support in the Tier-2 integration suite

**Status:** Draft 1 — committed at the head of the Phase 4.7 work.
**Date:** 2026-06-22.
**Anchors:** GSR-Golang-Plan-revision.md §1, §2, §4, §5 (especially §5.7), §6, §7, §9, §11.

---

## 1. Why now

Phases 0–4.5 left the integration-tests module with full §5.3 scenario
coverage but every scenario runs against `integration-tests/pkg/fakeglue`. A
`grep -rln 'glue.NewFromConfig\|config.LoadDefaultConfig'` over
`integration-tests/` returns zero matches. The plan’s §5.7 explicitly says
LocalStack does not emulate Glue Schema Registry; therefore production
parity demands a real-AWS path. Phase 5 (canary harness, §6) builds on
that path — it needs a `gsrcore.GlueClient` implementation backed by the
SDK v2 `glue` client before any IAM resolver or functional canary work
starts.

Phase 4.7 wires the path. It does **not** run real-AWS tests. The user
runs `make test-integ-real` against their beta account themselves.

## 2. Scope

In:

- A new `integration-tests/pkg/realglue/` package implementing
  `gsrcore.GlueClient` over `glue.NewFromConfig`.
- A test-level selector that chooses fake vs real based on env vars.
- A scenario-annotation flag (`RequiresRealGlue`) so scenarios that
  only make sense against real Glue (server-side compatibility
  enforcement, throttling, IAM-denied) `t.Skip` on fake.
- A `t.Cleanup`-driven teardown layer that deletes every registry /
  schema a test creates, in reverse insertion order.
- Per-test randomized name prefixes so retries and parallel runs
  don’t collide.
- A `make test-integ-real` Makefile target with a 5-second
  safety-banner pause and a hard-fail if AWS creds aren’t resolvable.
- A short runbook (`integration-tests/REAL-AWS-RUNBOOK.md`) the user
  follows to actually invoke it.

Out:

- Running real-AWS tests in this pass — that’s the user’s call.
- Phase 5’s IAM resolver matrix and CloudWatch metrics — those live
  in Phase 5.
- Java↔Go interop sidecar (§5.5) — Phase 4 left this on the
  testdata-golden regression footing; Phase 4.7 doesn’t touch it.
- LocalStack — §5.7 already rules it out for Glue.

## 3. Architecture

### 3.1 The `realglue` package

```
integration-tests/pkg/realglue/
  real_glue.go        // New(ctx, opts...) -> *Real; implements gsrcore.GlueClient
  cleanup.go          // Cleanup struct + Run(ctx); narrow cleanupClient seam
  real_glue_test.go   // Tier-1 tests; no //go:build integration
  cleanup_test.go     // Tier-1 tests; recorder stub for cleanupClient
```

Mirrors `fakeglue`'s public shape (constructor + `gsrcore.GlueClient`
methods) so the selector can return either as a `gsrcore.GlueClient`
without further interface gymnastics.

`New(ctx, opts...)` options:

- `WithRegion(string)` — overrides the region. Default chain: opt,
  then `AWS_REGION` env, then `us-east-2` (matches the canary account
  in plan §6.3).
- `WithProfile(string)` — passed to `config.WithSharedConfigProfile`.
- `WithEndpoint(string)` — sets `glue.Options.BaseEndpoint`.
- `WithAWSConfig(aws.Config)` — test-only seam: callers inject a
  pre-built `aws.Config` so `New` skips `config.LoadDefaultConfig`
  and the credential probe. NOT for production use.

Production path: `config.LoadDefaultConfig(ctx, WithRegion(...),
WithSharedConfigProfile(...))` then a probing
`cfg.Credentials.Retrieve(ctx)` so a missing credential chain
surfaces at construction, not at first API call.

### 3.2 The selector

A single helper in `integration-tests/tests/glue_selector_test.go`:

```go
type glueHandle struct {
    Client gsrcore.GlueClient
    Fake   *fakeglue.Fake          // non-nil iff mode == "fake" or ""
    Real   *realglue.Real          // non-nil iff mode == "real"
    Cleanup *realglue.Cleanup      // non-nil iff Real != nil
}

func newGlueHandle(t *testing.T) *glueHandle
```

`newGlueHandle(t)` reads `GSR_GLUE`:

- `""` or `"fake"`: returns a fresh `fakeglue.Fake`. No AWS, no creds.
- `"real"`: requires `AWS_INTEGRATION=1`, calls `realglue.New`,
  attaches a `Cleanup` whose `Run` is invoked via `t.Cleanup`.

Hard-fails (not silently falls back) when:

- `GSR_GLUE=real` but `AWS_INTEGRATION != 1`.
- `GSR_GLUE=real` but `config.LoadDefaultConfig` or
  `creds.Retrieve(ctx)` errors.
- `GSR_GLUE` is set to anything other than `""`, `"fake"`, or `"real"`.

For tests that wrap the client in `BaseIntegrationSuite`, the same
helper is reachable via `s.NewGlueHandle()` on the suite. The two
entry points share the env-parsing implementation.

### 3.3 `RequiresRealGlue` annotation

`scenarioGate(t, requiresReal, requiresFake bool)`:

- `requiresReal=true` && `GSR_GLUE != "real"` → `t.Skip`.
- `requiresFake=true` && `GSR_GLUE == "real"` → `t.Skip`.

Tests that work against either mode pass both as `false` (the
default). Tests that use fakeglue affordances (`ForceCreateError`,
exact `CallCounts` assertions) opt into `requiresFake=true`. Tests
that genuinely exercise server-side enforcement (compat rejection,
real throttling) opt into `requiresReal=true`.

### 3.4 Cleanup layer

`realglue.Cleanup` keeps two ordered slices: `schemas` and
`registries`. Tests register names via `Cleanup.TrackSchema` /
`TrackRegistry` as the test creates them. `Cleanup.Run(ctx)` iterates
`schemas` then `registries`, both in reverse insertion order:

1. `glue.DeleteSchema` for each schema (registry+name).
2. `glue.DeleteRegistry` for each registry name.

Errors are collected via `errors.Join` so a single mid-run failure
doesn’t skip the rest. The selector wires `Cleanup.Run` into
`t.Cleanup` so teardown fires on `t.Fail` too.

### 3.5 Randomized prefixes

A new helper `randomGlueName(t, base)` returns
`<base>-<UNIX_TS>-<RANDOM_SHORT>` (4 random hex bytes). Tests that
currently use literal schema names (`"lifecycle-13"`, `"compat-18"`,
…) call this once per test. The fakeglue path is unaffected
(collision-free state is in-memory and torn down per-test); the
realglue path inherits collision safety on parallel runs and on
retries against the same beta account.

## 4. Test migration shape

For each of `schema_lifecycle_test.go`, `compatibility_test.go`,
`concurrency_test.go`, `negative_test.go`:

1. Replace `f := fakeglue.New()` with `h := newGlueHandle(t)`.
2. Replace literal schema names with `randomGlueName(t, "lifecycle-13")`.
3. Use `h.Client` where the test currently passes the fake to
   `NewGsrEncoderForTest`.
4. Where the test sets `ForceXxxError` or asserts exact
   `CallCounts`, gate with `scenarioGate(t, requiresFake=true)`
   and access `h.Fake` for the forced affordance.
5. Where the test makes sense to run against real Glue too,
   register cleanup via `h.Cleanup.TrackSchema(registry, name)`
   immediately after the encoder’s `Encode` succeeds.

`wire_format_direct_test.go` doesn’t touch Glue and is not
migrated.

## 5. Makefile

New `make test-integ-real`:

```
@echo "About to bill real AWS Glue in <region>. Press Ctrl-C within 5s to abort."
@sleep 5
cd integration-tests && GSR_GLUE=real AWS_INTEGRATION=1 \
  go test -tags integration -timeout 30m -count=1 ./...
```

Hard-fails earlier (before sleep) if `AWS_PROFILE` is unset AND no
default-credential discovery succeeds; the safety is layered with the
in-test credential probe.

`make test-integ` keeps its existing behavior (fakeglue path only).

## 6. Runbook

`integration-tests/REAL-AWS-RUNBOOK.md`:

- Prereqs: beta account creds, region, IAM permissions list.
- The exact `make test-integ-real` invocation.
- Expected cost ceiling (rough estimate from
  `CreateSchema`+`GetSchemaByDefinition` per scenario × matrix
  size).
- Cleanup verification:
  `aws glue list-registries --region <r>` should show zero
  `gsr-go-it-*` registries after the run; the teardown layer
  sanity-checks this.

## 7. Acceptance

- `go test -short -count=1 ./...` in outer + inner modules — green.
- `go vet -tags integration ./...` + `go build -tags integration
  ./...` in `integration-tests/` — green.
- `GSR_GLUE=real AWS_INTEGRATION=1 go test -tags integration -run
  "TestNothing$" ./integration-tests/...` — passes with 0 tests.
- `make -n test-integ-real` shows the right command line and the
  safety banner.

## 8. What this leaves for Phase 5

- Building the actual single-binary canary that exercises the IAM
  resolver matrix (env / IMDSv2 / IRSA / Pod Identity / Lambda /
  AssumeRole) — Phase 4.7 only proves the SDK path compiles.
- CloudWatch metric emission — Phase 5.
- Tagging the `gsr-client-canary-golang` CodeBuild project in the
  canary CDK — Phase 5 (still tracked in `PHASE-4-AWS-NOTES.md`).
