# Phase 4 review closeout — 15 findings, resolved

**Status:** closed.
**Date:** 2026-06-22.

The high-effort `/code-review` pass over commits `8b6fd6a..HEAD`
surfaced 15 findings. This doc maps each to its resolution.

| # | Finding | Severity | Status | Commit |
|---|---------|----------|--------|--------|
| 1 | Vacuous `TestCompatibility_BackwardAll_ThreeVersions` (cache key hides v2/v3) | correctness | FIXED | `e7734ad` |
| 1b | Same bug in `TestCompatibility_FullBothDirections` | correctness | FIXED | `e7734ad` |
| 2 | `=` in scenario topic names → InvalidTopicException at broker | correctness | FIXED | `e7734ad` |
| 3 | confluent Produce `min(30s, ctx)` cap overrides caller's longer ctx | correctness | FIXED | `e7734ad` |
| 4 | fmtJSON{JDWS,POJO} + fmtProtobuf{Static,Dynamic} share identical code paths | correctness | FIXED | `e7734ad` |
| 5 | `TestNegative_NonUTF8Path` wraps `require.Equal` inside `if err == nil` → vacuous on error | correctness | FIXED | `e7734ad` |
| 6 | fakeglue `RegisterSchemaVersion` stores `DataFormat=""` → silent protobuf corruption | correctness | FIXED | `e7734ad` |
| 7 | sarama `Produce` defer-Close races in-flight `SendMessage` on ctx-cancel | correctness | FIXED | `e7734ad` |
| 8 | segmentio + confluent `GroupID = UnixNano` → coarse-clock collisions | correctness | FIXED | `e7734ad` |
| 9 | `MultiThreadedIntegrationSuite` is the 3rd byte-identical copy of `getKafkaBroker` / `generateTestTopicName` | cleanup | FIXED | (post-`e7734ad`) |
| 10 | `fakeglue.CallCounts` raw exported map invites race-prone direct reads | correctness | FIXED | `e7734ad` |
| 11 | `configFor`'s `gsrPath` parameter is dead | cleanup | FIXED | `e7734ad` |
| 12 | `extraAdapterCtors` two-mechanism registry (switch + map) is fragile | cleanup | FIXED | (post-`e7734ad`) |
| 13 | `TestConcurrency_SharedInstancesDoNotRace` generates unique schemaNames → cache-fast-path untested | correctness | FIXED | `e7734ad` |
| 14 | BaseIntegrationSuite + MultiThreadedIntegrationSuite + matrix → 7 Kafka containers per run | cleanup | FIXED | (post-`e7734ad`) |
| 15 | Plan §7 says "sarama + confluent are officially supported"; matrix defaults to sarama + segmentio | correctness | FIXED | `e7734ad` |

Plus 3 Phase 4.5 encoder bugs surfaced by the same review, already
closed in commits `dd37fc9`, `6092766`, `83f9f6d` (see
`PHASE-4.5-ENCODER-BUGS.md` for the per-bug detail):

- Bug 3 (`strings.Contains` for `AlreadyExistsException`) → `errors.As`.
- Bug 2 (encoder swallows non-EntityNotFound errors) → `errors.As(EntityNotFoundException)` gate.
- Bug 1 (`SchemaAutoRegistrationEnabled=false` ignored) → flag-gated.

## Resolution of the formerly-deferred items

The earlier draft of this doc deferred #9, #12, and #14 to Phase 5.
Closing them is now part of Phase 4's deliverable per the
`/goal fix all the bugs` directive. Summary of how each landed:

### #9 — getKafkaBroker / generateTestTopicName lifted to package-level helpers

`scenario_helper_test.go` now exposes:

- `resolveKafkaBroker(broker *kafkaharness.Broker) string` — single
  source of truth for broker precedence (harness > KAFKA_BROKER
  env > defaultKafkaBroker).
- `newRandomTopicName(t testing.TB, prefix string) string` — single
  source of truth for topic-name generation; sanitizes t.Name() and
  appends 4 random hex bytes.

`BaseIntegrationSuite` and `MultiThreadedIntegrationSuite` both
delegate via one-line methods. `base_integration_suite.go` was
renamed to `_test.go` so it can reference the test-package helpers.
MultiThreaded's previous topic-name shape dropped `t.Name()`
entirely; the shared helper fixes that latent collision risk.

### #12 — One adapter registry, init()-driven

Collapsed `adapterFor`'s switch + `extraAdapterCtors` map into one
package-level `adapterCtors` map populated by per-adapter
`init()` in `tests/round_trip_{sarama,segmentio}_register_test.go`
and (build-tag gated) `tests/round_trip_confluent_test.go`.
`registerAdapter` panics on duplicate registration so copy-paste
errors surface at first invocation. `allRoundTripScenarios` sorts
the registry's keys for deterministic t.Run names. Adding franz-go
is one new file under tests/ — no switch to edit.

### #14 — One Kafka container per `go test` invocation

`kafkaharness.StartShared` is the TestMain-friendly variant of
Start: returns `(broker, stop, error)` instead of registering
`t.Cleanup`. `tests/main_test.go` calls it once, sets
`KAFKA_BROKER` to the bootstrap address, and defers stop. Every
SetupSuite's `kafkaharness.Start` short-circuits on the env var
and reuses the shared broker.

Skipped when no AWS_INTEGRATION test will actually run
(`AWS_INTEGRATION!=1 AND no KAFKA_BROKER`) so wire-format-only
runs don't pay container-startup cost.

## Verification at closeout

- `go test -short -count=1 ./...` — green in outer module.
- `go test -short -count=1 ./...` — green in inner `pkg/gsrserde-go/core/`.
- `go vet -tags integration ./...` — green in integration-tests.
- `go test -tags integration -count=1 ./...` — green (96s wall clock).
- `go build -tags 'integration confluent' ./...` — green.
- `make test-integ-confluent` — new Makefile target; runs the
  confluent leg of the matrix per plan §7's "officially supported
  clients" rule.

## Phase 4 final commit list (since 8b6fd6a)

```
<head>  Phase 4 closeout part 2: close the 3 deferred review items (#9, #12, #14)
81eeb99 Phase 4 closeout doc: 15 review findings mapped to commits
e7734ad Phase 4 closeout: fix remaining review-flagged bugs
83f9f6d Phase 4.5 bug 1: honor SchemaAutoRegistrationEnabled=false
6092766 Phase 4.5 bug 2: only fall through to CreateSchema on EntityNotFoundException
dd37fc9 Phase 4.5 bug 3: errors.As typed-error match for AlreadyExistsException recovery
7accdc7 Phase 4.5: open the pre-existing encoder bugs
3f121b4 Phase 4 review fix: tests that asserted nothing + vacuous-matrix bugs
f046048 Phase 4 review fix: adapter cancellation correctness
be10ba0 Phase 4 item 6: PHASE-4-AWS-NOTES.md (canary account research)
f9d9648 Phase 4 item 5: Makefile + dockerized runner align with §5.3 + AWS gates
c0b791a Phase 4 item 4: §5.3 30+ scenarios
837e696 Phase 4 item 3: Java golden-byte fixture scaffolding
7ae7adb Phase 4 item 2: Kafka client adapter packages
5284aa3 Phase 4 item 1: testcontainers-go Kafka harness
```
