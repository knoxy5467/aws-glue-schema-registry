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
| 9 | `MultiThreadedIntegrationSuite` is the 3rd byte-identical copy of `getKafkaBroker` / `generateTestTopicName` | cleanup | DEFERRED | tracked below |
| 10 | `fakeglue.CallCounts` raw exported map invites race-prone direct reads | correctness | FIXED | `e7734ad` |
| 11 | `configFor`'s `gsrPath` parameter is dead | cleanup | FIXED | `e7734ad` |
| 12 | `extraAdapterCtors` two-mechanism registry (switch + map) is fragile | cleanup | DEFERRED | tracked below |
| 13 | `TestConcurrency_SharedInstancesDoNotRace` generates unique schemaNames → cache-fast-path untested | correctness | FIXED | `e7734ad` |
| 14 | BaseIntegrationSuite + MultiThreadedIntegrationSuite + matrix → 7 Kafka containers per run | cleanup | DEFERRED | tracked below |
| 15 | Plan §7 says "sarama + confluent are officially supported"; matrix defaults to sarama + segmentio | correctness | FIXED | `e7734ad` |

Plus 3 Phase 4.5 encoder bugs surfaced by the same review, already
closed in commits `dd37fc9`, `6092766`, `83f9f6d` (see
`PHASE-4.5-ENCODER-BUGS.md` for the per-bug detail):

- Bug 3 (`strings.Contains` for `AlreadyExistsException`) → `errors.As`.
- Bug 2 (encoder swallows non-EntityNotFound errors) → `errors.As(EntityNotFoundException)` gate.
- Bug 1 (`SchemaAutoRegistrationEnabled=false` ignored) → flag-gated.

## Deferred items (not bugs — refactor / scope)

Three findings are real cleanups but not correctness blockers. Each
costs a meaningful diff (>50 lines) and changes infrastructure
shared with the legacy suites; deferring lets Phase 5 do them as
part of its broader BaseIntegrationSuite migration.

### #9 — Three copies of getKafkaBroker / generateTestTopicName

`BaseIntegrationSuite.getKafkaBroker`,
`MultiThreadedIntegrationSuite.getKafkaBroker`, and
`scenario_helper_test.go`'s `scenarioTopicName` all carry near-
identical broker-resolution and topic-naming logic. The proper
fix is a package-level helper that both worlds call.

**Why deferred:** the Phase 4 review identified this alongside
plan §4 line 144's "migrate `BaseIntegrationSuite` off
testify/suite" mandate. The two cleanups go together — extracting
the helpers in isolation leaves the testify/suite migration
half-done. Phase 5 should do them in one PR.

### #12 — `extraAdapterCtors` two-mechanism registry

`adapterFor` hardcodes a switch for sarama/segmentio, then falls
through to `extraAdapterCtors` for build-tag-gated adapters.
Adding franz-go would require editing two places and there's no
compile-time check that a new adapter registered itself.

**Why deferred:** purely a refactor; current behavior is correct.
Phase 5 can collapse to one mechanism when it adds franz-go (or
when the matrix grows enough to justify the registry refactor on
its own).

### #14 — Seven Kafka containers per `make test-integ` run

BaseIntegrationSuite's SetupSuite is called by 5 suites (Avro,
Json, Protobuf, Sarama, ConfluentKafka), each spawning a fresh
testcontainers Kafka. MultiThreadedIntegrationSuite and
TestRoundTrip_Phase4Matrix add two more. With ~5-10s startup
per container, that's 35-70s of avoidable wall-clock.

**Why deferred:** a package-level `TestMain` that starts Kafka
once and exports `KAFKA_BROKER` for the suites needs to interact
with the testcontainers lifecycle in a way that's incompatible
with the legacy testify/suite pattern. Lump with the suite
migration (#9 deferred above).

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
