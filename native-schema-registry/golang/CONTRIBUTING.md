# Contributing

Thanks for working on the Go port of the AWS Glue Schema Registry client.
Local-machine setup lives in [`DEVELOPMENT.md`](./DEVELOPMENT.md); this file
covers conventions for changes you propose against this directory.

## Pull requests

- One concern per PR. Phase 0 deliverables (build-tag gate, docs, the
  protobuf off-by-one fix, branch alignment) ship as separate PRs; the same
  rule of thumb applies after Phase 0.
- Branch off `golang-mrknox` (or, once it lands upstream, `native-schema-registry-golang`).
- Use a descriptive branch name like `phase<N>/<short-slug>` while we're in
  Phase 0–2.
- The PR body should state **what** changed, **why**, and how you **verified**
  it. Paste the `go test` output (with the right tags) for the surface you
  touched. If you can't run a step, say so explicitly.

## Commit messages

- First line ≤ 72 chars, imperative ("Add X", not "Added X" / "Adds X").
- Wrap the body at ~72 chars.
- Reference the failing test or test set the change makes pass — TDD is the
  discipline this project is being rebuilt under.

## Code style

- Run `gofmt` / `goimports` before committing. CI will fail otherwise.
- `golangci-lint` is in the loop for new code; the rule set is the one
  already used elsewhere in the Brazil Go ecosystem (no project-specific
  overrides yet).
- Keep public-API doc comments in proper Go form (`// Foo does X.` starting
  with the identifier name).

## Tests

### Two tiers

| Tier   | Location                            | Build tag      | Env var                |
| ------ | ----------------------------------- | -------------- | ---------------------- |
| Unit   | `_test.go` next to production code  | none           | none                   |
| Integ. | `integration-tests/tests/`          | `integration`  | `AWS_INTEGRATION=1`    |

A default `go test ./...` must run zero integration tests and never bill
AWS. Every file under `integration-tests/tests/` carries a
`//go:build integration` tag at the top of the file; new IT files must do
the same. The runtime guard inside each test
(`s.shouldSkipIntegrationTests()`) checks `AWS_INTEGRATION=1` as a
belt-and-suspenders second gate.

### TDD

Every behavior-change PR should reference the failing test it makes pass.
For wire-format work, the test must include the exact byte layout being
asserted; for cache work, it must exercise the eviction trigger; for the
Glue client, it must drive the mock.

## No CGO

The Go port is being rewritten as a **pure-Go** library. New code under
`pkg/gsrserde-go/` must not introduce `import "C"`. The legacy CGO bridge
(`serializer.go`, `deserializer.go`, `schema.go`, `helpers.go`,
`cgo_flags.go`, the empty `lib/` directory) is scheduled for deletion once
the pure-Go core lands; please don't extend it.

## Reviewer

Code review on this directory is human-only for now. Don't dispatch
automated CR comment bots and don't reply to bot comments without
maintainer confirmation.
