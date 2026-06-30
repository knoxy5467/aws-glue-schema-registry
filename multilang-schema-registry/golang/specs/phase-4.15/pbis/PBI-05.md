# PBI-05: Add Makefile target and README

## Description

Adds the `demo-interop` target to the existing Makefile (spec 8.3) and creates `integration-tests/cmd/demo-interop/README.md` with build/run prerequisites, invocation commands, and environment variable reference. Implements spec sections 8.1-8.3.

## Scope Boundary

Does NOT modify any Go source files in `cmd/demo-interop/` (that is PBI-01 through PBI-04). Does NOT execute the demo or validate its output (that is PBI-06). Only touches the Makefile and a new README file.

## Acceptance Criteria

- [ ] `make demo-interop` target exists in the Makefile at `multilang-schema-registry/golang/Makefile`.
- [ ] The target depends on `java-sidecar-build` (prerequisite).
- [ ] The target checks for AWS credentials (either `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`) and exits with an error message if neither is set.
- [ ] The target runs `cd integration-tests && $(GO) run ./cmd/demo-interop/`.
- [ ] `integration-tests/cmd/demo-interop/README.md` exists with: prerequisites (AWS creds, Go 1.21+, JDK 11+, Docker, sidecar JAR, `default-registry`), environment variables table (spec 6.1), invocation examples (`make demo-interop`, manual `go run`, transcript capture via `tee`).
- [ ] `make -n demo-interop` (dry run) succeeds without errors from the Makefile at `multilang-schema-registry/golang/`.

## Files Touched

- `Makefile` (modified: add `demo-interop` target)
- `integration-tests/cmd/demo-interop/README.md` (new)

## Dependencies

- PBI-01 (the target references `cmd/demo-interop/` which must exist)

## Size

S (under 100 LOC: ~15 lines Makefile target, ~60 lines README)

## Verification

```bash
cd multilang-schema-registry/golang && make -n demo-interop
```

Dry-run exits 0. README content is reviewable for completeness against spec 8.1.

## Implementor Notes

- The Makefile target format is prescribed in spec 8.3. Match it closely (the `.PHONY`, the credential check, the `@echo`, the `cd integration-tests`).
- The target name is `demo-interop` (not `demo` as mentioned in one place in room memory; the spec uses `demo-interop`).
- README should reference `REAL-AWS-RUNBOOK.md` for `default-registry` setup.
- Keep README concise: one-screen maximum. The demo is for developers on a developer machine, not end-users.
