# PBI-04: Implement summary table, result aggregation, and exit-code logic

## Description

Implements the demo results summary section (spec 4.4): aggregates pass/fail results from all 6 scenario pairs, prints the formatted summary table, determines exit code (0 only if all 6 pass, non-zero otherwise), and prints any `[FAIL]` markers for failed scenarios. Also implements the `[CLEANUP]` narration that prints each deleted schema name. Implements spec sections 4.4, 9.1, 9.2 narration.

## Scope Boundary

Does NOT implement scenario execution logic (that is PBI-02 and PBI-03). Does NOT modify or create schemas in Glue (cleanup only deletes). Does NOT change the Makefile or README (that is PBI-05).

## Acceptance Criteria

- [ ] After all scenarios run, the demo prints a summary table matching spec section 4.4 format: format names in rows, Direction A and Direction B in columns, PASS/FAIL status per cell.
- [ ] The summary shows `Total: N/6 PASS` with actual count.
- [ ] If any scenario fails, the demo prints `[FAIL] <format> Direction <A|B>: <error>` before the summary table.
- [ ] Exit code is 0 only when all 6 pairs pass. Non-zero otherwise.
- [ ] The `[CLEANUP]` section narrates each deleted schema name (prefix-swept).
- [ ] The post-run verification reminder is printed (spec 9.4: the `aws glue list-schemas` command).
- [ ] `go build -tags integration ./cmd/demo-interop/` exits 0.

## Files Touched

- `integration-tests/cmd/demo-interop/main.go` (modified: add result aggregation, summary printing, exit-code logic)
- `integration-tests/cmd/demo-interop/narrator.go` (modified: add `printSummaryTable`, `printCleanupNarration`, `printPostRunReminder` helpers)

## Dependencies

- PBI-01
- PBI-03 (serializes all main.go modifications: PBI-01 creates, PBI-02 adds Direction A, PBI-03 adds Direction B, then PBI-04 adds aggregation)

## Size

S (under 100 LOC: ~80 LOC summary logic in main, ~80 LOC narrator helpers)

## Verification

```bash
cd integration-tests && go build -tags integration ./cmd/demo-interop/ && go vet -tags integration ./cmd/demo-interop/...
```

Build and vet pass. Summary table format matches spec section 4.4 (reviewable by reading the code).

## Implementor Notes

- Consumes `[]ScenarioResult` (defined in PBI-01's `scenarios.go`, populated by PBI-02 and PBI-03). Iterate over results to build the summary table.
- The cleanup narration wraps `realglue.Cleanup` callbacks. Check if `realglue` exposes a hook for per-schema deletion logging, or if the demo must list deleted schemas itself via `aws glue list-schemas` before and after.
- The summary table uses fixed-width columns. Match the spec 4.4 format exactly (column headers, separator line style).
- The post-run reminder is static text (spec 9.4). Print it after `[DONE]`.
- Exit logic: use `os.Exit(1)` after deferred cleanup if any scenario failed. The defer-based cleanup must run before `os.Exit` (handle by not calling `os.Exit` directly but by returning from main with a deferred os.Exit check).
