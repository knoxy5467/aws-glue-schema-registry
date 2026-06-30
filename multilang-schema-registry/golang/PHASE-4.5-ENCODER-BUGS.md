# Phase 4.5 — Pre-existing encoder bugs surfaced by Phase 4

**Status:** open, awaiting the project owner go-ahead.
**Date authored:** 2026-06-22.
**Scope owner:** Phase 4.5 (post-Phase-4, pre-Phase-5).

Phase 4's §5.3 integration tests pinned several pre-existing bugs in
`pkg/gsrserde-go/core/encoder.go`. The tests are currently inverted
(they assert the bug state) and document the gap with TODO comments;
this file is the consolidated punch list for the fixes.

The bugs were introduced before Phase 4 (commit `9a9d6cd` and the
Phase 1 rewrite) — they are **not** Phase 4 regressions. Phase 4 just
made them visible.

---

## Bug 1 — `SchemaAutoRegistrationEnabled=false` is ignored

### Reference
- Plan §5.3 item 14: *"Auto-register disabled + unknown schema → returns the documented error type without calling `CreateSchema`."*
- Plan §2.2 ("Stubbed or partial" list): the flag is on the missing-features list.

### Where it lives
- `pkg/gsrserde-go/core/encoder.go:47` — field declared on `GsrEncoder`.
- `pkg/gsrserde-go/core/encoder.go:81` — value set from `Config`.
- `pkg/gsrserde-go/core/encoder.go:194-241` — `fetchSchemaVersionID`
  never reads the field; on a `GetSchemaByDefinition` miss it
  unconditionally calls `s.createSchema(...)`.

### Sentinel already exists
- `pkg/gsrserde-go/core/errors.go:46` declares
  `ErrSchemaAutoRegistrationDisabled` — never returned anywhere.

### Test currently pinning the bug
- `integration-tests/tests/schema_lifecycle_test.go:65-100`
  (`TestLifecycle_AutoRegisterDisabled_UnknownSchemaErrors`) asserts
  `require.NoError(t, err)` and `CallCounts["CreateSchema"] == 1`.
  The test name and its TODO comment flag this as inverted.

### The fix
In `fetchSchemaVersionID`, after `GetSchemaByDefinition` returns
EntityNotFound (or returns no `SchemaVersionId`):
```
if !s.schemaAutoRegistrationEnabled {
    return nil, fmt.Errorf("%w: schema %q not registered", ErrSchemaAutoRegistrationDisabled, schemaName)
}
```
Then flip `TestLifecycle_AutoRegisterDisabled_UnknownSchemaErrors` to:
- `require.ErrorIs(t, err, gsrcore.ErrSchemaAutoRegistrationDisabled)`
- `require.Equal(t, 0, f.CallCounts["CreateSchema"])`

### Customer impact
A user explicitly setting `schemaAutoRegistrationEnabled=false` (a
compliance posture: producers must NOT mutate the registry) today
gets the opposite behavior — producers DO mutate the registry. Audit
fails; no log line, no error.

---

## Bug 2 — Encoder swallows non-EntityNotFound errors from GetSchemaByDefinition

### Reference
- Plan §5.3 item 23: *"IAM denied → returns a typed error wrapping the SDK's `AccessDeniedException`."*
- Plan §5.3 item 24: *"Throttling → retried per the SDK default retryer; surfaces only after exhausted retries."*
- Java reference: `AWSSchemaRegistryClient.java:151` falls through
  to `CreateSchema` **only** when `getSchemaByDefinition` raises
  `EntityNotFoundException`; every other exception type propagates up.

### Where it lives
- `pkg/gsrserde-go/core/encoder.go:197-218` — `fetchSchemaVersionID`
  calls `GetSchemaByDefinition`, success-fast-paths on
  `err == nil && getResp.SchemaVersionId != nil && Status == Available`,
  and otherwise falls through to `s.createSchema(...)` **regardless of
  the error type**. AccessDenied, Throttling, NetworkTimeout,
  ValidationException — all become CreateSchema attempts.

### Test currently pinning the bug
- `integration-tests/tests/negative_test.go:46-71`
  (`TestNegative_IAMDenied`) sets **both** `ForceGetSchemaError` and
  `ForceCreateError` to make the AccessDenied surface — exactly
  because the encoder swallows the first.
- `integration-tests/tests/negative_test.go:72-92`
  (`TestNegative_Throttling`) does the same dance.

### The fix
Type-check the GetSchemaByDefinition error. Only `EntityNotFoundException`
should fall through to the auto-register path. Anything else
propagates to the caller wrapped via `%w`.

```go
import "github.com/aws/aws-sdk-go-v2/service/glue/types"

getResp, err := s.client.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{...})
if err != nil {
    var enf *types.EntityNotFoundException
    if !errors.As(err, &enf) {
        return nil, fmt.Errorf("get schema by definition: %w", err)
    }
    // EntityNotFound: fall through to auto-register (subject to Bug 1's flag check)
}
if err == nil && getResp.SchemaVersionId != nil && getResp.Status == types.SchemaVersionStatusAvailable {
    ... // existing success path
}
```

Then drop the `ForceCreateError` lines from `TestNegative_IAMDenied`
and `TestNegative_Throttling`.

### Customer impact
- Read-only IAM (GetSchemaByDefinition denied, CreateSchema denied):
  the surfaced error claims CreateSchema failed when GetSchema was
  the real culprit — misdirects the customer to fix permissions they
  didn't need.
- ThrottlingException on GetSchemaByDefinition: encoder doubles the
  load against an already-throttled Glue by also attempting
  CreateSchema. SDK retry middleware budgets are halved.
- Partial-permission edge (read denied, write allowed): silent write
  amplification — every encode for an existing schema also creates a
  duplicate schema-version.

---

## Bug 3 — AlreadyExistsException recovery uses `strings.Contains`

### Reference
- Same Phase 4 review pass; surfaces alongside Bug 2.

### Where it lives
- `pkg/gsrserde-go/core/encoder.go:221` —
  `if strings.Contains(err.Error(), "AlreadyExistsException") || strings.Contains(err.Error(), "already exists")`

### The fix
`errors.As(err, &*types.AlreadyExistsException{})`. The SDK v2 surfaces
this as a typed error; string-matching is brittle to SDK version bumps
(format-prefix additions), to localized/translated messages, and to
middleware wrapping.

```go
var alreadyExists *types.AlreadyExistsException
if errors.As(err, &alreadyExists) {
    id, ver, regErr := s.registerSchemaVersion(...)
    ...
}
```

### Customer impact
A future AWS SDK release changes `Error()` formatting (it has done so
between minor versions before — e.g. adding `operation error Glue: `
prefix). The substring match might still work; or a localized message
drops the canonical name and the encoder surfaces a misleading
"failed to create schema" error instead of recovering via
RegisterSchemaVersion.

---

## Sequencing and risk

These three changes touch one file (`encoder.go`) and the affected
tests (`schema_lifecycle_test.go`, `negative_test.go`). No public API
changes; no behavioral changes that a correctly-configured caller
would notice — only error paths flip from "wrong" to "right". Bug 1
fixes a customer-visible behavioral change (auto-register=false now
actually does what the docs say); Bugs 2 and 3 fix error-typing /
error-routing under existing failure modes.

Recommended order:
1. **Bug 3 first** (smallest, lowest risk, anchors `errors.As` import).
2. **Bug 2** (depends on Bug 3's `errors.As` import; touches the same
   `fetchSchemaVersionID` function).
3. **Bug 1** (adds the new gate; sits inside the EntityNotFound branch
   Bug 2 introduced).

Each gets its own commit. Per-bug commit message should:
- Reference this doc.
- Reference the §5.3 item it un-inverts.
- Quote the Java parity line that proves the fix is correct.

---

## What this doc is NOT

- Not Phase 5. Canary harness is its own scope.
- Not a fix for the broader §2.2 "Stubbed or partial" list. Only the
  three bugs Phase 4's tests surfaced as inverted assertions are in
  scope here.
- Not a rewrite of `BaseIntegrationSuite` off testify/suite (plan §4
  line 144). That migration is tracked separately; see plan §9 risk
  row.

---

## Open question for the project owner

Phase 4.5 fits in the gap between Phase 4 (just landed locally) and
Phase 5 (canary harness — gated on the PHASE-4-AWS-NOTES.md
resolution). Should:

- (a) Phase 4.5 land before Phase 5 starts (~1 day's work, the three
  bugs are small and have ready-to-flip tests waiting)?
- (b) Or roll Phase 4.5 into Phase 5's setup work, since the canary
  will exercise these paths in production anyway?

Recommend (a) — landing the fixes before canary keeps the canary's
first failures pointing at infra issues (IAM, AWS account, OIDC)
rather than orchestrator bugs.
