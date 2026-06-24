# Phase 4.12 — PBI-4.12-9 Regression Verification Log

## Verified at HEAD `153c474` (phase-4.12 branch)
## Date: 2026-06-24

This log documents the regression verification per spec §3.5/§3.6/§3.11/§3.12.
No new tests, no production code changes. Pre-existing tests must stay green.

---

## Item 22 — Incompatible change rejected at CreateSchema (BACKWARD)

- **Spec**: §3.5
- **Tier-1 test**: `TestEncoder_CompatibilityRejection_SurfacesTypedError`
- **Tier-1 location**: `pkg/gsrserde-go/core/glue_negatives_test.go:189`
- **Tier-2 test**: `TestCompatibility_IncompatibleRejected`
- **Tier-2 location**: `integration-tests/tests/compatibility_test.go:190`
- **Tier-2 _Real companion**: `TestCompatibility_IncompatibleRejected_Real` (line 225) — skipped under `GSR_GLUE=fake` per design
- **Run (Tier-1)**: `cd pkg/gsrserde-go/core && go test ./... -run TestEncoder_CompatibilityRejection_SurfacesTypedError -v`
- **Run (Tier-2)**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestCompatibility_IncompatibleRejected -v`
- **Result**: PASS
- **Output**:
  ```
  === RUN   TestEncoder_CompatibilityRejection_SurfacesTypedError
  --- PASS: TestEncoder_CompatibilityRejection_SurfacesTypedError (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core	0.015s

  --- PASS: TestCompatibility_IncompatibleRejected (0.00s)
  --- SKIP: TestCompatibility_IncompatibleRejected_Real (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/tests	0.113s
  ```
- **Observations**: `_Real` companion correctly skips under `GSR_GLUE=fake`; per spec §6 and PBI-4.12-9, real-mode pass is not a DoD requirement.

---

## Item 23 — IAM denied (typed error wrapping AccessDeniedException)

- **Spec**: §3.6
- **Tier-1 test**: `TestEncoder_IAMDenied_PreservesTypedSDKError`
- **Tier-1 location**: `pkg/gsrserde-go/core/glue_negatives_test.go:110`
- **Tier-2 test**: `TestNegative_IAMDenied`
- **Tier-2 location**: `integration-tests/tests/negative_test.go:54`
- **Run (Tier-1)**: `cd pkg/gsrserde-go/core && go test ./... -run TestEncoder_IAMDenied_PreservesTypedSDKError -v`
- **Run (Tier-2)**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestNegative_IAMDenied -v`
- **Result**: PASS
- **Output**:
  ```
  === RUN   TestEncoder_IAMDenied_PreservesTypedSDKError
  --- PASS: TestEncoder_IAMDenied_PreservesTypedSDKError (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core	0.015s

  --- PASS: TestNegative_IAMDenied (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/tests	0.118s
  ```
- **Observations**: None.

---

## Item 28 — Truncated payload <18 bytes (ErrIncompatibleData)

- **Spec**: §3.11
- **Tier-1 test**: `TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData`
- **Tier-1 location**: `pkg/gsrserde-go/core/payload_negatives_test.go:32`
- **Tier-2 test**: `TestNegative_TruncatedPayload`
- **Tier-2 location**: `integration-tests/tests/negative_test.go:274`
- **Run (Tier-1)**: `cd pkg/gsrserde-go/core && go test ./... -run TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData -v`
- **Run (Tier-2)**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestNegative_TruncatedPayload -v`
- **Result**: PASS
- **Output**:
  ```
  === RUN   TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData
  --- PASS: TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core	0.015s

  --- PASS: TestNegative_TruncatedPayload (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/tests	0.117s
  ```
- **Observations**: None.

---

## Item 29 — Header version 0x03 + corrupt UUID (typed error)

- **Spec**: §3.12
- **Tier-1 test**: `TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError`
- **Tier-1 location**: `pkg/gsrserde-go/core/payload_negatives_test.go:60`
- **Tier-2 test**: `TestNegative_UnknownVersionUUID`
- **Tier-2 location**: `integration-tests/tests/negative_test.go:305`
- **Run (Tier-1)**: `cd pkg/gsrserde-go/core && go test ./... -run TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError -v`
- **Run (Tier-2)**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestNegative_UnknownVersionUUID -v`
- **Result**: PASS
- **Output**:
  ```
  === RUN   TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError
  --- PASS: TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core	0.015s

  --- PASS: TestNegative_UnknownVersionUUID (0.00s)
  ok  	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/tests	0.117s
  ```
- **Observations**: None.

---

## GR-1 (Compatibility suite) — Full run

- **Run**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestCompatibility -v`
- **Result**: PASS (5 PASS, 5 SKIP for `_Real` variants gated on `AWS_INTEGRATION`)
- **Summary**:
  ```
  --- PASS: TestCompatibility_BackwardV1ToV2 (0.00s)
  --- PASS: TestCompatibility_BackwardAll_ThreeVersions (0.00s)
  --- PASS: TestCompatibility_ForwardV2ToV1 (0.00s)
  --- PASS: TestCompatibility_FullBothDirections (0.00s)
  --- PASS: TestCompatibility_IncompatibleRejected (0.00s)
  --- SKIP: TestCompatibility_IncompatibleRejected_Real (0.00s)
  --- SKIP: TestCompatibility_BackwardV1ToV2_Real (0.00s)
  --- SKIP: TestCompatibility_BackwardAll_ThreeVersions_Real (0.00s)
  --- SKIP: TestCompatibility_ForwardV2ToV1_Real (0.00s)
  --- SKIP: TestCompatibility_FullBothDirections_Real (0.00s)
  ok  integration-tests/tests 0.113s
  ```

## GR-2 (Negative suite) — Full run

- **Run**: `cd integration-tests && GSR_GLUE=fake go test -tags integration ./tests/... -run TestNegative -v`
- **Result**: PASS (all 7 tests)
- **Summary**:
  ```
  --- PASS: TestNegative_IAMDenied (0.00s)
  --- PASS: TestNegative_Throttling (0.00s)
  --- PASS: TestNegative_EntityNotFoundFallsThroughToCreate (0.00s)
  --- PASS: TestNegative_MalformedDecodePayload (0.00s)
  --- PASS: TestNegative_NonUTF8Path (0.00s)
  --- PASS: TestNegative_TruncatedPayload (0.00s)
  --- PASS: TestNegative_UnknownVersionUUID (0.00s)
  ok  integration-tests/tests 0.117s
  ```

## GR-3 (Tier-1 core suite) — Full run

- **Run**: `cd pkg/gsrserde-go/core && go test ./...`
- **Result**: PASS
- **Summary**: `ok  github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core`

## GR-7 (PHASE-4.9-AUDIT.md not modified)

- **Run**: `git diff golang-mrknox..phase-4.12 -- native-schema-registry/golang/PHASE-4.9-AUDIT.md`
- **Result**: empty (0 bytes) — PASS. Audit file is untouched.

## Interop tests — pre-existing skip

- `TestInterop_GoEncode_JavaDecode` and `TestInterop_JavaEncode_GoDecode` fail because `java-interop/target/java-interop-sidecar.jar` is not built in this worktree. This is a pre-existing environment constraint (JAR requires Maven build), unrelated to items 22/23/28/29. These tests are not in GR-1 through GR-7 scope for this PBI.

---

## Summary

4 of 4 items verified PASS at HEAD `153c474`. No regressions. No code changes required.

| Item | Tier-1 Test | Tier-2 Test | Result |
|------|-------------|-------------|--------|
| 22 — Incompatible rejected | `TestEncoder_CompatibilityRejection_SurfacesTypedError` | `TestCompatibility_IncompatibleRejected` | ✅ PASS |
| 23 — IAM denied | `TestEncoder_IAMDenied_PreservesTypedSDKError` | `TestNegative_IAMDenied` | ✅ PASS |
| 28 — Truncated payload | `TestGsrDecoder_TruncatedPayload_SurfacesIncompatibleData` | `TestNegative_TruncatedPayload` | ✅ PASS |
| 29 — Corrupt UUID | `TestGsrDecoder_ValidHeader_UnknownUUID_SurfacesTypedError` | `TestNegative_UnknownVersionUUID` | ✅ PASS |
