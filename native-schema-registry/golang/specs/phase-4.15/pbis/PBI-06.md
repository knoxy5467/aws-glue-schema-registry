# PBI-06: End-to-end validation: execute demo against real AWS and capture transcript

## Description

Executes the completed demo binary against real AWS (account `850995546034`, region `us-east-2`), captures the full stdout transcript to `demo.log`, verifies all 6 cross-pairs pass, verifies cleanup leaves no leaked schemas, and attaches the log to the IMPL_RESULT. This is the binding deliverable that proves Phase 4.15 is complete. Implements spec sections 9.1, 9.4, and Definition of Done (section 11).

## Acceptance Criteria

- [ ] `make demo-interop 2>&1 | tee demo.log` exits 0.
- [ ] `demo.log` contains narration for all 6 cross-pairs (3 formats x 2 directions), each showing PASS.
- [ ] `demo.log` contains the summary table with `Total: 6/6 PASS`.
- [ ] Each narrated section includes: schema body, Glue version-id, Go config, wire-byte hex dump with decomposition, decoded values, and equality check.
- [ ] `aws glue list-schemas --registry-id RegistryName=default-registry --region us-east-2 --query 'Schemas[?starts_with(SchemaName, \`demo-4.15-\`)].SchemaName' --output text` returns empty after the demo completes.
- [ ] `demo.log` is attached to the IMPL_RESULT message (or its content is pasted in full if attachment is not supported).
- [ ] Demo completes in under 5 minutes.

## Files Touched

- No source files modified. This PBI validates existing code.
- `demo.log` (new, ephemeral artifact: captured demo transcript)

## Dependencies

- PBI-01
- PBI-02
- PBI-03
- PBI-04
- PBI-05

## Size

S (under 100 LOC: no code changes; execution + verification steps only)

## Verification

Real-AWS execution:
```bash
cd native-schema-registry/golang && make demo-interop 2>&1 | tee demo.log
echo "Exit code: $?"
aws glue list-schemas --registry-id RegistryName=default-registry --region us-east-2 \
  --query 'Schemas[?starts_with(SchemaName, `demo-4.15-`)].SchemaName' --output text
```

Exit code 0, transcript shows 6/6 PASS, no leaked schemas.

## Implementor Notes

- This PBI is a validation step, not a code-writing step. The implementor runs the demo, captures output, and reports the result.
- If any scenario fails, the implementor should report the failure in IMPL_RESULT with the relevant log segment so the build-lead can route fixes back to the appropriate PBI.
- Ensure Docker is running, JDK is on PATH, AWS creds are set for account `850995546034`, and `default-registry` exists before running.
- The 5-minute budget excludes first-time Docker image pulls.
