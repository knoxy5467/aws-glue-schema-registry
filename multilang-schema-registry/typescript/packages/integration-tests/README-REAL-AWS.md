# Real-AWS-Glue integration test runbook (TypeScript)

This is the operator runbook for `npm run test:integ:real`. It runs the
Tier-3 integration suite against **real AWS Glue Schema Registry** in a
non-production AWS account you control. Use it when you want to prove
the path end-to-end against the actual service — for a release tag,
after touching the `@gsr/core` Glue seam, or whenever you want to
confirm the real-AWS code path compiles and resolves credentials.

Ported from the reference Go client's real-AWS runbook. The two runbooks
track each other beat-for-beat; the differences are runtime (Node vs
Go), test runner (vitest vs `go test`), and the schema-name prefix
(`gsr-ts-it-` vs `gsr-go-it-`).

## Prereqs

1. **AWS account credentials** for a non-production account you control,
   resolvable through one of:
   - `AWS_PROFILE=<your-profile>` with a usable `~/.aws/config`
     entry (recommended — supports SSO + role chains).
   - `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (and
     `AWS_SESSION_TOKEN` if temporary).

   The `test:integ:real` script hard-fails if NEITHER `AWS_PROFILE`
   nor `AWS_ACCESS_KEY_ID` is set, so a typo doesn't reach AWS. The
   script exits non-zero before the 5-second abort banner prints.

2. **Region.** Defaults to `us-east-2`. Override via
   `AWS_REGION=us-west-2 npm run test:integ:real`.

3. **IAM permissions.** The role/user must have at minimum:

   ```
   glue:CreateSchema
   glue:RegisterSchemaVersion
   glue:GetSchemaByDefinition
   glue:GetSchemaVersion
   glue:DeleteSchema
   glue:ListSchemas                (for the post-run leak-check below)
   glue:PutSchemaVersionMetadata
   glue:QuerySchemaVersionMetadata
   glue:GetTags
   ```

   All operations scoped to the region in step 2. Note: the suite
   does NOT call `glue:CreateRegistry` or `glue:DeleteRegistry` —
   every test schema lives inside the pre-existing `default-registry`
   (see step 4) and only the schemas are torn down by the
   `CleanupTracker`.

4. **`default-registry` exists.** The suite uses Glue's
   `default-registry` for every schema. If your account does not
   already have it, Glue will auto-create it on the first
   `CreateSchema` call so no manual bootstrap is required. To confirm
   it is present up front:

   ```
   aws glue get-registry \
     --registry-id RegistryName=default-registry \
     --region <r>
   ```

   Or create it once by hand (idempotent):

   ```
   aws glue create-registry \
     --registry-name default-registry \
     --region <r>
   ```

5. **Docker (optional).** The Tier-3 real-Glue scenarios in this
   package do not require Docker — they only touch Glue's control
   plane. If you also want to run the Kafka-round-trip Tier-2 scenarios
   in the same invocation, they need Docker for `testcontainers`; skip
   them by leaving `KAFKA_BROKER` unset and only the Glue scenarios
   run (Kafka-required scenarios loud-skip with a one-line reason).

## Invocation

```
cd multilang-schema-registry/typescript
AWS_PROFILE=<your-profile> AWS_REGION=us-east-2 npm run test:integ:real
```

The script will:

1. Verify creds env vars are set. If neither `AWS_PROFILE` nor
   `AWS_ACCESS_KEY_ID` is set, print an error and exit non-zero
   (no AWS call made).
2. Print an `About to bill real AWS Glue in <region>. Press Ctrl-C
   within 5s to abort.` banner and `sleep 5`.
3. Run:
   ```
   GSR_GLUE=real AWS_INTEGRATION=1 \
     vitest run --config vitest.integration.config.ts
   ```
4. On exit, the `CleanupTracker` deletes every schema the run created
   (in reverse insertion order, tolerating `EntityNotFoundException`).

## Expected cost

Each Tier-2 fake-only scenario (the bulk of the suite) skips loudly
under `GSR_GLUE=real`. Today the `GSR_GLUE=real` path exercises the
Tier-3 real-Glue scenarios in
`packages/integration-tests/test/tier3/`, which is currently:

- **1** round-trip scenario (`real-glue-roundtrip.integ.test.ts`).
  On real Glue, the encoder does:
  - `GetSchemaByDefinition` (miss on first-run) → 1 call.
  - `CreateSchema` (accepted) → 1 call.
  - `GetSchemaVersion` polling (typically 1 call, up to 10 on eventual
    consistency) → 1–10 calls.
  - Metadata flush (non-fatal): `PutSchemaVersionMetadata` → 1 call.
  - Second call for the same schema+definition: cache hit, **0**
    additional Glue calls.
- Cleanup pass: `DeleteSchema` per tracked schema → **1 call**.

Total Glue control-plane calls per `npm run test:integ:real` run:
**roughly 4–13** (typically 4 with fast eventual-consistency, up to
13 if the poll loop takes its full budget). Re-estimate when the
Tier-3 matrix grows.

At Glue Schema Registry public pricing (free for the first ~1M
ops/month at the time of writing) this is well inside the free tier
even at hundreds of runs/day.

## Post-run cleanup verification

The `CleanupTracker` wires cleanup into vitest's `afterAll`, so the
suite deletes every schema it created in reverse insertion order —
even on test failure (`afterAll` fires regardless). The suite does
NOT create or delete registries; every test schema lives inside the
pre-existing `default-registry`.

To sanity-check that nothing leaked, list schemas in the registry the
suite uses and grep for the runbook prefix:

```
aws glue list-schemas \
  --registry-id RegistryName=default-registry \
  --region <r> \
  --query 'Schemas[?starts_with(SchemaName, `gsr-ts-it-`)].SchemaName' \
  --output text
```

The expected output is **empty**. Any name printed is a leaked
schema. Delete it by hand:

```
aws glue delete-schema \
  --schema-id RegistryName=default-registry,SchemaName=<leaked-name> \
  --region <r>
```

Do NOT run `delete-registry` against `default-registry` — that
registry is shared infrastructure and the suite expects it to exist
on every run.

## Troubleshooting

- **`test:integ:real: refusing to run — neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set`**
  — the script hard-fails before touching AWS. Set one of the two
  env vars (see Prereq 1). Do not modify the script to skip this
  check.
- **`GSR_GLUE=real requires AWS credentials`** — the runtime
  `requireRealCreds()` guard fired inside `beforeAll`. Same fix as
  above; this is the belt-and-suspenders check that runs after the
  shell-level guard.
- **`ThrottlingException`** — back off and retry. Glue throttles
  `CreateSchema` at a relatively low rate. Reduce parallelism if you
  add more Tier-3 scenarios; vitest runs test files in parallel by
  default (`--pool=forks`), which is why per-run namespacing is
  mandatory.
- **`ExpiredTokenException`** — the SSO session expired. Re-run
  `aws sso login --profile <p>` and try again.
- **Suite skips with `SKIP tier3/real-glue-roundtrip — AWS_INTEGRATION!=1`**
  — that is correct behavior when `AWS_INTEGRATION` is unset (the
  `describeIntegration` gate). The `test:integ:real` script exports
  `AWS_INTEGRATION=1` for you; if you see this line under
  `test:integ:real`, the env export chain broke.
- **Pre-existing `gsr-ts-it-*` leaks** from an earlier crashed run —
  clean those up before invoking; otherwise the new run's
  `CreateSchema` calls succeed but the count of schemas in your
  account grows.

## When to use this vs. `npm run test:integ`

- `npm run test:integ` — fake-backend path. Bills nothing. Run on
  every commit, in PR gates, locally.
- `npm run test:integ:real` — real-Glue path. Bills AWS. Run before
  a release tag, after touching `@gsr/core`'s Glue client seam, or
  whenever you want to prove the real-AWS code path compiles and
  resolves credentials.
