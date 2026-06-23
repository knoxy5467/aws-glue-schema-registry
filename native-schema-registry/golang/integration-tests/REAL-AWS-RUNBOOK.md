# Real-AWS-Glue integration test runbook

This is the operator runbook for `make test-integ-real`. It runs the
§5.3 integration suite against **real AWS Glue Schema Registry** in
your beta account. Use it once Phase 4.7 lands and you want to prove
the path end-to-end against the actual service.

## Prereqs

1. **Beta AWS account credentials** resolvable through one of:
   - `AWS_PROFILE=<your-beta-profile>` with a usable `~/.aws/config`
     entry (recommended — supports SSO + role chains).
   - `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (and
     `AWS_SESSION_TOKEN` if temporary).
   - `mwinit`-managed Conduit-issued creds, if your dev host is set
     up that way.

   The Makefile target hard-fails if NEITHER `AWS_PROFILE` nor
   `AWS_ACCESS_KEY_ID` is set, so a typo doesn't reach AWS.

2. **Region.** Defaults to `us-east-2` (plan §6.3 anchor — matches
   the existing Java / C# canary account). Override via
   `AWS_REGION=us-west-2 make test-integ-real`.

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
   (see step 4) and only the schemas are torn down by
   `realglue.Cleanup`. The earlier draft of this runbook listed
   those permissions; they are NOT required.

4. **`default-registry` exists.** The suite uses Glue's
   `default-registry` for every schema. If your account does not
   already have it (`aws glue get-registry --registry-id RegistryName=default-registry`),
   create it once:

   ```
   aws glue create-registry --registry-name default-registry --region <r>
   ```

5. **Docker** — `make test-integ-real` keeps the testcontainers-go
   Kafka layer the standard `make test-integ` uses. If you want to
   skip Kafka entirely (e.g. you only care about the non-Kafka
   scenarios), set `KAFKA_BROKER=` to suppress the
   testcontainers-go startup; scenarios that require Kafka will then
   t.Skip rather than fail.

## Invocation

```
cd native-schema-registry/golang
AWS_PROFILE=<your-beta> AWS_REGION=us-east-2 make test-integ-real
```

The recipe will:
1. Verify creds env vars are set.
2. Print a "About to bill real AWS Glue in <region>. Press Ctrl-C
   within 5s to abort." banner and `sleep 5`.
3. Run:
   ```
   cd integration-tests && GSR_GLUE=real AWS_INTEGRATION=1 \
     go test -tags integration -count=1 -timeout 30m ./...
   ```
4. Print the cleanup verification command after the run.

## Expected cost

Each `RequiresFakeGlue=true` scenario (the bulk of the §5.3 suite,
because they assert on fakeglue `Force*Error` / `Count` affordances)
skips under `GSR_GLUE=real`. Today the `GSR_GLUE=real` path
exercises roughly:

- **6** wire-format direct scenarios (no Glue at all).
- **2** compatibility round-trips that hit real Glue end-to-end:
  - `TestCompatibility_BackwardV1ToV2` — registers v1 then a
    backward-compatible v2 (≈ 2 × `CreateSchema/RegisterSchemaVersion`
    + 2 × `GetSchemaByDefinition` + 2 × `GetSchemaVersion`).
  - `TestCompatibility_ForwardV2ToV1` — registers v2 then v1 under
    `Compatibility=FORWARD` (same shape).
- **1** `RequiresRealGlue=true` companion
  (`TestCompatibility_IncompatibleRejected_Real`) — registers v1,
  attempts an incompatible v2 the server must reject (≈ 1 successful
  CreateSchema + 1 rejected attempt).
- **1** negative decode that reaches Glue
  (`TestNegative_UnknownVersionUUID` → 1 `GetSchemaVersion`).

Total fresh Glue control-plane calls per `make test-integ-real` run:
**roughly 15-20**.

Phase 4.8 nit #8: this estimate is tighter than Phase 4.7's earlier
"<50" figure because the gate review reclassified
`MalformedDecodePayload`, `NonUTF8Path`, and `TruncatedPayload` as
`requiresFake=true` (they never reached the real GlueClient anyway).
Re-estimate when the §5.3 matrix grows or when new `requiresReal`
scenarios are added.

At Glue Schema Registry public pricing (free for the first ~1M
ops/month at the time of writing) this is well inside the free
tier even at hundreds of runs/day.

## Post-run cleanup verification

The selector wires `realglue.Cleanup.Run` into `t.Cleanup`, so the
suite deletes every schema it created in reverse insertion order —
even on `t.Fail`. The suite does NOT create or delete registries;
every test schema lives inside the pre-existing `default-registry`.

To sanity-check that nothing leaked, list SCHEMAS in the registry
the suite uses and grep for the runbook prefix:

```
aws glue list-schemas \
  --registry-id RegistryName=default-registry \
  --region <r> \
  --query 'Schemas[?starts_with(SchemaName, `gsr-go-it-`)].SchemaName' \
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
registry is shared infrastructure and the suite expects it to
exist on every run.

## Troubleshooting

- **`realglue: resolve AWS credentials: ...`** — the eager probe in
  `realglue.New` failed. Either the profile is misspelled or the
  SSO session has expired. Re-run `aws sso login --profile <p>` and
  try again.
- **`ThrottlingException`** — back off and retry. Glue throttles
  CreateSchema at a relatively low rate. Reduce parallelism by
  passing `-p 1` to `go test` (edit the target if you need this
  often).
- **Tests t.Skip** with "scenario requires real Glue;" — that's
  correct behavior for the fakeglue-side runs of those scenarios.
  Their requiresReal companions run instead.
- **Pre-existing `gsr-go-it-*` leaks** from an earlier crashed
  run — clean those up before invoking; otherwise the new run's
  `CreateSchema` calls succeed but the count of registries in your
  account grows.

## When to use this vs. `make test-integ`

- `make test-integ` — fakeglue path. Bills nothing. Run on every
  commit, in PR gates, locally.
- `make test-integ-real` — real-Glue path. Bills AWS. Run before a
  release tag, after touching `core/`'s GlueClient surface, or
  whenever you want to prove the real-AWS code path compiles and
  resolves credentials.
- (Future) Phase 5 canary harness — runs continuously in beta with
  CloudWatch alarms. That is separate from this runbook.
