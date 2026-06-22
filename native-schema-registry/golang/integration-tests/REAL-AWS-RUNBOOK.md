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
   glue:CreateRegistry             (only if you don't pre-provision
                                   the `default-registry`)
   glue:DeleteRegistry
   glue:CreateSchema
   glue:RegisterSchemaVersion
   glue:GetSchemaByDefinition
   glue:GetSchemaVersion
   glue:DeleteSchema
   glue:PutSchemaVersionMetadata
   glue:QuerySchemaVersionMetadata
   glue:GetTags
   ```

   `glue:ListRegistries` is needed for the post-run cleanup
   verification step below. All operations scoped to the region in
   step 2.

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

Each `RequiresRealGlue=false` scenario (the bulk of the suite) skips
under `GSR_GLUE=real` because the assertion shape requires fakeglue
introspection. Today the `GSR_GLUE=real` path exercises roughly:

- ~6 wire-format direct scenarios (no Glue).
- ~3 compatibility round-trips that hit real Glue
  (CreateSchema + GetSchemaByDefinition × 2).
- ~1 `RequiresRealGlue=true` companion that exercises server-side
  compatibility rejection (CreateSchema × 2 — one succeeds, one
  rejected at v2).
- Negative-decode scenarios that fetch by version-UUID (one
  GetSchemaVersion per).

Conservative ceiling per run: **< 50 Glue control-plane calls**.
At Glue Schema Registry public pricing (free for the first ~1M
ops/month at the time of writing), this is well inside the free
tier. The §5.3 matrix expansion in a future phase will increase
the call count — re-estimate when the matrix grows.

## Post-run cleanup verification

The selector wires `realglue.Cleanup.Run` into `t.Cleanup`, so the
suite deletes every registry / schema it created in reverse insertion
order — even on `t.Fail`. To sanity-check that nothing leaked:

```
aws glue list-registries --region <r> | grep gsr-go-it-
```

The expected output is **empty**. Any line printed is a leak. If you
see leaks, delete them by hand:

```
aws glue delete-registry --registry-id RegistryName=<leaked-name> --region <r>
```

(Schemas inside a registry are deleted as part of `delete-registry`;
you don't need a separate `delete-schema` pass for leak cleanup.)

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
