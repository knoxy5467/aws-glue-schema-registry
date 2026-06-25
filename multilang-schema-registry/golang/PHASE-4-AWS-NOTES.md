# Phase 4 — AWS account notes for the Go GSR canary

**Status:** open question for mrknox@ to triage.
**Date authored:** 2026-06-21.

## Plan §6.3 reference text

> The existing `AWSSchemaRegistryClientCanaryInfraCDK` defines a
> `gsr-client-canary-csharp` CodeBuild project on the canary AWS
> account **747156111546** in **us-east-2**. A `gsr-client-canary-golang`
> will be added alongside it.

## What I could verify from this dev host

- Access to account 747156111546 was NOT attempted from this Phase 4
  pass. The plan instructed not to actually bill AWS, and this
  account is also outside the AssumeRole chain the dev host carries
  (verified via `cat ~/.aws/config` — only `mrknox-*` personal stack
  accounts and the developer "customer" account 850995546034 are
  present).
- The plan's open-question #2 (§11) explicitly asks whether reusing
  747156111546 is the default for the Go canary. The Java and C#
  canaries both run there.
- The Canary CDK package
  (`AWSSchemaRegistryClientCanaryInfraCDK`, see plan §6.3) is the
  authoritative source for the AWS account + region. Inspecting it
  requires `brazil ws use` and is out of Phase 4 scope.

## Recommendation (no decision)

If 747156111546 is reusable for the Go canary:

1. Add `gsr-client-canary-golang` to the canary CDK's CodeBuild
   project list (alongside the C# project).
2. Update `githubOidcStack.ts`'s allowlist if and only if Phase 0's
   branch-name decision lands at something other than
   `native-schema-registry-golang`. Today, Phase 0 confirms the work
   stays on the fork (`knoxy5467/aws-glue-schema-registry:golang-mrknox`)
   until upstream contribution is accepted; the CDK allowlist points
   at `awslabs/aws-glue-schema-registry:native-schema-registry-golang`,
   so the OIDC trust would NOT work for the fork. Either the fork's
   branch needs to be added to the allowlist, or the work needs to
   move upstream before canary wiring.

If 747156111546 is NOT reusable:

1. Provision a new beta account dedicated to the Go canary.
2. Mirror the Java/C# canary's IAM allow-list:
   - `glue:CreateSchema`, `glue:CreateRegistry`, `glue:RegisterSchemaVersion`
   - `glue:GetSchemaByDefinition`, `glue:GetSchemaVersion`
   - `glue:DeleteSchema`, `glue:DeleteRegistry`
   - `glue:TagResource`, `glue:PutSchemaVersionMetadata`,
     `glue:QuerySchemaVersionMetadata`, `glue:GetTags`
   - `kinesis:PutRecord`, `kinesis:GetRecords`, … (for the
     §6.2 functional-canary subset).
3. Same OIDC trust setup as the Java/C# canary, gated on the new
   account ID.

## Open items for mrknox@

1. Confirm whether 747156111546 has quota for one more set of
   `glue:CreateSchema` / `glue:CreateRegistry` operations beyond the
   current Java + C# canary cadence.
2. Confirm whether the Phase 4 IT runs (which use a randomized
   schema-name suffix and `t.Cleanup`-driven delete) are acceptable
   to run against the canary account in dev or whether a separate
   beta account is needed for ad-hoc dev runs.
3. Confirm the canary CDK package owner is happy with a new
   CodeBuild project name `gsr-client-canary-golang` (Java/C# follow
   the `gsr-client-canary-<lang>` pattern; Go should fit cleanly).

Phase 4 ships without resolving these — they belong to Phase 5
(canary harness).
