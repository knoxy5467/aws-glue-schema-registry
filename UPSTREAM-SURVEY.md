# Upstream Survey — `upstream/master` vs `golang-mrknox-4x-integrated @ 7d3a7fe`

**Branch under survey:** `golang-mrknox-4x-integrated` @ `7d3a7fe5e19a54118192c35b3688c6162046aa93`
**Upstream:** `awslabs/aws-glue-schema-registry/master` @ `4b9cac4`
**Merge-base:** `c6f9168`
**Upstream is 12 commits ahead, our branch is 119 commits ahead, with broad multi-file overlap.**

> All counts and SHAs come from `git log` / `git ls-tree` / `git diff` run 2026-06-25.

---

## Section 1 — Upstream commit list (12 commits, oldest → newest)

| # | Short SHA | Subject | Category | "Why we'd care" |
|---|-----------|---------|----------|-----------------|
| 1 | `325b315` | test(deserializer): secondary deserializer routing logic tests (#428) | Java test additions | Pure Java test in `serializer-deserializer/`. Not on our Go path. |
| 2 | `03bd9d3` | fix(kafkaconnect): resolve secondary deserializer schema extraction for non-GSR data (#440) | Kafka Connect bug fix | Affects Kafka Connect users only. Not on our Go path. |
| 3 | `c09ad35` | Fix recursive protobuf schema StackOverflowError (#448) | Protobuf bug fix | **Relevant.** Fixes recursive-proto schema → Connect conversion. Adds `RecursiveTypeSyntax2.proto` / `RecursiveTypeSyntax3.proto` fixtures useful for our cross-language sweep. |
| 4 | `a45534c` | Add multi-language support for C# clients (#485) | Directory rename + C# SDK | **The structural blocker.** Renames `native-schema-registry/` → `multilang-schema-registry/` and lands C# scaffolding. 289 files / +19,968 lines. Source of all merge conflicts. |
| 5 | `8f99caf` | Fix commons compress version to latest (#489) | Dep pin | Trivially cherry-pickable. |
| 6 | `ec1145d` | Moving from sonatype-nexus to central-publishing-maven (#496) | Maven publish | Affects publishing only. Touches every pom.xml. Conflicts with our version bumps. |
| 7 | `b2e88d4` | Lz4 Dependency Upgrade (#497) | Java runtime fix | Adds `lz4-shim/` module (reverted by #499). |
| 8 | `8f5e669` | Bumping version to 1.1.27 (#498) | Version bump | We're still on 1.1.25 across all 15 module poms. |
| 9 | `b280404` | Removes Lz4 Shim and Updates Dependency Version (#499) | Java runtime fix | **CVE-2025-66566.** Replaces `org.lz4:lz4-java:1.8.1` (vulnerable) with `at.yawk.lz4:lz4-java:1.10.2`. Cherry-pickable: pom.xml exclusions + explicit dep. |
| 10 | `c3315a0` | feat(csharp-demos): Migrate to AWS.Glue.SchemaRegistry NuGet (#520) | C# only | Zero impact on Go. |
| 11 | `6c44560` | [Java] Fix NoClassDefFoundError: lombok/Lombok at runtime (#514) | Java runtime fix | **Real Java production bug.** Lombok-generated `Lombok.sneakyThrow()` calls fail at runtime because lombok is `provided`-scoped. Replaces `@SneakyThrows` with explicit try-catch in `ProtobufDataToConnectDataConverter`, `GlueSchemaRegistryDeserializerDataParser`, `GlueSchemaRegistrySerializationFacade`, `AvroSerializer`. |
| 12 | `4b9cac4` | fix(integration-tests): pin localstack to 4.12 (#521) | CI fix | Pins `localstack:latest` → `localstack:4.12`. Recent `:latest` is incompatible with `cloud.localstack:0.2.23`. Same flake hit Phase 6.x. |

---

## Section 2 — Net-new files in `multilang-schema-registry/` (25 files)

514 of 539 upstream `multilang-schema-registry/` files are 1:1 renames of our `native-schema-registry/` files. **25 are net-new.**

### 2a. C# tests & scaffolding (8 files) — DO NOT need to absorb

| File (path under `multilang-schema-registry/`) | Absorb? |
|---|---|
| `csharp/AWSGsrSerDe/AWSGsrSerDe/native-shared-object-version.txt` | No — C#-only |
| `csharp/AWSGsrSerDe/AWSGsrSerDe/scripts/extract-native-so-version.sh` | No — C#-only |
| `csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/configuration/ConfigValidationTests.cs` | No — but the shared properties files it consumes ARE useful (see 2b) |
| `csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/integ-tests/configuration/ConfigValidationIntegTests.cs` | No |
| `csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/integ-tests/evolution-tests/AvroEvolutionTests.cs` | No — but useful reference for evolution coverage parity |
| `csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/serializer/GlueSchemaRegistryKafkaSerializerFuzzTests.cs` | No |
| `csharp/AWSGsrSerDe/AWSGsrSerDe.Tests/utils/IntegrationTestBase.cs` | No |
| `csharp/AWSGsrSerDe/icon.png` | No |

### 2b. Shared test-config fixtures (15 files) — **YES, absorb**

Path prefix: `multilang-schema-registry/shared/test/configs/`. Deliberately shared across all language clients.

| File | Negative path / scenario |
|---|---|
| `auto-registration-disabled.properties` | Should fail on unknown schema when auto-register=false |
| `cache-disabled.properties` | Forces fresh GetSchemaVersion per call |
| `cache-ttl-short.properties` | Schema fetch storm test |
| `custom-user-agent.properties` | User-agent header propagation |
| `different-region-registry.properties` | Cross-region positive path |
| `inexistent-registry.properties` | EntityNotFoundException negative path (Phase 4.14) |
| `insufficient-iam-permissions.properties` | AccessDeniedException negative path |
| `invalid-endpoint.properties` | UnknownHostException |
| `invalid-region.properties` | Invalid AWS region |
| `invalid-role-to-assume.properties` | STS AssumeRole failure |
| `minimal-auto-registration-custom-registry.properties` | Happy-path baseline (non-default registry) |
| `minimal-auto-registration-default-registry.properties` | Happy-path baseline (default registry) |
| `non-default-endpoint.properties` | Explicit endpoint honored |
| `region-endpoint-mismatch.properties` | Region/endpoint mismatch |
| `zlib-compression-enabled.properties` | Compression toggle on (Phase 4.10) |

**Recommendation:** Copy these 15 files into `native-schema-registry/shared/test/configs/` and wire through our Go config-loader negative-path tests.

### 2c. Misc (2 files)

| File | Notes |
|---|---|
| `multilang-schema-registry/test/java/com/amazonaws/services/schemaregistry/serializer/ProtobufPreprocessorTest.java` | Likely mis-rooted; should live under `serializer-deserializer/src/test/java/...`. Investigate before adopting. |
| `multilang-schema-registry/CHANGELOG.md` | Sub-tree changelog. We don't maintain one for `native-schema-registry/`. Optional. |

---

## Section 3 — pom.xml + build-system deltas

### 3a. Root version

| Field | Ours | Upstream |
|---|---|---|
| `<version>` in root pom.xml | `1.1.25` | `1.1.27` |
| Cross-module versions | `1.1.25` | `1.1.27` |

### 3b. Module list (the rename)

Slot 8 in `<modules>`: **ours = `native-schema-registry`**; **upstream = `multilang-schema-registry`**. All other 11 module slots match.

### 3c. protobuf-java pin

| Field | Ours | Upstream |
|---|---|---|
| `<protobuf.version>` | `3.25.5` | `3.25.5` |

**Unchanged.** The `php_generic_services` parity gap Phase 4.16 flagged is NOT closed by upstream. Still ours.

### 3d. Lz4 + commons-compress

| Field | Ours | Upstream |
|---|---|---|
| `commons-compress` pin | Present | Present + fixed version (`8f99caf`) |
| `lz4-java` | Transitive 1.8.1 (vulnerable to CVE-2025-66566) | `at.yawk.lz4:lz4-java:1.10.2` explicit + 3 transitive exclusions (`b280404`) |

**CVE-2025-66566:** our branch still pulls vulnerable `org.lz4:lz4-java:1.8.1`. Upstream's fix is straightforward to cherry-pick — exclusions + one explicit dep in `pom.xml` and `serializer-deserializer/pom.xml`.

---

## Section 4 — Code conflicts in a real merge

The aborted merge produced 14 UU conflicts:

### 4a. Module/version conflicts (12 pom.xml files)

3-way `<module>native-schema-registry</module>` vs `<module>multilang-schema-registry</module>` + `1.1.25` vs `1.1.27` collisions. `pom.xml` itself is the worst — every section overlaps (modules, version, lz4, commons-compress, Maven publish).

### 4b. Tree-shape conflicts (2 files)

- `.gitignore` — both sides appended. Dedupe and pick path prefix.
- `.github/workflows/build-csharp.yml` — add/add. Resolve = take upstream + add our extra branch trigger.

### 4c. Auto-merged but suspicious

- `CHANGELOG.md` — auto-merged; verify the order.
- `README.md` — both sides touched. Trivially mergeable.

### 4d. Java source overlaps with semantic risk

| File | Upstream | Ours | Risk |
|---|---|---|---|
| `common/.../SchemaByDefinitionFetcher.java` | Lombok removal | Phase 4.x audit | Re-validate post-merge |
| `common/.../AWSSchemaRegistryClient.java` | Lombok removal | Phase 4.x | Re-validate |
| `common/.../GlueSchemaRegistryConfiguration.java` | PR #485 | Phase 4.x config-parity | **High overlap** — this is the class our Go config mirrors |
| `avro-kafkaconnect-converter/.../AWSKafkaAvroConverter.java` | `03bd9d3` | Phase 4.x | Java-only — adopt upstream verbatim |
| `serializer-deserializer/.../ProtobufWireFormatDecoder.java` | `c09ad35` | Phase 4.17 | Verify recursive-proto fix doesn't regress reader-schema projection |
| `serializer-deserializer/.../utils/apicurio/ProtobufSchemaLoader.java` | `a45534c` +19 lines | Phase 4.x | Conflict possible |
| `serializer-deserializer/.../utils/ProtobufSchemaParser.java` | `a45534c` +20 lines | Phase 4.x | Conflict possible |

---

## Section 5 — Migration decision (the user must pick)

### Option A — Keep both trees post-merge

**Mechanics:** Resolve every pom.xml conflict by listing **both** modules. End up with `native-schema-registry/` + `multilang-schema-registry/` side by side. 514 of 539 files are duplicates.

| | |
|---|---|
| Effort | Low (~1 day) |
| What gets lost | Nothing |
| What gets broken | Two top-level Maven modules build the same C/C# artifacts. Disk footprint doubles. Future upstream merges keep paying conflict tax. Confused developers. |

**Verdict:** Structural disaster. Not recommended.

### Option B — Delete `multilang-schema-registry/` after merge

**Mechanics:** Merge upstream, then `git rm -rf multilang-schema-registry/`, restore `<module>native-schema-registry</module>` in `pom.xml`.

| | |
|---|---|
| Effort | Low (~½ day) |
| What gets lost | The 25 net-new files in Section 2 — most critically the 15 `shared/test/configs/*.properties` fixtures we actually want |
| What gets broken | Nothing structural — pre-PR-485 layout |

**Verdict:** Cleanest short-term, but throws away genuinely useful upstream test work.

### Option C — Migrate our Go work to `multilang-schema-registry/`

**Mechanics:** Accept upstream's rename. `git mv native-schema-registry/golang multilang-schema-registry/golang`, same for `GolangDemoGSRKafka/`, `perf/`, etc. Update every reference.

| | |
|---|---|
| Effort | **High** (~2-3 days). 84+ Go source files reference `native-schema-registry/` paths. Plus Makefile, shell scripts, CI workflows, docs, perf baselines, README links, demo dockerfiles. |
| What gets lost | Nothing — git history preserved by `git mv` |
| What gets broken | Every hardcoded path reference until updated. Phase 6.x perf baselines, D19, 4.13, 4.16, 4.17 all need re-runs. |

**Verdict:** Structurally correct. The right long-term answer. Should be a dedicated phase.

---

## Section 6 — Recommended next steps

1. **Do not perform the upstream merge as a one-shot.** The structural rename (#485) + version bump (#498) + lz4 fix (#499) + Lombok fix (#514) want to be merged as independent logical patches, each verifiable on its own.

2. **Phase 7 candidate scope:**
   - **Phase 7.1: Surgical cherry-picks** of the safe upstream fixes into our `native-schema-registry/`-rooted tree:
     - `c09ad35` (recursive proto fix + new fixtures)
     - `8f99caf` (commons-compress pin)
     - `b280404` (lz4 CVE-2025-66566 fix)
     - `6c44560` (Lombok runtime fix)
     - `4b9cac4` (localstack pin)
     - `8f5e669` (version bump to `1.1.27`)
   - **Phase 7.2: Adopt the 15 shared-config-fixture files** into `native-schema-registry/shared/test/configs/`. Wire through Go config-loader negative-path tests. Validates cross-language fixture parity.
   - **Phase 7.3 (deferred, large):** Execute Option C migration. Dedicated phase, fresh CR, full real-AWS re-run.

3. **Until Phase 7.3 lands**, this `golang-mrknox-4x-integrated` branch remains the canonical "Go GSR client" tree at `native-schema-registry/golang/`. The upstream `multilang-schema-registry/` layout is for the C# SDK and is not yet our concern.

4. **Skip outright:** `c3315a0` (C# NuGet demo), `a45534c` (C# scaffolding — we don't ship C#), `ec1145d` (Maven publish swap — we don't publish to Central), `325b315` + `03bd9d3` (Kafka Connect Java-only). No value for the Go client.

---

## Section 7 — Backup state

Pre-merge backups created 2026-06-25T18:07:23Z. All 13 branches preserved both locally (under `refs/backups/20260625T180723Z/`) and on the fork (under `backup/20260625T180723Z/<branch-name>` on `origin` = `https://github.com/knoxy5467/aws-glue-schema-registry`):

| Branch | SHA |
|---|---|
| `golang-mrknox-4x-integrated` | `7d3a7fe` |
| `golang-mrknox` | `fe0ce5b` |
| `phase-4.9-audit` | `821f93c` |
| `phase-4.10` | `d23c60a` |
| `phase-4.11` | `44430e3` |
| `phase-4.12` | `d3f7b30` |
| `phase-4.13` | `d7f2a6d` |
| `phase-4.14` | `d1b6282` |
| `phase-4.15` | `2288a9e` |
| `phase-4.16` | `6999646` |
| `phase-4.17` | `3ae775d` |
| `phase-6.3` | `b2819a7` |
| `phase-6.4` | `0afd0ed` |

Recovery: `git fetch origin "+refs/heads/backup/20260625T180723Z/*:refs/heads/restore/*"` brings every branch back as `restore/<name>`.

---

## Appendix A — Reproduction commands

```bash
# 1. Commit list
git -C /workplace/mrknox/4x-integrated log --oneline c6f9168..upstream/master

# 2. Net-new files in multilang-schema-registry/
comm -23 \
  <(git -C /workplace/mrknox/4x-integrated ls-tree -r upstream/master --name-only \
    | grep "^multilang-schema-registry/" | sed 's|^multilang-schema-registry/|X/|' | sort) \
  <(find /workplace/mrknox/4x-integrated/native-schema-registry/ -type f \
    | sed 's|.*/native-schema-registry/|X/|' | sort)

# 3. Files changed on both sides (conflict candidates)
comm -12 \
  <(git -C /workplace/mrknox/4x-integrated diff --name-only c6f9168 HEAD | sort) \
  <(git -C /workplace/mrknox/4x-integrated diff --name-only c6f9168 upstream/master | sort)

# 4. Hardcoded path footprint (Option C scoping)
grep -rl "native-schema-registry/" --include="*.go" \
  /workplace/mrknox/4x-integrated/native-schema-registry/golang/ | wc -l
# -> 84+

# 5. Backup recovery
git fetch origin "+refs/heads/backup/20260625T180723Z/*:refs/heads/restore/*"
```

---

*Document generated 2026-06-25. The merge into `upstream/master` was attempted, surveyed, and aborted in favor of this documentation-first approach per the user's "ensure that we are not missing anything" instruction. No source files were modified; integrated branch is unchanged at `7d3a7fe`.*
