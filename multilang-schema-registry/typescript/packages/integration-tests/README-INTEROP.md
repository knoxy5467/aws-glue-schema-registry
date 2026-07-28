# Cross-language interop test runbook (TypeScript ↔ Java)

This is the operator runbook for `npm run test:integ:interop:real`. It
runs the cross-language interop suites against a live Java sidecar
(vendored under `packages/integration-tests/java-interop/`), a
`testcontainers`-managed Kafka broker, **and** real AWS Glue Schema
Registry in a non-production AWS account you control. Use it when you
want to prove wire-format parity end-to-end between the TypeScript
client and the reference Java `aws-glue-schema-registry` implementation
— for a release tag, after touching the `@gsr/serde` wire path, or
whenever you want to confirm the
Kafka-in-the-loop customer API stays wire-compatible across languages.
This tier exercises live cross-language round-trips beyond the offline
committed-golden-vector set the `gate/` byte-identity suite covers (see
[`testdata/golden-java/PROVENANCE.md`](../../../testdata/golden-java/PROVENANCE.md)
for the offline-scope note).

The runbook is a companion to
[`README-REAL-AWS.md`](./README-REAL-AWS.md). The two share their
credentials and cost model; this document adds the Java + Kafka
prerequisites the interop tier layers on top.

## What runs under `test:integ:interop:real`

Three suites under `packages/integration-tests/test/interop/`:

| Suite | Direction | What it proves |
|-------|-----------|----------------|
| `ts-to-java-same-version.integ.test.ts` | TS produces, Java consumes | TS-encoded framed bytes are decodable by the reference Java Kafka deserializer for all five subtypes (Avro Generic, Avro Specific, Protobuf Dynamic, Protobuf concrete, JSON-Schema) under NONE and ZLIB compression. |
| `java-to-ts-same-version.integ.test.ts` | Java produces, TS consumes | Reference-Java-encoded framed bytes are decodable by the TypeScript deserializer for the same five subtypes and compressions. |
| `cross-version.integ.test.ts` | Both directions | v1 records framed with v1's schema-version UUID decode correctly even when v2 (BACKWARD-compatible successor) is the latest registered version — writer-schema semantics across languages. |

All three suites are `.integ.test.ts` files and therefore selected only
by `vitest.integration.config.ts`; the default `npm test` never picks
them up.

## Prereqs

1. **AWS account credentials** for a non-production account you control
   — same rules as [`README-REAL-AWS.md`](./README-REAL-AWS.md). One of
   `AWS_PROFILE` or `AWS_ACCESS_KEY_ID` must be set. The
   `test:integ:interop:real` script hard-fails before touching AWS if
   neither is set.

2. **Region.** Defaults to `us-east-2`. Override via `AWS_REGION`. The
   Java sidecar and the TypeScript deserializer both honor
   `AWS_REGION` so a mismatched pair cannot exist.

3. **IAM permissions.** Superset of the Tier-3 runbook — the interop
   tier also drives the Java `KafkaSerializer` /
   `KafkaDeserializer` code paths, which call the same Glue actions
   the TS deserializer does plus a few extras the Java client uses
   for schema-name lookups by ARN:

   ```
   glue:CreateSchema
   glue:RegisterSchemaVersion
   glue:GetSchemaByDefinition
   glue:GetSchemaVersion
   glue:GetSchema
   glue:DeleteSchema
   glue:ListSchemas                (for the leak-check below)
   glue:PutSchemaVersionMetadata
   glue:QuerySchemaVersionMetadata
   glue:GetTags
   ```

   All operations scoped to the region in step 2.

4. **`default-registry` exists.** Same precondition as the Tier-3
   runbook. The interop tier's schemas all live inside
   `default-registry`. Glue auto-creates it on the first
   `CreateSchema` call if it is not already present, or you can create
   it once by hand:

   ```
   aws glue create-registry \
     --registry-name default-registry \
     --region <r>
   ```

5. **JDK 17+.** Required to build and run the sidecar. Any Java 17+
   distribution (OpenJDK, Amazon Corretto, Temurin, etc.) works — the
   sidecar only relies on standard Java SE APIs. Confirm the install
   with `java -version`.

6. **Maven 3.9+.** Required by `maven-shade-plugin` 3.6 to produce the
   sidecar fat JAR. If `mvn` is not on PATH, install locally per the
   sidecar module's own README
   ([`java-interop/README.md`](./java-interop/README.md)).

7. **Docker.** Required by `testcontainers` to start the ephemeral
   Kafka broker used by the Kafka-in-the-loop endpoints
   (`/kafka-produce`, `/kafka-consume`). Skip this only if you supply an
   external broker via `KAFKA_BROKER=<host:port>` (see env-var matrix
   below).

8. **Built sidecar fat JAR.** The interop launcher looks for the JAR
   at `packages/integration-tests/java-interop/target/java-interop-sidecar.jar`.
   Build it once per checkout with:

   ```
   cd packages/integration-tests
   npm run build:sidecar
   ```

   The build takes ~10 seconds warm, ~1 minute cold (downloads the
   `schema-registry-common` / `schema-registry-serde` deps from Maven
   Central on first run). The JAR is ~65 MB and is gitignored.

## Env-var matrix

| Var | Values | Purpose |
|-----|--------|---------|
| `AWS_INTEGRATION` | `1` (any other value = unset) | Master switch for the integration project. Required. |
| `GSR_GLUE` | `real` (anything else = fake) | Selects the real Glue seam. `test:integ:interop:real` exports this for you. |
| `AWS_REGION` | e.g. `us-east-2` | Region for real Glue. Defaults to `us-east-2` when unset or empty. |
| `AWS_PROFILE` | your AWS profile name | Preferred credentials source. Either this or `AWS_ACCESS_KEY_ID` is required; the script hard-fails otherwise. |
| `AWS_ACCESS_KEY_ID` (+ `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) | AWS access key material | Alternative credentials source. |
| `GSR_INTEROP_PARTNER` | `java` or unset | Selects the interop sidecar. `java` is the only supported partner today. Any other non-empty value is a **loud error**, not a skip, so a downgrade to a nonexistent sidecar cannot ship a false green. Unset falls through to `java`. |
| `GSR_INTEROP_JAVA` | absolute path to a `java` binary | Overrides the JDK the launcher spawns. When unset, the launcher probes for `java` on PATH. Mirrors the reference client's hermetic-build seam. |
| `KAFKA_BROKER` | absent / empty / bootstrap string | Three-state broker discovery. **Absent** → testcontainers starts a broker automatically. **Empty** (`KAFKA_BROKER=`) → explicit no-Docker signal; Kafka-requiring suites loud-skip. **Non-empty bootstrap** → reuses the external broker at that address (testcontainers not started). |

Any other Tier-2/3 env var honored by
[`README.md`](./README.md) — `GSR_REFERENCE_ROOT` and friends — also
applies here; the interop tier does not shadow them.

## Invocation

```
cd multilang-schema-registry/typescript

# One-time (per checkout): build the sidecar fat JAR
(cd packages/integration-tests && npm run build:sidecar)

# Then run the interop suites against real AWS + real Java + Kafka
AWS_PROFILE=<your-profile> AWS_REGION=us-east-2 npm run test:integ:interop:real
```

The script will:

1. Verify creds env vars are set. If neither `AWS_PROFILE` nor
   `AWS_ACCESS_KEY_ID` is set, print a refusal message and exit
   non-zero (no AWS call made, no sidecar spawn, no Docker start).
2. Print an `About to bill real AWS Glue and drive a Java sidecar +
   Kafka against <region>. Press Ctrl-C within 5s to abort.` banner and
   `sleep 5`.
3. Run:

   ```
   GSR_GLUE=real AWS_INTEGRATION=1 \
     vitest run --config vitest.integration.config.ts
   ```

4. Each interop suite runs its own three-gate check inside
   `beforeAll`:
   - `describeIntegration` — the outer `AWS_INTEGRATION=1` gate.
   - `requireInterop()` — stacks `GSR_GLUE=real`,
     `GSR_INTEROP_PARTNER` (loud-error on unsupported values),
     JVM probe (`GSR_INTEROP_JAVA` or `java` on PATH), and the
     built-JAR probe.
   - `resolveBroker()` — the tri-state Kafka discovery from
     `kafka-broker.ts`.

   Any unmet gate emits exactly one machine-parseable line
   (`SKIP <suite> — <reason>`) and short-circuits the suite's tests.

5. On exit, each suite's `afterAll` runs teardown in reverse order:
   sidecar SIGTERM → broker stop → `CleanupTracker.run()` deleting
   every schema the run tracked (in reverse insertion order,
   tolerating `EntityNotFoundException`). Cleanup fires on test
   failure too.

## Loud-skip contract

Every skipped interop suite emits a single line naming the suite and
the missing gate:

- `SKIP interop/ts-to-java-same-version — AWS_INTEGRATION!=1`
- `SKIP interop/ts-to-java-same-version — GSR_GLUE!=real`
- `SKIP interop/java-to-ts-same-version — JVM not available (set GSR_INTEROP_JAVA or install a JDK on PATH)`
- `SKIP interop/cross-version — sidecar JAR not built (run npm run build:sidecar)`
- `SKIP interop/ts-to-java-same-version — KAFKA_BROKER= (no-Docker signal)`

A bare `it.skip` or `describe.skip` with no reason line is a defect —
same rule as the tiered README.

Unsupported partner selectors are a **loud error**, not a skip. If you
set `GSR_INTEROP_PARTNER=go`, `requireInterop()` throws:

```
GSR_INTEROP_PARTNER=go is not supported: only 'java' or unset are valid
```

## Expected cost

Same order-of-magnitude as
[`README-REAL-AWS.md`](./README-REAL-AWS.md), scaled by the number of
interop cells:

- Same-version TS→Java: 5 subtypes × 2 compressions = 10 cells. Per
  cell: 1× `CreateSchema` (first-time) or `RegisterSchemaVersion`
  (subsequent), 1× `GetSchemaByDefinition`, 1× `GetSchemaVersion` (may
  poll up to 10× on eventual consistency), 1× `PutSchemaVersionMetadata`
  (non-fatal), 1× `DeleteSchema` at teardown.
- Same-version Java→TS: 10 cells, same profile.
- Cross-version: 6 cells × 2 directions = 12 sub-tests. Each cell
  registers v1 + v2 under one namespaced schema name, so
  `CreateSchema` × 1 + `RegisterSchemaVersion` × 1 + `GetSchemaVersion`
  polling. Cleanup deletes one schema name per cell.

Rough total: **150–500 Glue control-plane calls per full run**,
comfortably inside the Glue free tier at the time of writing. Kafka
cost is zero (testcontainers is ephemeral, or the external broker is
already yours).

## Post-run cleanup verification

The `CleanupTracker` wires cleanup into every interop suite's
`afterAll`, so all namespaced schemas the run created are deleted in
reverse insertion order on exit. To sanity-check that nothing leaked,
list schemas in `default-registry` and grep for the runbook prefix:

```
aws glue list-schemas \
  --registry-id RegistryName=default-registry \
  --region <r> \
  --query 'Schemas[?starts_with(SchemaName, `gsr-ts-it-`)].SchemaName' \
  --output text
```

The expected output is **empty**. Any name printed is a leaked schema.
Delete by hand:

```
aws glue delete-schema \
  --schema-id RegistryName=default-registry,SchemaName=<leaked-name> \
  --region <r>
```

Do NOT run `delete-registry` against `default-registry`.

## Troubleshooting

- **`test:integ:interop:real: refusing to run — neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set`**
  — the script hard-fails before touching AWS. Set one of the two env
  vars.
- **`SKIP interop/... — sidecar JAR not built (run npm run build:sidecar)`**
  — build the fat JAR first with
  `(cd packages/integration-tests && npm run build:sidecar)` and re-run.
- **`SKIP interop/... — JVM not available`** — set `GSR_INTEROP_JAVA=/absolute/path/to/java`
  or install a JDK 17+ on PATH.
- **`GSR_INTEROP_PARTNER=<other> is not supported`** — loud error. Set
  `GSR_INTEROP_PARTNER=java` or leave it unset.
- **Sidecar hangs on start** — `beforeAll` gives the JVM 120 seconds
  to print its `PORT: <n>` first-line handshake and answer `/health`.
  If it hangs, run the JAR manually to see the stack trace:
  ```
  java -jar packages/integration-tests/java-interop/target/java-interop-sidecar.jar --port=0
  ```
- **`ThrottlingException`** — Glue rate-limits `CreateSchema`. Back off,
  reduce parallelism (`vitest --pool=forks --poolOptions.forks.singleFork`).
- **`ExpiredTokenException`** — refresh SSO: `aws sso login --profile <p>`.
- **`Docker daemon not running`** — start Docker or supply
  `KAFKA_BROKER=<host:port>` pointing at an external broker; the Kafka
  suites will reuse it instead of starting testcontainers.
- **Leaked `gsr-ts-it-*` schemas from a prior crashed run** — clean
  them up before invoking; otherwise the run's `CreateSchema` calls
  succeed but your account's schema count grows.

## When to use this vs. sibling scripts

- `npm test` — Tier-1 unit only. Free. No network, no Docker, no AWS.
- `npm run test:integ` — Tier-2 fake-Glue + testcontainers Kafka. Free.
  Interop suites loud-skip under `GSR_GLUE!=real`.
- `npm run test:integ:real` — Tier-3 real-Glue round-trip. Bills AWS,
  no Java sidecar involved.
- `npm run test:integ:interop:real` — this runbook. Bills AWS, spawns
  the Java sidecar, drives real Kafka. Run before a release tag or
  after touching wire-format bytes on either side.

## Cross-references

- **Tier-3 real-Glue runbook** — [`README-REAL-AWS.md`](./README-REAL-AWS.md).
- **Sidecar module README** — [`java-interop/README.md`](./java-interop/README.md).
  Sidecar HTTP contract, prereqs, and build details.
- **Tiered README** — [`README.md`](./README.md). The full env-gate
  matrix and directory layout.
- **Reference client runbook** — the reference client's real-AWS
  runbook is the source this runbook is patterned after.
