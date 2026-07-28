# Narrated TypeScript interop demo — operator runbook

This is the operator runbook for `npm run demo:interop:real`. It runs a
narrated, self-explaining program that walks 21 cross-language wire-format
round-trips out loud against a live Java sidecar (vendored under
`packages/integration-tests/java-interop/`), a `testcontainers`-managed
Kafka broker, **and** real AWS Glue Schema Registry in a non-production
AWS account you control.

Where `npm run test:integ:interop:real` proves the same wire-format parity
through terse vitest PASS/FAIL lines, the demo prints each schema body,
the wire-byte hex dump (header byte, compression byte, 16-byte schema-
version UUID, payload body), the decoded values, and a per-scenario
PASS/FAIL line so the compatibility claim is legible without reading test
source. Use it when you want a stakeholder-facing walkthrough — for a
release tag, after touching the `@gsr/serde` wire path, or whenever you
want to confirm the Kafka-in-the-loop customer API stays wire-compatible
across languages. This demo exercises live cross-language round-trips
against a Java sidecar and real Glue; for the offline byte-identity
evidence set (committed golden vectors, single record/message shape per
Avro/Protobuf format, five JSON-Schema shapes) see
[`testdata/golden-java/PROVENANCE.md`](../../../../testdata/golden-java/PROVENANCE.md).

This runbook is a companion to
[`../README-INTEROP.md`](../README-INTEROP.md) and
[`../README-REAL-AWS.md`](../README-REAL-AWS.md). The three share their
credentials and cost model; this document layers the demo-specific
invocation and narrated-output expectations on top.

## What runs under `demo:interop:real`

A single `vite-node` process that drives 21 scenarios in order, each
narrating its schema body, Glue-returned schema-version UUID, wire-byte
hex dump, decoded values, and a PASS/FAIL equality line. The 21
scenarios reconcile against the Go reference `cmd/demo-interop`:

| Group | Count | What it proves |
|-------|-------|----------------|
| Cross-version matrix — Java writes v1, TS reads v2 | 6 | Java-encoded v1-framed bytes decode correctly under a v2 reader schema (writer-schema semantics across languages), for Avro / JSON-Schema / Protobuf, under NONE and ZLIB compression. |
| Cross-version matrix — TS writes v1, Java reads v2 | 6 | TS-encoded v1-framed bytes decode correctly under a v2 reader schema on the Java side, same three formats, same two compressions. |
| Same-version baseline | 6 | Both sides v1; three formats × two directions. Anchors that any cross-version failure is genuinely a v1/v2 evolution issue, not a same-version regression. |
| Cache behavior | 1 | The `@gsr/core` in-memory cache misses on first `getSchemaVersionId`, hits on second, and misses again after `cache.remove(key)`. |
| Auto-register fall-through | 1 | With `schemaAutoRegistrationEnabled=true` and an unregistered schema, the first serialize path auto-registers and returns a valid schema-version UUID. |
| Writer-registers, cold-cache read | 1 | TS registers + writes v1; a fresh Java sidecar consumer with an empty cache resolves the writer schema from the wire UUID and decodes. |

Total: **21 scenarios**. Every scenario prints:

1. A section header with `scenario N of 21` and the human title.
2. A `[scenario] What:` line describing the step about to run, and a
   `[scenario] Proves:` line describing what GSR concept the scenario
   exercises (mirrors the Go reference `cmd/demo-interop`'s per-section
   narration bar).
3. The full multi-line schema body being registered (Avro JSON /
   JSON-Schema JSON / Proto3 IDL) under a `[schema] <label>` header,
   indented so the schema shape reads naturally on the terminal.
4. The Glue-returned schema-version UUID.
5. A `[record]` line naming the record about to be encoded, and — on the
   receiving side — a second `[record]` line for the decoded value.
6. The wire-byte hex dump, decomposed as `[wire-format]` stage lines:
   header byte `0x03`, compression byte, 16-byte schema-version UUID,
   payload body.
7. `[encoder]` / `[kafka]` / `[sidecar]` / `[consumer]` stage lines that
   narrate the produce/consume steps by name.
8. Per-check `printEqualityCheck` PASS/FAIL lines asserting field
   equality **and** wire schema-version UUID equality.
9. A final `[verdict] PASS/FAIL — <detail>` line summarizing the
   scenario outcome (redundant with the earlier equality lines by
   design — an operator scrolling past the tail sees the outcome
   without scrolling back up).

`printEqualityCheck` returns the boolean it prints, and the verdict
line's boolean is the AND of those checks — so the PASS/FAIL you see
per scenario is the PASS/FAIL the process summary uses. No divergent
hidden comparison.

At exit the process prints a summary table listing all 21 scenarios with
a total pass/fail count and returns exit code 0 iff every scenario passed,
else 1.

## Prereqs

1. **Beta AWS account credentials** — same rules as
   [`../README-REAL-AWS.md`](../README-REAL-AWS.md). One of `AWS_PROFILE`
   or `AWS_ACCESS_KEY_ID` must be set. The `demo:interop:real` script
   hard-fails before touching AWS if neither is set.

2. **Region.** Defaults to `us-east-2`. Override via `AWS_REGION`. The
   Java sidecar and the TypeScript deserializer both honor `AWS_REGION`
   so a mismatched pair cannot exist.

3. **IAM permissions.** Superset of the tiered Glue runbook — the demo
   also drives the Java `KafkaSerializer` / `KafkaDeserializer` code
   paths, which call the same Glue actions the TS deserializer does plus
   a few extras the Java client uses for schema-name lookups by ARN:

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

4. **`default-registry` exists.** Every schema the demo creates lives
   inside the pre-existing Glue `default-registry`. Glue auto-creates it
   on the first `CreateSchema` call if it is not already present, or you
   can create it once by hand:

   ```
   aws glue create-registry \
     --registry-name default-registry \
     --region <r>
   ```

   The demo NEVER calls `CreateRegistry` or `DeleteRegistry`.

5. **JDK 17+.** Required to build and run the sidecar. Any Java 17+
   distribution (OpenJDK, Amazon Corretto, Temurin, etc.) works — the
   sidecar only relies on standard Java SE APIs. Confirm the install
   with `java -version`.

6. **Maven 3.9+.** Required by `maven-shade-plugin` 3.6 to produce the
   sidecar fat JAR. If `mvn` is not on PATH, install locally per the
   sidecar module's own README
   ([`../java-interop/README.md`](../java-interop/README.md)).

7. **Docker.** Required by `testcontainers` to start the ephemeral
   Kafka broker used by the Kafka-in-the-loop endpoints
   (`/kafka-produce`, `/kafka-consume`). Skip this only if you supply an
   external broker via `KAFKA_BROKER=<host:port>` (see env-var matrix
   below).

8. **Built sidecar fat JAR.** The demo looks for the JAR at
   `packages/integration-tests/java-interop/target/java-interop-sidecar.jar`.
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
| `AWS_INTEGRATION` | `1` (any other value = unset) | Master switch for the integration project. `demo:interop:real` exports this for you; when unset the demo's `requireInterop()` gate loud-skips and exits 0. |
| `GSR_GLUE` | `real` (anything else = fake) | Selects the real Glue seam. `demo:interop:real` exports this for you. When unset the gate loud-skips. |
| `AWS_REGION` | e.g. `us-east-2` | Region for real Glue. Defaults to `us-east-2` when unset or empty. |
| `AWS_PROFILE` | your AWS profile name | Preferred credentials source. Either this or `AWS_ACCESS_KEY_ID` is required; the script hard-fails otherwise. |
| `AWS_ACCESS_KEY_ID` (+ `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) | AWS access key material | Alternative credentials source. |
| `GSR_INTEROP_PARTNER` | `java` or unset | Selects the interop sidecar. `java` is the only supported partner today. Any other non-empty value is a **loud error**, not a skip, so a downgrade to a nonexistent sidecar cannot ship a false green. Unset falls through to `java`. |
| `GSR_INTEROP_JAVA` | absolute path to a `java` binary | Overrides the JDK the launcher spawns. When unset, the launcher probes for `java` on PATH. Mirrors the reference client's hermetic-build seam. |
| `KAFKA_BROKER` | absent / empty / bootstrap string | Three-state broker discovery. **Absent** → testcontainers starts a broker automatically. **Empty** (`KAFKA_BROKER=`) → explicit no-Docker signal; the demo loud-skips. **Non-empty bootstrap** → reuses the external broker at that address (testcontainers not started). |
| `AWS_ACCOUNT_ID` | your AWS account id (optional) | If set, printed on the demo banner so the operator confirms the target account. When unset the banner prints `(unresolved)` — the demo does not call STS to resolve it (banner-only convenience value). |

## Invocation

```
cd multilang-schema-registry/typescript

# One-time (per checkout): build the sidecar fat JAR
(cd packages/integration-tests && npm run build:sidecar)

# Then run the narrated demo against real AWS + real Java + Kafka
AWS_PROFILE=<your-profile> AWS_REGION=us-east-2 npm run demo:interop:real
```

The script will:

1. Verify creds env vars are set. If neither `AWS_PROFILE` nor
   `AWS_ACCESS_KEY_ID` is set, print a refusal message and exit
   non-zero (no AWS call made, no sidecar spawn, no Docker start).
2. Print an `About to bill real AWS Glue and drive a Java sidecar +
   Kafka against <region>. Press Ctrl-C within 5s to abort.` banner and
   `sleep 5`.
3. Exec `GSR_GLUE=real AWS_INTEGRATION=1 vite-node
   packages/integration-tests/demo/main.ts`.
4. `main.ts` runs its own gates in order:
   - `requireInterop()` — the three-gate discipline
     (`AWS_INTEGRATION=1`, `GSR_GLUE=real`,
     `GSR_INTEROP_PARTNER` (loud-error on unsupported values),
     JVM probe (`GSR_INTEROP_JAVA` or `java` on PATH),
     built-JAR probe). Any unmet gate emits one machine-parseable
     `SKIP <suite> — <reason>` line and exits 0.
   - `requireRealCreds()` — hard throw when `GSR_GLUE=real` and no
     credentials resolve. A silent skip would misreport a run that never
     billed.
   - `resolveBroker()` — the tri-state Kafka discovery. On the `{skip}`
     branch (Docker unavailable or `KAFKA_BROKER=` empty) the demo
     exits 0 cleanly with a `SKIP` line.
5. Bootstrap: banner → broker → sidecar → `parseConfig()` →
   `selectGlueBackend()` (real Glue client + `CleanupTracker`) →
   `SchemaRegistrar` with `createCache` + `createMetadata`.
6. Dispatch scenarios 1–21 in order, collecting one `ScenarioResult`
   per row.
7. On exit — success, failure, or `SIGINT` / `SIGTERM` — teardown runs
   in reverse order via `try/finally`: sidecar `stop()` → broker
   `stop()` → `CleanupTracker.run()` (reverse-order `DeleteSchema`).
   Cleanup fires on scenario failure too.
8. Print the summary table (per-row PASS/FAIL + total) and exit 0 iff
   every scenario passed, else 1.

## Loud-skip contract

Every unmet gate emits a single machine-parseable line and a clean
exit-0 process termination (except missing creds, which is a hard exit-1
refusal from the wrapper script before any process starts):

- `SKIP demo-interop — AWS_INTEGRATION!=1`
- `SKIP demo-interop — GSR_GLUE!=real`
- `SKIP demo-interop — JVM not available (set GSR_INTEROP_JAVA or install a JDK on PATH)`
- `SKIP demo-interop — sidecar JAR not built (run npm run build:sidecar)`
- `SKIP demo-interop — KAFKA_BROKER= (no-Docker signal)`

A bare `it.skip` or `describe.skip` with no reason line is not applicable
here — the demo is a plain `vite-node` process, not a vitest suite. The
same loud-skip discipline applies via `requireInterop()` and
`resolveBroker()` returning explicit `{skip, reason}` values that the
demo's `main()` prints and honors.

Unsupported partner selectors are a **loud error**, not a skip. If you
set `GSR_INTEROP_PARTNER=go`, `requireInterop()` throws:

```
GSR_INTEROP_PARTNER=go is not supported: only 'java' or unset are valid
```

## Cost warning

The demo BILLS AWS. It drives real `CreateSchema`,
`RegisterSchemaVersion`, `GetSchemaByDefinition`, `GetSchemaVersion`,
`PutSchemaVersionMetadata`, and `DeleteSchema` calls against the region
you name. Rough total per full 21-scenario run: **200–600 Glue
control-plane calls**, comfortably inside the Glue free tier at the time
of writing. Kafka cost is zero (testcontainers is ephemeral, or the
external broker is already yours). JVM sidecar cost is zero (spawned
locally and torn down at exit).

The 5-second abort banner exists so you can Ctrl-C before the demo
touches AWS if you invoked it against the wrong account or region.

## Post-run cleanup verification

Every schema the demo creates is per-run-namespaced with the prefix
`gsr-ts-it-` and tracked in a `CleanupTracker` BEFORE the `CreateSchema`
call. On exit (success, failure, or signal) `CleanupTracker.run()`
deletes in reverse insertion order via `DeleteSchema`, tolerating
`EntityNotFoundException`. The Kafka topics the demo produces to are
testcontainers ephemera dropped when the broker stops (or best-effort
`deleteTopics` when an external broker is reused).

To sanity-check that nothing leaked, list schemas in `default-registry`
and grep for the runbook prefix after the demo exits:

```
aws glue list-schemas \
  --registry-id RegistryName=default-registry \
  --region <r> \
  --query 'Schemas[?starts_with(SchemaName, `gsr-ts-it-`)].SchemaName' \
  --output text
```

The expected output is **empty**. Any name printed is a leaked schema
(a prior crashed run or a mid-run kill that outran the signal handler).
Delete by hand:

```
aws glue delete-schema \
  --schema-id RegistryName=default-registry,SchemaName=<leaked-name> \
  --region <r>
```

Do NOT run `delete-registry` against `default-registry`.

## Troubleshooting

- **`demo:interop:real: refusing to run — neither AWS_PROFILE nor AWS_ACCESS_KEY_ID is set`**
  — the wrapper script hard-fails before touching AWS. Set one of the
  two env vars.
- **`SKIP demo-interop — sidecar JAR not built (run npm run build:sidecar)`**
  — build the fat JAR first with
  `(cd packages/integration-tests && npm run build:sidecar)` and re-run.
- **`SKIP demo-interop — JVM not available`** — set
  `GSR_INTEROP_JAVA=/absolute/path/to/java` or install a JDK 17+ on PATH.
- **`GSR_INTEROP_PARTNER=<other> is not supported`** — loud error. Set
  `GSR_INTEROP_PARTNER=java` or leave it unset.
- **Sidecar hangs on start** — the launcher gives the JVM 120 seconds
  to print its `PORT: <n>` first-line handshake and answer `/health`.
  If it hangs, run the JAR manually to see the stack trace:
  ```
  java -jar packages/integration-tests/java-interop/target/java-interop-sidecar.jar --port=0
  ```
- **`ThrottlingException`** — Glue rate-limits `CreateSchema`. Wait a
  minute and retry; the demo is single-threaded so backing off is the
  fix.
- **`ExpiredTokenException`** — refresh SSO: `aws sso login --profile <p>`.
- **`Docker daemon not running`** — start Docker or supply
  `KAFKA_BROKER=<host:port>` pointing at an external broker; the demo
  reuses it instead of starting testcontainers.
- **Leaked `gsr-ts-it-*` schemas from a prior crashed run** — clean
  them up before invoking; otherwise the run's `CreateSchema` calls
  succeed but your account's schema count grows.

## When to use this vs. sibling scripts

- `npm test` — Tier-1 unit only. Free. No network, no Docker, no AWS.
  The demo's two pure narrator + fixture unit tests run here.
- `npm run test:integ` — Tier-2 fake-Glue + testcontainers Kafka. Free.
  Interop suites loud-skip under `GSR_GLUE!=real`.
- `npm run test:integ:real` — Tier-3 real-Glue round-trip. Bills AWS,
  no Java sidecar involved.
- `npm run test:integ:interop:real` — cross-language interop tier as
  terse vitest PASS/FAIL. Bills AWS, spawns the Java sidecar, drives
  real Kafka.
- `npm run demo:interop:real` — this runbook. Bills AWS, spawns the
  Java sidecar, drives real Kafka, prints a full narrated log of every
  wire-byte round-trip. Use when you want a stakeholder-facing
  walkthrough of the same interop the sibling script proves via
  PASS/FAIL.

## Cross-references

- **Cross-language interop tier runbook** — [`../README-INTEROP.md`](../README-INTEROP.md).
- **Tier-3 real-Glue runbook** — [`../README-REAL-AWS.md`](../README-REAL-AWS.md).
- **Sidecar module README** — [`../java-interop/README.md`](../java-interop/README.md).
  Sidecar HTTP contract, prereqs, and build details.
- **Tiered README** — [`../README.md`](../README.md). The full env-gate
  matrix and directory layout.
