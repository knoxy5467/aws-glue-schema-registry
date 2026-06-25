# demo-interop

Narrated Java↔Go cross-language, cross-version interop demo for AWS Glue Schema
Registry. Exercises all three data formats (Avro, JSON Schema, Protobuf) in both
directions against real AWS Glue, with schema evolution (v1 producer, v2 consumer).

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Go | 1.21+ | `go version` |
| JDK | 11+ | Required to build the Java sidecar |
| Maven | 3.9+ | `mvn --version` — see `java-interop/README.md` |
| Docker | 20+ | testcontainers-go starts a Kafka broker in-process |
| AWS credentials | — | Account `850995546034`, region `us-east-2` |

The `make demo-interop` target builds the Java sidecar JAR/image
(`integration-tests/java-interop/target/java-interop-sidecar.jar`) automatically
via the `java-sidecar-build` prerequisite.

If `default-registry` does not yet exist in your AWS account, create it once:
```bash
aws glue create-registry --registry-name default-registry --region us-east-2
```
See `integration-tests/REAL-AWS-RUNBOOK.md` for the full one-time setup checklist.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_PROFILE` | required* | — | Named profile in `~/.aws/config` |
| `AWS_ACCESS_KEY_ID` | required* | — | Alternative to `AWS_PROFILE` |
| `AWS_SECRET_ACCESS_KEY` | — | — | Companion to `AWS_ACCESS_KEY_ID` |
| `AWS_REGION` | recommended | `us-east-2` | Glue region |
| `DEMO_REGISTRY` | optional | `default-registry` | Registry name |

\* Either `AWS_PROFILE` **or** `AWS_ACCESS_KEY_ID` must be set; the Makefile
  target checks and errors early if neither is present.

## Invocation

**Preferred — Makefile target (builds sidecar JAR first):**
```bash
make demo-interop
```

**With transcript capture:**
```bash
make demo-interop 2>&1 | tee demo.log
```

**Manual — more control over environment variables:**
```bash
cd integration-tests
go run -tags integration ./cmd/demo-interop/
```

**With explicit region and registry:**
```bash
AWS_REGION=us-east-2 DEMO_REGISTRY=default-registry \
  make demo-interop 2>&1 | tee demo.log
```

## What the Demo Does

1. Starts a Kafka broker via testcontainers-go and the Java sidecar HTTP service.
2. For each format (Avro, JSON Schema, Protobuf):
   - Registers v1 schema with AWS Glue; narrates the schema body and Glue version-id.
   - Registers v2 schema (adds an optional field); narrates the evolution.
   - **Java → Go:** Java sidecar serializes a v1 record (via `GlueSchemaRegistryKafkaSerializer`);
     Go deserializer consumes it and narrates hex wire bytes, GSR header, UUID, and decoded fields.
   - **Go → Java:** Go serializer produces a v1 record; Java sidecar deserializes and reports
     decoded values back via the sidecar HTTP API.
   - Cross-version evolution: producer uses v1, consumer reads with v2 registered — compatibility verified.
3. Cleans up all `demo-4.15-*` schemas from Glue on exit (success **or** failure).

Exit code 0 = all scenarios passed. Non-zero = at least one scenario failed (error printed to stderr).

## Verifying Cleanup

After the demo completes, verify no schemas were leaked:
```bash
aws glue list-schemas --registry-id RegistryName=default-registry \
  --query 'Schemas[?starts_with(SchemaName, `demo-4.15-`)].SchemaName' \
  --region us-east-2 --output text
```
Empty output means no leaks. If schemas remain (e.g. demo was killed mid-run),
delete them manually or re-run the demo (it will delete on exit).
