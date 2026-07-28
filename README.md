# AWS Glue Schema Registry — TypeScript client

A TypeScript / npm implementation of the [AWS Glue Schema Registry
(GSR)](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
client, wire-compatible with the canonical Java and Go GSR clients.

**Supported formats:** Avro, Protobuf, JSON-Schema (Draft-07). Every
message uses the same 18-byte wire header as the Java canonical client,
so a TypeScript producer and a Java consumer (or vice versa)
round-trip on the shared wire format. Byte-identity is proven offline
against a committed set of Java-canonical golden vectors — see
[`multilang-schema-registry/testdata/golden-java/PROVENANCE.md`](./multilang-schema-registry/testdata/golden-java/PROVENANCE.md).

**Node version:** 20 or newer.

**License:** Apache-2.0.

## Repository layout

```
.
├── LICENSE
├── README.md                                                # this file
├── .github/workflows/typescript-ci.yml                      # public CI
└── multilang-schema-registry/
    ├── PROVENANCE.md                                        # vendored-trees provenance
    ├── fixtures.manifest.json                               # SHA-256 manifest for shared/test + testdata/golden-java
    ├── scripts/verify-fixtures.mjs                          # CI: cross-checks fixtures against manifest
    ├── shared/test/                                         # Avro / Protobuf / JSON-Schema source-schemas + instance data (shared corpus)
    ├── testdata/golden-java/                                # 18 Java-canonical wire-byte golden vectors + PROVENANCE
    └── typescript/                                          # THE TypeScript client — npm workspaces monorepo
        ├── package.json                                     # workspaces root
        ├── tsconfig.base.json, tsconfig.json, ...
        ├── vitest.config.ts, vitest.integration.config.ts, vitest.bench.config.ts
        ├── examples/                                        # runnable code examples (type-checked by CI)
        ├── scripts/                                         # verify-pack, check-no-ai-artifacts, ...
        └── packages/
            ├── core/                                        # @gsr/core — wire-format, compression, cache, Glue seam
            ├── serde/                                       # @gsr/serde — Avro / Protobuf / JSON-Schema serde on top of @gsr/core
            ├── integration-tests/                           # tiered integration + interop harness (private)
            │   └── java-interop/                            # Java sidecar the interop suites drive
            └── PUBLISHING.md                                # release runbook (private-registry and public-npm paths)
```

## Getting started

From the `multilang-schema-registry/typescript/` directory:

```
npm install         # install all workspace deps
npm run typecheck   # tsc --noEmit across all packages
npm test            # vitest run across all packages (unit + offline byte-identity gate)
npm run build       # build @gsr/core + @gsr/serde
```

Test runner is [vitest](https://vitest.dev). This project does not use
jest.

## Consumers — start here

If you are looking to serialize or deserialize records, jump straight
to the [`@gsr/serde`
README](./multilang-schema-registry/typescript/packages/serde/README.md).
It has the install command, a copy-pasteable Avro round-trip, per-format
guides for Avro / Protobuf / JSON-Schema, compression, schema-evolution,
and the IAM permissions this client needs against real Glue.

Runnable examples that both READMEs excerpt from live under
[`examples/`](./multilang-schema-registry/typescript/examples/) and are
type-checked by `npm run docs:examples:typecheck`.

## Publishing

See
[`multilang-schema-registry/typescript/packages/PUBLISHING.md`](./multilang-schema-registry/typescript/packages/PUBLISHING.md)
for the release runbook — covers publishing to a private npm registry
and to the public npm registry, prerequisites, and recommended
pre-release gates.
