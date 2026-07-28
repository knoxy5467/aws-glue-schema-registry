# AWS Glue Schema Registry — TypeScript Client

TypeScript / npm implementation of the AWS Glue Schema Registry (GSR) client, wire-compatible with the Java (canonical) and Go references that live as sibling packages in this monorepo.

## Consumers — start here

If you are looking to serialize or deserialize records, jump straight to the [`@gsr/serde` README](./packages/serde/README.md). It has the install command, a copy-pasteable Avro round-trip, per-format guides for Avro / Protobuf / JSON-Schema, compression, schema-evolution, and the IAM permissions this client needs against real Glue.

- **Supported formats:** Avro, Protobuf, and JSON-Schema (Draft-07). Every message uses the same 18-byte wire header as the Java canonical, so a TS producer and a Java consumer (or vice versa) round-trip on the shared wire format. Tested byte-identity is proven offline against a committed set of Java-canonical golden vectors — see the [evidence-scope note in `@gsr/core`](./packages/core/README.md#evidence-of-byte-identity) for the exact record/message shapes and capture provenance. Wire-compatibility with the Go reference client is asserted architecturally (identical header layout and compression byte discriminants) rather than by an offline byte-identity vector set.
- **Node version:** 20 or newer.
- **Distribution:** Publish paths (private npm registry and public npm registry) are documented in [`packages/PUBLISHING.md`](./packages/PUBLISHING.md). Operator-gated; no automated publish runs from this repository.
- **Narrated interop demo:** `npm run demo:interop:real` walks 21 cross-language round-trips against real Glue + a Java sidecar + Kafka, printing each schema body, wire-byte hex dump, and PASS/FAIL line. See [`packages/integration-tests/demo/README.md`](./packages/integration-tests/demo/README.md).

## Layout

This directory is an npm-workspaces monorepo:

```
typescript/
├── package.json            # workspaces root (private)
├── tsconfig.base.json      # shared strict TypeScript config
├── tsconfig.json           # aggregate typecheck config
├── vitest.config.ts        # test runner config
├── .nvmrc                  # Node version floor
└── packages/
    ├── core/               # wire-format, compression, config, cache, errors, Glue seam
    ├── serde/              # Avro / Protobuf / JSON-Schema serializers/deserializers
    └── integration-tests/  # tiered integration + interop harness (private)
```

## Prerequisites

- Node.js **20 or newer** (`.nvmrc` pins the version). Use `nvm use` if you have `nvm` installed.
- npm 10+.

## Getting started

From this directory:

```
npm install         # install all workspace deps
npm run typecheck   # tsc --noEmit across all packages
npm test            # vitest run across all packages
```

Test runner is [vitest](https://vitest.dev). This project does not use jest.

## Documentation

This root README is the maintainer / monorepo front page. Customer-facing usage documentation lives in the per-package READMEs:

- [`@gsr/serde`](./packages/serde/README.md) — end-to-end quick-start (serialize → deserialize round-trip), per-format guides (Avro, Protobuf, JSON-Schema), compression, and schema-name strategy selection.
- [`@gsr/core`](./packages/core/README.md) — complete configuration-key reference, error taxonomy, and wire-format overview.

Runnable examples that both READMEs excerpt from live under [`examples/`](./examples/) and are type-checked by `npm run docs:examples:typecheck`. The rendered API reference (TypeDoc HTML) is produced by `npm run docs` and written to a git-ignored `docs/api/` directory.
