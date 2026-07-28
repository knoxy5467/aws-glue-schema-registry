# Monorepo provenance — vendored trees

The following subtrees of `multilang-schema-registry/` are vendored into
this repository:

- `shared/test/` — Avro / protobuf / JSON-Schema source-schemas and JSON
  instance data shared across every language client. Every TypeScript
  encode-slice, byte-identity, and fixture-sweep test reads from here.
- `testdata/golden-java/` — 18 Java-canonical wire-format `.bin` vectors
  + their sidecar `.json` descriptors + `PROVENANCE.md` (the
  human-readable oracle). Regenerable via the `golden-gen-java` command
  captured in the file above.

The vectors and shared source-schemas were captured against the
reference GSR monorepo — see
[`testdata/golden-java/PROVENANCE.md`](./testdata/golden-java/PROVENANCE.md)
for the reference-repo commit SHA the byte vectors were generated at.

## Machine-verified integrity

`multilang-schema-registry/fixtures.manifest.json` records the SHA-256
of every file under `shared/test/` and `testdata/golden-java/`. The
`scripts/verify-fixtures.mjs` script recomputes and diffs the manifest;
CI (`.github/workflows/typescript-ci.yml`) runs it on every push. Any
drift — missing file, extra file, or content change without a manifest
update — fails the build.

## Regenerating

- To refresh the machine-readable manifest after any intentional
  fixture change:
  `node multilang-schema-registry/scripts/verify-fixtures.mjs --write`

  The Java `golden-gen-java` tool that regenerates the `.bin` byte
  vectors themselves lives in the reference GSR monorepo (see
  [`testdata/golden-java/PROVENANCE.md`](./testdata/golden-java/PROVENANCE.md)).
  It is not shipped here.
