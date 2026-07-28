# Golden-vector provenance

These files are Java-canonical GSR wire-format byte vectors captured by
`golden-gen-java`. Each `.bin` file is the raw wire bytes; the sidecar
`.json` file records the format / record type / compression mode /
schema-version UUID / source-schema / provenance / SHA-256.

- Tool: `golden-gen-java` — a Java command that lives in the reference
  GSR monorepo (not vendored here). It uses the canonical Java GSR
  client to encode each fixture record + schema pair to wire bytes and
  writes the `.bin` / `.json` pair for every cell in the table below.
- Reference monorepo SHA: `54af0eed1adfd5beab428ec440f52619fd368d91`
- Captured at (this run): `2026-07-23T05:59:44.173715268Z`
- Fixed schema-version UUID: `01020304-0506-0708-090a-0b0c0d0e0f10`

Regenerate with the `golden-gen-java` tool from the reference monorepo:

```
mvn -q -f <reference-repo>/multilang-schema-registry/golang/integration-tests/cmd/golden-gen-java/pom.xml \
    -DskipTests package exec:java \
    -Doutput.dir=$PWD/multilang-schema-registry/testdata/golden-java \
    -Drepo.sha=$(git rev-parse HEAD)
```

## Captured cells

| Format | Record type | Compression | File | Bytes | SHA-256 |
|--------|-------------|-------------|------|------:|---------|
| avro | generic | NONE | `avro__generic__comp-NONE__test-v1.bin` | 25 | `ab3e33e2e16c720a91a98dc07f9351fd9c847185a8884b5c44ba8d464b7c6286` |
| avro | generic | ZLIB | `avro__generic__comp-ZLIB__test-v1.bin` | 33 | `4b109b2d510e7e61a95110311831e2a3316aae3717d817d0c0a907143f5d35c0` |
| avro | specific | NONE | `avro__specific__comp-NONE__test-v1.bin` | 25 | `ab3e33e2e16c720a91a98dc07f9351fd9c847185a8884b5c44ba8d464b7c6286` |
| avro | specific | ZLIB | `avro__specific__comp-ZLIB__test-v1.bin` | 33 | `4b109b2d510e7e61a95110311831e2a3316aae3717d817d0c0a907143f5d35c0` |
| protobuf | concrete | NONE | `protobuf__concrete__comp-NONE__stringvalue-hello.bin` | 26 | `6e44e9d9e4180008b691288c6f08d17ad4881513876fc546bc45e924dcb20bd5` |
| protobuf | concrete | ZLIB | `protobuf__concrete__comp-ZLIB__stringvalue-hello.bin` | 34 | `d64b9314351108b570bf2c22d4956344dc2d2975e89644d38dbccee41326d3fd` |
| protobuf | dynamic | NONE | `protobuf__dynamic__comp-NONE__stringvalue-hello.bin` | 26 | `6e44e9d9e4180008b691288c6f08d17ad4881513876fc546bc45e924dcb20bd5` |
| protobuf | dynamic | ZLIB | `protobuf__dynamic__comp-ZLIB__stringvalue-hello.bin` | 34 | `d64b9314351108b570bf2c22d4956344dc2d2975e89644d38dbccee41326d3fd` |
| jsonschema | draft07 | NONE | `jsonschema__draft07__comp-NONE__product-v1.bin` | 87 | `afee6c5d9ad99105d54fb49305fb973bd55ba031b4e73529bfe240304600d6e7` |
| jsonschema | draft07 | ZLIB | `jsonschema__draft07__comp-ZLIB__product-v1.bin` | 94 | `1384fa11c75631d3ff28ac85bd9e7ef6c3092168d416276e170e1737812eb231` |
| jsonschema | draft07 | NONE | `jsonschema__draft07__comp-NONE__customer-v1.bin` | 153 | `e562e9c8a89288931d9340d42594b26f58ff43227cb0df92a39b5cdb61d9fadd` |
| jsonschema | draft07 | ZLIB | `jsonschema__draft07__comp-ZLIB__customer-v1.bin` | 140 | `07548d832423fd62e67b680d3afe08e6d8ded653ea4ad408b60762e1dc882533` |
| jsonschema | draft07 | NONE | `jsonschema__draft07__comp-NONE__invoice-v1.bin` | 218 | `37d6b181f5069442ad85224732f1dc75a4ca1a736d358e1a8e00ff659d323ee7` |
| jsonschema | draft07 | ZLIB | `jsonschema__draft07__comp-ZLIB__invoice-v1.bin` | 167 | `6a759f17e1d82b6282ffe4202f243bfba16f6029222dea4f23966704ae0f9e4e` |
| jsonschema | draft07 | NONE | `jsonschema__draft07__comp-NONE__event-v1.bin` | 137 | `c2caaba5597636fb8f9d1e0f8383b43a32949f9d403fc33f168cb51bfc78ec4d` |
| jsonschema | draft07 | ZLIB | `jsonschema__draft07__comp-ZLIB__event-v1.bin` | 131 | `a13a9c57ec058d74bd7415c9b6f59fcc16935b6d940deab76474cb56b7cf8f11` |
| jsonschema | draft07 | NONE | `jsonschema__draft07__comp-NONE__single.bin` | 49 | `f65491387027be1c1ce01a02b8123da26a0a4d2ea4ab86500a39a5b224bd4d1f` |
| jsonschema | draft07 | ZLIB | `jsonschema__draft07__comp-ZLIB__single.bin` | 57 | `3b66e2da7a15514db36d296fe4e8c47743d8179541ae374bc00a553b48cb3f63` |

## Uncovered cells

Every `format × {0x00, 0x05}` cell not captured above is recorded here
as explicitly uncovered by this tool run (no silent gaps).

_None — all enumerated cells (Avro Generic/Specific, Protobuf concrete/Dynamic,
JSON-Schema Draft-07) × {NONE, ZLIB} are captured above._

## Evidence scope of the "byte-identical wire format" claim

Read the captured-cells table as evidence of exact wire-byte parity **on
the shapes captured above**, at the pinned reference-monorepo SHA. The
scope is intentionally narrow and easy to audit:

- **18 golden cells → 14 distinct SHA-256 vectors.** Avro Generic and
  Avro Specific share bytes (the wire payload is identical; only the
  reader-side type varies), and Protobuf concrete and Protobuf dynamic
  share bytes for the same reason — so the 18 rows above collapse to
  14 distinct wire-byte vectors.
- **Avro coverage.** One record shape (`avro-test-v1.avsc` under
  `multilang-schema-registry/shared/test/avro/`), exercised under
  Generic and Specific reader types × {NONE, ZLIB}.
- **Protobuf coverage.** One message type
  (`google.protobuf.StringValue`, wire payload `"hello"`), exercised
  under concrete and dynamic reader types × {NONE, ZLIB}. This message
  sits at a non-zero index inside `wrappers.proto` so the message-index
  varint prefix is exercised, but it is a single-field
  (`string value = 1`) message — multi-field field-ordering divergence
  from the Java canonical is NOT probed by these vectors.
- **JSON-Schema coverage.** Five Draft-07 schema shapes
  (`product-v1`, `customer-v1`, `invoice-v1`, `event-v1`, `single`),
  each × {NONE, ZLIB}.
- **Capture timing.** Vectors were captured once, on
  `2026-07-23T05:59:44Z`, at the pinned reference-monorepo SHA above.
  They are committed alongside this file. The offline byte-identity
  gate re-runs against these committed bytes — it is not a live Java
  run per gate invocation. Live Java parity is only exercised by the
  Tier-3 interop demo (`packages/integration-tests/demo/`).

Broadening the evidence (a second Avro record shape, a multi-field
Protobuf message, or a fresh golden capture) is straightforward — the
`golden-gen-java` tool documented above regenerates the whole set from
whatever shapes the Java canonical accepts. Anything not listed here
is **outside the byte-identity oracle** on this tree.

