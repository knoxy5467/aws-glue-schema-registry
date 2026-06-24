# Changelog

## [Unreleased] — Phase 4.14

### secondaryDeserializer removed
The `secondaryDeserializer` config key and `Config.SecondaryDeserializer` field
have been removed. Java's fallback-deserializer chain is intentionally not
implemented in Go. Callers that receive non-GSR-framed data get a typed
`ErrIncompatibleData` (via `errors.Is(err, gsrcore.ErrIncompatibleData)`) and
may route to their own pre-existing decoder. See package docs on
`pkg/gsrserde-go/deserializer` for rationale.

### User-Agent identity
The Go client identifies itself as `glue-schema-registry-go/<version>` in the
HTTP User-Agent header (distinct from Java's
`aws-glue-schema-registry-java/<app>`). The `userAgentApp` config key sets the
`<version>` segment at construction time; the default is `"default"`. Runtime
mutation of the user-agent string (Java's `OverrideUserAgentApp`) is
intentionally not implemented. The construction-time config knob provides
sufficient flexibility for all known use cases.

### Out of scope (Phase 4.14)
The following items from the validation matrix (section 5.3) are documented as
NOT requiring real-AWS companion tests:

- Item 23 (IAM denied): Tier-1 + Tier-2 fake-backend coverage only. A real-AWS
  IAM-denied scenario would require a separate restricted role; deferred to
  canary infrastructure (Phase 5).
- Item 24 (Throttling): Tier-1 + Tier-2 fake-backend coverage only. Real Glue
  cannot reliably produce ThrottlingException in a deterministic test; deferred
  to canary harness (Phase 5).
- Items 26-29 (Malformed Avro/JSON/Protobuf, non-UTF-8 JSON, truncated
  payload, corrupt UUID): Kept at Tier-1 unit + Tier-2 fake-backend only.
  These are pure client-side decode paths where a real-AWS gate adds zero
  signal (the Glue service is not exercised in the failure path).

## Release 1.0.0
* Initial Release

## Release 1.0.1
* Added more documentation
* Reduced logging
* Added flexibility to schema naming
* Added Kinesis Data Streams usage examples
* Added integration tests

## Release 1.1.0
* Added Support for JSONSchema Format.
* Added Validation logic while using encode method for calls through KPL.
* Generalized Kafka Specific Serializer/Deserializer to a data format agnostic classes like 
GlueSchemaRegistryKafkaSerializer/GlueSchemaRegistryKafkaDeserializer.
* Generalized AWSKafkaAvroSerDe to GlueSchemaRegistryKafkaSerDe for it to be used for multiple data formats.
* Using better convention for poms and maven inheritance.
* Added JSON Kafka Converter.
* Improved integration tests to run with local dockerized streaming systems.

## Release 1.1.1
* Fixed checkstyle errors with maven build in integration-tests folder.
* Reduced number of Canaries tests.
* Removed jitpack as a repo for everit and using maven central to pull everit.

## Release 1.1.2
* Introduce cache to improve serialization performance
* Add DatumReader Cache to improve de-serialization performance
* Reduce logging
* Add additional examples of configuring Kafka Connect and clarification on what property names are expected
* Fix resource clean up in Kafka integration test

## Release 1.1.3
* Modify UserAgent to emit usage metrics
* Add tests to include key and value schemas both 

## Release 1.1.4
* Upgrade Apache Kafka version to 2.8.1

## Release 1.1.5
* Fix security vulnerability in transitive dependencies
* Remove configuration logging information

## Release 1.1.9
* Added Support for Protobuf Format
* Improved the caching mechanism to improve availability of the serializer and deserializer

## Release 1.1.10
* Fix bug for missing Protobuf wellknown types
* Fix Json schema converter NPEs due to missing connect.index and connect.type for sink only cases
* Add AWS SDK dependency to allow irsa service account

## Release 1.1.11
* Add support for Kafka Connect Protobuf converter

## Release 1.1.12
* Upgraded Avro Version to prevent a CVE

## Release 1.1.13
* Upgraded kotlin dependency versions to prevent a CVE

## Release 1.1.14
* Upgraded Protobuf dependency version to prevent a CVE
* Upgraded everit-json-schema dependency version to prevent a CVE

## Release 1.1.15
* Upgrade Avro, Apicurio and Localhost utils versions

## Release 1.1.16
* Upgraded Wire version
* Excluded some transitive dependencies that are having vulnerabilities

## Release 1.1.17
* Upgraded kafka dependencies version

## Release 1.1.18
* Add a dummy class in the serializer-deserializer-msk-iam module for javadoc and source jar generation
* Upgraded Avro and Json dependencies version
* Upgraded AWS SDK v1 and v2 versions to fix vulnerabilities

## Release 1.1.19
* Upgraded dependency versions to remove ION dependencies

## Release 1.1.20
* Upgrade the dependency version to remove commons:compress dependency

## Release 1.1.21
* Upgraded Avro dependencies version to fix vulnerabilities

## Release 1.1.22
* Upgraded protobuf dependencies version to fix vulnerabilities

## Release 1.1.23
* Upgraded json-schema dependencies version to fix vulnerabilities

## Release 1.1.24
* Upgraded square-wireschema version to fix vulnerabilities

## Release 1.1.25
* Upgraded aws-sdk version to fix vulnerabilities
