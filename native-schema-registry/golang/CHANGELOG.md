# Go Client Changelog

Changes to the GSR Go client live here. The top-level repository
`CHANGELOG.md` tracks Java releases.

## Unreleased

### Changed
- **Configuration:** `LoadConfigFromMap` now synthesizes a default value for
  `Config.Description` of `DEFAULT-DESCRIPTION-<region>-<registryName>` when
  the `description` key is absent or empty. Previously the field stayed blank.
  Mirrors Java `GlueSchemaRegistryConfiguration.java:343-352` and matches the
  description string the Glue console surfaces for Java callers. Empty region
  collapses the region segment to two consecutive dashes
  (`DEFAULT-DESCRIPTION--<registryName>`), matching Java. Callers that pass an
  explicit non-empty `description` are unaffected. (PBI-4.10-2)
