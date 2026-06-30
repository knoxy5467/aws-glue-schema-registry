//go:build integration

// Phase 4.16 Layer B — Tier-2 .avsc evolution sweep against real AWS Glue.
//
// Exercises all 23 .avsc fixtures in shared/test/avro/ against the real Glue
// Schema Registry (a configured AWS account). Tests are integration-tagged AND
// gated by GSR_GLUE=real + AWS_INTEGRATION=1 (scenarioGate requiresReal=true).
//
// Test 1 (TestFixtureAvroEvolution_Real): for each positive compatibility mode
// subdirectory (backward, forward, full, disabled, none), registers all schema
// versions in alphabetical order under a fresh Glue schema name, encoding +
// decoding a sample record at each version.
//
// Test 2 (TestFixtureAvroNegativeEvolution_Real): for each non-malformed
// negative subdirectory, registers v1 successfully then asserts that v2+
// registration fails with an InvalidInputException (compatibility violation).
//
// NO Kafka. NO Java sidecar. NO production-code changes.

package integration_tests

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/core"
)

// ---------------------------------------------------------------------------
// Compatibility mode mapping
// ---------------------------------------------------------------------------

// glueCompatMode maps the fixture subdirectory name to the Glue API
// compatibility string. The encoder's GsrEncoderOptions.Compatibility field
// uses these exact-case strings (Java-parity).
var glueCompatMode = map[string]string{
	"backward": "BACKWARD",
	"forward":  "FORWARD",
	"full":     "FULL",
	"disabled": "DISABLED",
	"none":     "NONE",
}

// ---------------------------------------------------------------------------
// fixtureBasePath resolves the shared/test/avro directory from the test file
// location (integration-tests/tests/).
// ---------------------------------------------------------------------------

func fixtureAvroBasePath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join(".", "..", "..", "..", "shared", "test", "avro"))
	require.NoError(t, err, "resolving shared/test/avro path")
	_, err = os.Stat(p)
	require.NoError(t, err, "shared/test/avro directory must exist: %s", p)
	return p
}

// buildSampleRecord and buildSampleValue are defined in fixture_helpers_test.go
// (factored in Phase 4.16 Layer C to share across fixture tests).

// ---------------------------------------------------------------------------
// randomFixtureName generates a unique schema name for a fixture sweep.
// ---------------------------------------------------------------------------

func randomFixtureName(t *testing.T, mode string) string {
	t.Helper()
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("gsr-go-it-fixture-avro-%s-%x", mode, buf[:])
}

// ---------------------------------------------------------------------------
// Test 1: Positive evolution sweep
// ---------------------------------------------------------------------------

// TestFixtureAvroEvolution_Real exercises all positive .avsc fixtures against
// real Glue. For each compatibility mode subdirectory it registers schema
// versions in order, encoding + decoding a sample record at each version.
func TestFixtureAvroEvolution_Real(t *testing.T) {
	scenarioGate(t, true, false)
	h := newGlueHandle(t)

	basePath := fixtureAvroBasePath(t)

	// The 5 positive mode subdirectories.
	modes := []string{"backward", "forward", "full", "disabled", "none"}

	for _, mode := range modes {
		mode := mode
		t.Run("fixture-avro-evolution/"+mode, func(t *testing.T) {
			t.Parallel()

			modeDir := filepath.Join(basePath, mode)
			_, err := os.Stat(modeDir)
			require.NoError(t, err, "mode directory must exist: %s", modeDir)

			// Collect .avsc files.
			entries, err := os.ReadDir(modeDir)
			require.NoError(t, err, "reading mode directory %s", modeDir)

			var avscFiles []string
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".avsc") {
					avscFiles = append(avscFiles, e.Name())
				}
			}
			require.NotEmpty(t, avscFiles, "no .avsc files in %s", modeDir)

			// Sort alphabetically — v1 before v2, etc.
			sort.Strings(avscFiles)

			// Unique schema name for this mode.
			schemaName := randomFixtureName(t, mode)
			h.Cleanup.TrackSchema(testRegistryName, schemaName)

			compat := glueCompatMode[mode]
			require.NotEmpty(t, compat, "unknown compatibility mode: %s", mode)

			// Decoder can be shared across versions — it resolves via the
			// SchemaVersionId embedded in the GSR wire-format header, not
			// a local cache keyed on schema definition.
			dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
				RegistryName: testRegistryName,
			})
			require.NoError(t, err, "NewGsrDecoderForTest (%s)", mode)
			t.Cleanup(func() { _ = dec.Close() })

			for _, fileName := range avscFiles {
				fileName := fileName
				t.Run(fileName, func(t *testing.T) {
					// NOT parallel within a mode — schemas must register
					// in version order (v1, v2, v3...).
					avscPath := filepath.Join(modeDir, fileName)
					avscBytes, err := os.ReadFile(avscPath)
					require.NoError(t, err, "reading %s", avscPath)
					avscText := string(avscBytes)

					// Parse the schema to build a sample record.
					parsed, err := hambaavro.Parse(avscText)
					require.NoError(t, err, "parsing %s", fileName)

					record := buildSampleRecord(parsed)
					require.NotNil(t, record, "buildSampleRecord returned nil for %s", fileName)

					// Marshal the record to Avro binary using hamba/avro.
					avroBytes, err := hambaavro.Marshal(parsed, record)
					require.NoError(t, err, "hambaavro.Marshal for %s", fileName)

					// Fresh encoder per version: the encoder's internal schemaCache
					// is keyed on (schemaName, dataFormat) — NOT on the schema
					// definition body. If we reuse an encoder across v1/v2/v3, the
					// cache returns v1's UUID for all subsequent versions, silently
					// skipping registration of v2+ in Glue. The negative test
					// (line ~310) demonstrates the same pattern. See code-review
					// finding #1 (CRITICAL).
					enc, encErr := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
						RegistryName:                  testRegistryName,
						Compatibility:                 compat,
						SchemaAutoRegistrationEnabled: true,
					})
					require.NoError(t, encErr, "NewGsrEncoderForTest for %s (%s)", fileName, mode)
					defer func() { _ = enc.Close() }()

					// Encode via GSR encoder (registers the schema version in Glue).
					schema := &gsrcore.Schema{
						SchemaDefinition: avscText,
						SchemaName:       schemaName,
						DataFormat:       "AVRO",
					}
					encoded, encErr := enc.Encode(avroBytes, schemaName, schema)
					require.NoError(t, encErr, "GsrEncoder.Encode for %s (mode=%s)", fileName, mode)
					require.NotEmpty(t, encoded)

					// Decode: strip the GSR wire-format header + decompress.
					decoded, err := dec.Decode(encoded)
					require.NoError(t, err, "GsrDecoder.Decode for %s (mode=%s)", fileName, mode)

					// Unmarshal the decoded Avro bytes into a map for comparison.
					var got map[string]interface{}
					err = hambaavro.Unmarshal(parsed, decoded, &got)
					require.NoError(t, err, "hambaavro.Unmarshal for %s (mode=%s)", fileName, mode)

					// Value-by-value assertion: verify each key produced by
					// buildSampleRecord round-trips correctly. This catches
					// silent zero-fills and type drift that a structural-only
					// check (field presence) would miss.
					require.Equal(t, len(record), len(got),
						"field count mismatch for %s: want %d, got %d", fileName, len(record), len(got))
					for key, want := range record {
						gotVal, exists := got[key]
						require.True(t, exists,
							"field %q missing from decoded record for %s", key, fileName)
						require.Equal(t, want, gotVal,
							"field %q value mismatch for %s", key, fileName)
					}
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test 2: Negative evolution sweep
// ---------------------------------------------------------------------------

// TestFixtureAvroNegativeEvolution_Real exercises the negative .avsc fixtures
// against real Glue. For each non-malformed subdirectory under negative/,
// registers v1 successfully then asserts that subsequent versions are rejected
// by Glue's compatibility check (InvalidInputException).
//
// The negative/malformed/ subdirectory is skipped here — it contains schemas
// that fail local parsing (covered by Layer A's TestSharedFixtures_AvroParseSweep)
// and should never reach Glue.
func TestFixtureAvroNegativeEvolution_Real(t *testing.T) {
	scenarioGate(t, true, false)
	h := newGlueHandle(t)

	basePath := fixtureAvroBasePath(t)
	negativeDir := filepath.Join(basePath, "negative")

	// Compatibility-checked modes: Glue rejects incompatible versions via
	// InvalidInputException. We assert strictly on the error type.
	compatCheckedModes := []string{"backward", "forward", "full"}

	// DISABLED mode: Glue uses a different rejection code path (not
	// InvalidInputException). We assert only that a non-nil error is returned.
	// Separated from the compatCheckedModes to avoid the soft-fallback that
	// would vacuously pass on transient network errors. See code-review
	// finding #7.
	disabledModes := []string{"disabled"}

	allModes := append(compatCheckedModes, disabledModes...)

	for _, mode := range allModes {
		mode := mode
		t.Run("fixture-avro-negative/"+mode, func(t *testing.T) {
			t.Parallel()

			modeDir := filepath.Join(negativeDir, mode)
			_, err := os.Stat(modeDir)
			require.NoError(t, err, "negative mode directory must exist: %s", modeDir)

			// Collect .avsc files.
			entries, err := os.ReadDir(modeDir)
			require.NoError(t, err, "reading negative mode directory %s", modeDir)

			var avscFiles []string
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".avsc") {
					avscFiles = append(avscFiles, e.Name())
				}
			}
			require.True(t, len(avscFiles) >= 2,
				"negative/%s must have at least v1 + v2 .avsc files, found %d", mode, len(avscFiles))

			// Sort alphabetically — v1 first, then v2, v3...
			sort.Strings(avscFiles)

			// Unique schema name.
			schemaName := randomFixtureName(t, "neg-"+mode)
			h.Cleanup.TrackSchema(testRegistryName, schemaName)

			compat := glueCompatMode[mode]
			require.NotEmpty(t, compat, "unknown compatibility mode: %s", mode)

			// Build encoder for registration.
			enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
				RegistryName:                  testRegistryName,
				Compatibility:                 compat,
				SchemaAutoRegistrationEnabled: true,
			})
			require.NoError(t, err, "NewGsrEncoderForTest (negative/%s)", mode)
			t.Cleanup(func() { _ = enc.Close() })

			// Step 1: Register v1 successfully.
			v1Path := filepath.Join(modeDir, avscFiles[0])
			v1Bytes, err := os.ReadFile(v1Path)
			require.NoError(t, err, "reading v1: %s", v1Path)
			v1Text := string(v1Bytes)

			// Parse and build sample record for v1.
			v1Parsed, err := hambaavro.Parse(v1Text)
			require.NoError(t, err, "parsing v1: %s", avscFiles[0])

			v1Record := buildSampleRecord(v1Parsed)
			require.NotNil(t, v1Record, "buildSampleRecord returned nil for v1: %s", avscFiles[0])

			v1AvroBytes, err := hambaavro.Marshal(v1Parsed, v1Record)
			require.NoError(t, err, "hambaavro.Marshal for v1: %s", avscFiles[0])

			v1Schema := &gsrcore.Schema{
				SchemaDefinition: v1Text,
				SchemaName:       schemaName,
				DataFormat:       "AVRO",
			}
			_, err = enc.Encode(v1AvroBytes, schemaName, v1Schema)
			require.NoError(t, err, "v1 registration must succeed for negative/%s (%s)", mode, avscFiles[0])

			// Determine if this mode should assert InvalidInputException strictly.
			isCompatChecked := mode == "backward" || mode == "forward" || mode == "full"

			// Step 2: Attempt to register v2+ — each should be rejected.
			for _, fileName := range avscFiles[1:] {
				fileName := fileName
				t.Run(fileName, func(t *testing.T) {
					// NOT parallel — shares the same schema name state.
					avscPath := filepath.Join(modeDir, fileName)
					avscBytes, err := os.ReadFile(avscPath)
					require.NoError(t, err, "reading %s", avscPath)
					avscText := string(avscBytes)

					// Parse to build a sample record (needed for Marshal -> Encode).
					parsed, err := hambaavro.Parse(avscText)
					require.NoError(t, err, "parsing %s", fileName)

					record := buildSampleRecord(parsed)
					require.NotNil(t, record, "buildSampleRecord returned nil for %s", fileName)

					avroBytes, err := hambaavro.Marshal(parsed, record)
					require.NoError(t, err, "hambaavro.Marshal for %s", fileName)

					// Build a fresh encoder so the cache doesn't short-circuit
					// the Glue call for the same schema name with a different
					// definition.
					encV2, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
						RegistryName:                  testRegistryName,
						Compatibility:                 compat,
						SchemaAutoRegistrationEnabled: true,
					})
					require.NoError(t, err, "NewGsrEncoderForTest for %s", fileName)
					defer func() { _ = encV2.Close() }()

					incompatSchema := &gsrcore.Schema{
						SchemaDefinition: avscText,
						SchemaName:       schemaName,
						DataFormat:       "AVRO",
					}
					_, encErr := encV2.Encode(avroBytes, schemaName, incompatSchema)

					// Strict assertion: registration MUST fail.
					require.Error(t, encErr,
						"expected compatibility violation for negative/%s/%s", mode, fileName)

					if isCompatChecked {
						// For backward/forward/full modes, Glue rejects the
						// incompatible v2/v3 in one of two ways:
						//   1. SYNCHRONOUS — CreateSchema/RegisterSchemaVersion
						//      returns InvalidInputException at submission.
						//   2. ASYNCHRONOUS — the schema registers and gets a
						//      UUID, then Glue's compatibility checker
						//      transitions status PENDING → FAILURE. The Go
						//      GSR client's waitForSchemaEvolutionCheck poll
						//      (Phase 4.10 PBI-5) surfaces this as a wrapped
						//      ErrGSR whose message contains "schema evolution
						//      check failed".
						// Both paths prove Glue rejected the incompatible
						// version; the client just observes the rejection at
						// different points. Real-AWS empirically returns the
						// async form for negative/backward, /forward, /full
						// (Phase 4.16 first real-AWS run). Accept either.
						var invalidInput *types.InvalidInputException
						isInvalidInput := errors.As(encErr, &invalidInput)
						isEvolutionFail := strings.Contains(encErr.Error(),
							"schema evolution check failed")
						require.True(t, isInvalidInput || isEvolutionFail,
							"expected InvalidInputException OR evolution-check FAILURE for negative/%s/%s, got %T: %v",
							mode, fileName, encErr, encErr)
					}
					// For DISABLED mode, any non-nil error is sufficient —
					// Glue uses a different rejection code path.
				})
			}
		})
	}
}
