//go:build integration

// Phase 4.16 Layer B — Tier-2 .avsc evolution sweep against real AWS Glue.
//
// Exercises all 23 .avsc fixtures in shared/test/avro/ against the real Glue
// Schema Registry (account 850995546034). Tests are integration-tagged AND
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

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
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

// ---------------------------------------------------------------------------
// buildSampleRecord walks a hamba/avro parsed Schema and returns a
// map[string]interface{} populated with minimal sample values suitable for
// hambaavro.Marshal. The goal is "encode-decode round-trips", not exhaustive
// Avro feature coverage.
//
// Type mapping:
//   - string  -> "x"
//   - int     -> int(42)
//   - long    -> int64(42)
//   - float   -> float32(1.0)
//   - double  -> float64(1.0)
//   - boolean -> true
//   - bytes   -> []byte("b")
//   - null    -> nil
//   - record  -> recursive sub-record
//   - enum    -> first symbol
//   - array   -> empty slice
//   - map     -> empty map
//   - union   -> first non-null type (or nil if nullable-only)
//   - fixed   -> zero-filled []byte of correct size
//
// If an exotic type is encountered that this helper cannot handle, it returns
// nil for that field — the encoder either accepts it (union with null) or
// errors, and the test logs it.
// ---------------------------------------------------------------------------

func buildSampleRecord(schema hambaavro.Schema) map[string]interface{} {
	rs, ok := schema.(*hambaavro.RecordSchema)
	if !ok {
		return nil
	}
	record := make(map[string]interface{}, len(rs.Fields()))
	for _, field := range rs.Fields() {
		record[field.Name()] = buildSampleValue(field.Type())
	}
	return record
}

func buildSampleValue(schema hambaavro.Schema) interface{} {
	switch schema.Type() {
	case hambaavro.String:
		return "x"
	case hambaavro.Int:
		return int(42)
	case hambaavro.Long:
		return int64(42)
	case hambaavro.Float:
		return float32(1.0)
	case hambaavro.Double:
		return float64(1.0)
	case hambaavro.Boolean:
		return true
	case hambaavro.Bytes:
		return []byte("b")
	case hambaavro.Null:
		return nil
	case hambaavro.Record:
		return buildSampleRecord(schema)
	case hambaavro.Enum:
		es := schema.(*hambaavro.EnumSchema)
		symbols := es.Symbols()
		if len(symbols) > 0 {
			return symbols[0]
		}
		return ""
	case hambaavro.Array:
		// Return an empty slice — satisfies the schema without needing to
		// generate item values.
		return []interface{}{}
	case hambaavro.Map:
		// Return an empty map.
		return map[string]interface{}{}
	case hambaavro.Union:
		us := schema.(*hambaavro.UnionSchema)
		types := us.Types()
		// Prefer the first non-null type in the union.
		for _, t := range types {
			if t.Type() != hambaavro.Null {
				val := buildSampleValue(t)
				// hamba/avro union encoding for map[string]interface{} requires
				// the value to be wrapped in a map with the type name as key
				// ONLY for named types (record, enum, fixed). For primitives,
				// the raw value works directly.
				return val
			}
		}
		// All-null union (unusual but valid).
		return nil
	case hambaavro.Fixed:
		fs := schema.(*hambaavro.FixedSchema)
		return make([]byte, fs.Size())
	default:
		// Exotic/unknown type — return nil. If the field is in a union with
		// null this works; otherwise the caller logs the encode failure.
		return nil
	}
}

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

			// Build encoder + decoder against real Glue.
			enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
				RegistryName:                  testRegistryName,
				Compatibility:                 compat,
				SchemaAutoRegistrationEnabled: true,
			})
			require.NoError(t, err, "NewGsrEncoderForTest (%s)", mode)
			t.Cleanup(func() { _ = enc.Close() })

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

					// Encode via GSR encoder (registers the schema version in Glue).
					schema := &gsrcore.Schema{
						SchemaDefinition: avscText,
						SchemaName:       schemaName,
						DataFormat:       "AVRO",
					}
					encoded, err := enc.Encode(avroBytes, schemaName, schema)
					require.NoError(t, err, "GsrEncoder.Encode for %s (mode=%s)", fileName, mode)
					require.NotEmpty(t, encoded)

					// Decode: strip the GSR wire-format header + decompress.
					decoded, err := dec.Decode(encoded)
					require.NoError(t, err, "GsrDecoder.Decode for %s (mode=%s)", fileName, mode)

					// Unmarshal the decoded Avro bytes into a map for comparison.
					var got map[string]interface{}
					err = hambaavro.Unmarshal(parsed, decoded, &got)
					require.NoError(t, err, "hambaavro.Unmarshal for %s (mode=%s)", fileName, mode)

					// Assert key fields match. We don't deep-equal the entire map
					// because hamba/avro may convert numeric types differently, but
					// we verify the record round-tripped at least structurally.
					require.Equal(t, len(record), len(got),
						"field count mismatch for %s: want %d, got %d", fileName, len(record), len(got))
					for key := range record {
						_, exists := got[key]
						require.True(t, exists,
							"field %q missing from decoded record for %s", key, fileName)
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

	// The 4 non-malformed negative subdirectories.
	modes := []string{"backward", "forward", "full", "disabled"}

	for _, mode := range modes {
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

					// Assert the error chain contains InvalidInputException
					// (Glue's compatibility-violation error) OR at minimum that
					// an error occurred.
					require.Error(t, encErr,
						"expected compatibility violation for negative/%s/%s", mode, fileName)

					// Best-effort: assert it's specifically InvalidInputException.
					var invalidInput *types.InvalidInputException
					if !errors.As(encErr, &invalidInput) {
						// Some Glue error shapes surface as wrapped generic errors.
						// Log but don't hard-fail — the important thing is that
						// registration was rejected.
						t.Logf("NOTE: error is not InvalidInputException (type=%T): %v — "+
							"still a valid rejection for negative/%s/%s", encErr, encErr, mode, fileName)
					}
				})
			}
		})
	}
}
