//go:build integration

// Phase 4.16 Layer C — Java<->Go cross-language interop with shared Avro
// fixtures.
//
// Exercises the multi-version .avsc evolution fixtures (shared/test/avro/)
// through both directions of the Java sidecar <-> Go client pipeline via a
// real Kafka broker. Only compatibility modes that test evolution are used:
// backward, forward, full (dropped: disabled, none — they don't constrain
// evolution and therefore don't add cross-language interop value).
//
// For each mode:
//   1. Reads .avsc files from the fixture directory (sorted alphabetically).
//   2. Registers all versions in Glue via the Java sidecar (implicit
//      registration via /kafka-produce to a throwaway topic).
//   3. Java -> Go: Java produces v1, Go consumes + deserializes.
//   4. Go -> Java: Go produces v1, Java consumes + deserializes.
//
// THIS BILLS AWS. Gated by AWS_INTEGRATION=1 + GSR_GLUE=real +
// GSR_INTEROP_MODE=local.
//
// NO production-code changes. NO sidecar extensions.

package integration_tests

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	hambaavro "github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
	gsravro "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// TestFixtureAvroInterop_Real exercises Java<->Go Avro interop using the
// shared multilang .avsc fixtures for evolution modes backward/forward/full.
func TestFixtureAvroInterop_Real(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-fixture-interop-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	basePath := fixtureAvroBasePath(t)

	// Only modes that test evolution (not disabled/none).
	modes := []string{"backward", "forward", "full"}

	for _, mode := range modes {
		mode := mode
		t.Run("fixture-avro-interop/"+mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()

			modeDir := filepath.Join(basePath, mode)
			_, statErr := os.Stat(modeDir)
			require.NoError(t, statErr, "mode directory must exist: %s", modeDir)

			// Collect .avsc files sorted alphabetically (v1, v2, ...).
			entries, readErr := os.ReadDir(modeDir)
			require.NoError(t, readErr, "reading mode directory %s", modeDir)

			var avscFiles []string
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".avsc") {
					avscFiles = append(avscFiles, e.Name())
				}
			}
			require.True(t, len(avscFiles) >= 2,
				"%s must have at least 2 .avsc files for evolution testing, found %d", mode, len(avscFiles))
			sort.Strings(avscFiles)

			// Unique schema name for this mode run.
			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-fixture-interop-" + mode + "-" + suffix
			cleanup.TrackSchema("default-registry", schemaName)

			compat := glueCompatMode[mode]
			require.NotEmpty(t, compat, "unknown compatibility mode: %s", mode)

			// Read all schema versions.
			schemas := make([]string, 0, len(avscFiles))
			for _, fileName := range avscFiles {
				avscPath := filepath.Join(modeDir, fileName)
				avscBytes, err := os.ReadFile(avscPath)
				require.NoError(t, err, "reading %s", avscPath)
				schemas = append(schemas, string(avscBytes))
			}

			// Register all schema versions via Java sidecar /kafka-produce to
			// throwaway topics. This exercises the Java library's
			// CreateSchema + RegisterSchemaVersion path with the appropriate
			// compatibility setting.
			for i, schemaDef := range schemas {
				throwawayTopic := schemaName + "-reg-" + avscFiles[i]

				// Parse the schema to build a sample record envelope for Java.
				parsed, parseErr := hambaavro.Parse(schemaDef)
				require.NoError(t, parseErr, "parse %s/%s", mode, avscFiles[i])
				record := buildSampleRecord(parsed)
				require.NotNil(t, record, "buildSampleRecord nil for %s/%s", mode, avscFiles[i])

				// Build the Java Avro envelope: {"fields": {...}}
				javaRecord := map[string]any{
					"fields": convertAvroRecordForJava(record),
				}

				_, prodErr := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
					Format:        "AVRO",
					Schema:        schemaDef,
					SchemaName:    schemaName,
					Record:        javaRecord,
					Compression:   "NONE",
					Bootstrap:     broker.Bootstrap,
					Topic:         throwawayTopic,
					Region:        real.Region,
					Compatibility: compat,
				})
				require.NoError(t, prodErr, "register schema version %d via Java (%s/%s)",
					i+1, mode, avscFiles[i])
			}

			// Now exercise both directions using v1 (the first schema version).
			v1Schema := schemas[0]
			v1Parsed, parseErr := hambaavro.Parse(v1Schema)
			require.NoError(t, parseErr, "parse v1 for interop (%s)", mode)
			v1Record := buildSampleRecord(v1Parsed)
			require.NotNil(t, v1Record, "buildSampleRecord nil for v1 (%s)", mode)

			// Direction: Java -> Go
			t.Run("java-to-go/v1", func(t *testing.T) {
				dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
				defer dirCancel()

				topic := schemaName + "-j2g"

				// Java produces a v1 record.
				javaRecord := map[string]any{
					"fields": convertAvroRecordForJava(v1Record),
				}
				_, prodErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
					Format:        "AVRO",
					Schema:        v1Schema,
					SchemaName:    schemaName,
					Record:        javaRecord,
					Compression:   "NONE",
					Bootstrap:     broker.Bootstrap,
					Topic:         topic,
					Region:        real.Region,
					Compatibility: compat,
				})
				require.NoError(t, prodErr, "Java produce v1 (%s)", mode)

				// Go consumes and deserializes.
				framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

				cfg := buildAvroInteropConfig(real.Region, "NONE")
				des, desErr := deserializer.NewDeserializer(cfg)
				require.NoError(t, desErr, "NewDeserializer (%s)", mode)
				t.Cleanup(func() { _ = des.Close() })

				got, desErr := des.Deserialize(topic, framed)
				require.NoError(t, desErr, "Deserialize (%s)", mode)

				// Assert decoded map has all v1 fields.
				gotMap, ok := got.(map[string]interface{})
				require.True(t, ok, "expected map[string]interface{}, got %T", got)
				for key := range v1Record {
					_, exists := gotMap[key]
					require.True(t, exists,
						"field %q missing from Go-decoded record (%s)", key, mode)
				}
			})

			// Direction: Go -> Java
			t.Run("go-to-java/v1", func(t *testing.T) {
				dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
				defer dirCancel()

				// Go serializer's DefaultSchemaNameStrategy uses topic as schema
				// name. Use the same schemaName so it finds the pre-registered
				// schema via GetSchemaByDefinition.
				topic := schemaName + "-g2j"

				// We need to pre-register under topic name for Go's lookup.
				// Register via Java to the exact topic name so
				// GetSchemaByDefinition resolves.
				javaRecord := map[string]any{
					"fields": convertAvroRecordForJava(v1Record),
				}
				_, regErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
					Format:        "AVRO",
					Schema:        v1Schema,
					SchemaName:    topic, // register under topic name
					Record:        javaRecord,
					Compression:   "NONE",
					Bootstrap:     broker.Bootstrap,
					Topic:         topic + "-reg",
					Region:        real.Region,
					Compatibility: compat,
				})
				require.NoError(t, regErr, "pre-register for Go->Java (%s)", mode)
				cleanup.TrackSchema("default-registry", topic)

				// Go serializes the v1 record.
				cfg := buildAvroInteropConfig(real.Region, "NONE")
				ser, serErr := serializer.NewSerializer(cfg)
				require.NoError(t, serErr, "NewSerializer (%s)", mode)
				t.Cleanup(func() { _ = ser.Close() })

				goRecord := &gsravro.AvroRecord{
					Schema: v1Schema,
					Data:   v1Record,
				}
				framed, serErr := ser.Serialize(topic, goRecord)
				require.NoError(t, serErr, "Serialize (%s)", mode)
				require.NotEmpty(t, framed)

				// Produce to Kafka.
				produceOne(t, dirCtx, broker.Bootstrap, topic, framed)

				// Java consumes and deserializes.
				resp, consumeErr := sc.KafkaConsume(dirCtx, javasidecar.KafkaConsumeRequest{
					Bootstrap: broker.Bootstrap,
					Topic:     topic,
					Format:    "AVRO",
					Region:    real.Region,
					TimeoutMs: 60_000,
				})
				require.NoError(t, consumeErr, "Java consume (%s)", mode)
				require.Equal(t, "AVRO", resp.DataFormat)

				// Assert the Java envelope contains the v1 fields.
				fields, ok := resp.Record["fields"].(map[string]any)
				require.True(t, ok, "Java envelope missing fields map (%s): %v", mode, resp.Record)
				for key := range v1Record {
					_, exists := fields[key]
					require.True(t, exists,
						"field %q missing from Java-decoded record (%s)", key, mode)
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildAvroInteropConfig builds a Go Configuration for Avro generic
// serialization/deserialization with real Glue.
func buildAvroInteropConfig(region, compression string) *common.Configuration {
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   compression,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey:  gsrMap,
		common.DataFormatTypeKey: common.DataFormatAvro,
		common.AvroRecordTypeKey: common.AvroRecordTypeGeneric,
	}
	return common.NewConfiguration(configMap)
}

// convertAvroRecordForJava converts a Go sample record map into a form
// suitable for the Java sidecar's Avro envelope. The Java GenericRecord
// builder expects integer fields as JSON numbers (not Go int which marshals
// to float64). We pass through as-is since json.Marshal handles Go numeric
// types correctly.
func convertAvroRecordForJava(record map[string]interface{}) map[string]any {
	out := make(map[string]any, len(record))
	for k, v := range record {
		switch val := v.(type) {
		case map[string]interface{}:
			// Nested record — recurse.
			out[k] = convertAvroRecordForJava(val)
		default:
			out[k] = val
		}
	}
	return out
}
