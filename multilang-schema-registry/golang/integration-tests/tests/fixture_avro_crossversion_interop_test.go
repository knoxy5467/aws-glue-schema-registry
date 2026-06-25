//go:build integration

// Phase 4.17 Item D — Cross-version Avro interop exercising reader-schema
// projection (Item A) in a live Java<->Go round-trip with real Kafka and
// real Glue.
//
// Registers multi-version .avsc fixture lineages in Glue, then:
//   - Java produces data encoded against one schema version.
//   - Go consumes using AvroReaderSchema set to a DIFFERENT version.
//   - Avro resolution rules project the payload: fields absent in the writer
//     get their declared defaults, fields absent in the reader are dropped.
//
// Direction-per-mode rationale (Avro resolution requires reader-side fields
// absent from the writer to have defaults):
//   - backward: v2/v3 reader reads v1 writer (new fields have defaults)
//   - forward:  v1 reader reads v2 writer (removed fields have defaults in v1)
//   - full:     v2 reader reads v1 writer (bidirectional with defaults)
//
// Go->Java direction is same-version v1 only — Java reader-schema projection
// is a Java-side concern and not a parity proof for the Go client.
//
// THIS BILLS AWS. Gated by AWS_INTEGRATION=1 + GSR_GLUE=real +
// GSR_INTEROP_MODE=local.
//
// NO production-code changes. Reuses existing sidecar plumbing.

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

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/realglue"
	gsravro "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// TestFixtureAvroCrossVersionInterop_Real exercises cross-version Avro
// reader-schema projection in a live Java<->Go interop loop.
//
// For backward mode: Java produces v1, Go reads with v2 reader -> v2-added
// fields get their declared defaults. Additional cell: Go reads v1 with v3
// reader -> both v2-added and v3-added fields get defaults.
//
// For forward mode: Java produces v2, Go reads with v1 reader -> v2-added
// fields are dropped, v1-only fields absent in writer get defaults.
//
// For full mode: Java produces v1, Go reads with v2 reader -> v1-only
// fields absent in reader are dropped, v2-added fields get defaults.
func TestFixtureAvroCrossVersionInterop_Real(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-fixture-crossversion-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	basePath := fixtureAvroBasePath(t)

	// Only modes that enforce evolution constraints.
	modes := []string{"backward", "forward", "full"}

	for _, mode := range modes {
		mode := mode
		t.Run("fixture-avro-crossversion/"+mode, func(t *testing.T) {
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
				"%s must have at least 2 .avsc files for cross-version testing, found %d",
				mode, len(avscFiles))
			sort.Strings(avscFiles)

			// Unique schema name for this mode run.
			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-fixture-crossversion-" + mode + "-" + suffix
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

			// Register all schema versions via Java sidecar (establishes the
			// evolution lineage in Glue under the correct compat mode).
			for i, schemaDef := range schemas {
				throwawayTopic := schemaName + "-reg-" + avscFiles[i]

				parsed, parseErr := hambaavro.Parse(schemaDef)
				require.NoError(t, parseErr, "parse %s/%s", mode, avscFiles[i])
				record := buildSampleRecord(parsed)
				require.NotNil(t, record, "buildSampleRecord nil for %s/%s", mode, avscFiles[i])

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

			// --- Cross-version cells per mode ---
			switch mode {
			case "backward":
				crossVersionBackward(t, ctx, sc, broker, real, schemaName, compat, schemas)
			case "forward":
				crossVersionForward(t, ctx, sc, broker, real, schemaName, compat, schemas)
			case "full":
				crossVersionFull(t, ctx, sc, broker, real, schemaName, compat, schemas)
			}

			// --- Go -> Java direction: same-version v1 only ---
			// Java reader-schema projection is a Java-side concern; this
			// direction confirms Go can serialize v1 and Java reads it back.
			t.Run("go-to-java/v1", func(t *testing.T) {
				dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
				defer dirCancel()

				v1Schema := schemas[0]
				v1Parsed, parseErr := hambaavro.Parse(v1Schema)
				require.NoError(t, parseErr, "parse v1 for go-to-java (%s)", mode)
				v1Record := buildSampleRecord(v1Parsed)
				require.NotNil(t, v1Record, "buildSampleRecord nil for v1 (%s)", mode)

				topic := schemaName + "-g2j"
				cleanup.TrackSchema("default-registry", topic)

				// Pre-register schema under topic name for Go's lookup.
				javaRecord := map[string]any{
					"fields": convertAvroRecordForJava(v1Record),
				}
				_, regErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
					Format:        "AVRO",
					Schema:        v1Schema,
					SchemaName:    topic,
					Record:        javaRecord,
					Compression:   "NONE",
					Bootstrap:     broker.Bootstrap,
					Topic:         topic + "-reg",
					Region:        real.Region,
					Compatibility: compat,
				})
				require.NoError(t, regErr, "pre-register for Go->Java (%s)", mode)

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
// Cross-version cell: backward mode
// v2 reader reads v1 writer: "status" gets default "registered"
// v3 reader reads v1 writer: "status" gets "registered", "age" gets 0
// ---------------------------------------------------------------------------

func crossVersionBackward(
	t *testing.T,
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	real *realglue.Real,
	schemaName, compat string,
	schemas []string,
) {
	t.Helper()

	v1Schema := schemas[0]
	v2Schema := schemas[1]

	// --- v1 -> v2 cell ---
	t.Run("java-to-go/v1-to-v2", func(t *testing.T) {
		dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
		defer dirCancel()

		topic := schemaName + "-j2g-v1v2"

		// Java produces a v1 record.
		v1Parsed, parseErr := hambaavro.Parse(v1Schema)
		require.NoError(t, parseErr, "parse v1")
		v1Record := buildSampleRecord(v1Parsed)
		require.NotNil(t, v1Record)

		javaRecord := map[string]any{"fields": convertAvroRecordForJava(v1Record)}
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
		require.NoError(t, prodErr, "Java produce v1 (backward)")

		// Go consumes with v2 reader schema.
		framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

		cfg := buildAvroReaderSchemaConfig(real.Region, "NONE", v2Schema)
		des, desErr := deserializer.NewDeserializer(cfg)
		require.NoError(t, desErr, "NewDeserializer with v2 reader")
		t.Cleanup(func() { _ = des.Close() })

		got, desErr := des.Deserialize(topic, framed)
		require.NoError(t, desErr, "Deserialize with v2 reader")

		gotMap, ok := got.(map[string]interface{})
		require.True(t, ok, "expected map[string]interface{}, got %T", got)

		// v1 fields preserved in v2 reader.
		require.Equal(t, v1Record["id"], gotMap["id"], "id field")
		require.Equal(t, v1Record["name"], gotMap["name"], "name field")
		require.Equal(t, v1Record["active"], gotMap["active"], "active field")

		// v2-added field gets default "registered".
		require.Equal(t, "registered", gotMap["status"],
			"v2 reader should fill 'status' default from v1 writer data")

		// v1's "email" field (dropped in v2) should NOT be in the decoded map.
		_, hasEmail := gotMap["email"]
		require.False(t, hasEmail,
			"v2 reader should drop 'email' field absent from v2 schema")
	})

	// --- v1 -> v3 cell (D.2) ---
	if len(schemas) >= 3 {
		v3Schema := schemas[2]
		t.Run("java-to-go/v1-to-v3", func(t *testing.T) {
			dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
			defer dirCancel()

			topic := schemaName + "-j2g-v1v3"

			v1Parsed, parseErr := hambaavro.Parse(v1Schema)
			require.NoError(t, parseErr, "parse v1")
			v1Record := buildSampleRecord(v1Parsed)
			require.NotNil(t, v1Record)

			javaRecord := map[string]any{"fields": convertAvroRecordForJava(v1Record)}
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
			require.NoError(t, prodErr, "Java produce v1 (backward v1->v3)")

			framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

			cfg := buildAvroReaderSchemaConfig(real.Region, "NONE", v3Schema)
			des, desErr := deserializer.NewDeserializer(cfg)
			require.NoError(t, desErr, "NewDeserializer with v3 reader")
			t.Cleanup(func() { _ = des.Close() })

			got, desErr := des.Deserialize(topic, framed)
			require.NoError(t, desErr, "Deserialize with v3 reader")

			gotMap, ok := got.(map[string]interface{})
			require.True(t, ok, "expected map[string]interface{}, got %T", got)

			// v1 fields preserved in v3.
			require.Equal(t, v1Record["id"], gotMap["id"], "id field")
			require.Equal(t, v1Record["name"], gotMap["name"], "name field")
			require.Equal(t, v1Record["active"], gotMap["active"], "active field")

			// v2-added and v3-added fields get their defaults.
			require.Equal(t, "registered", gotMap["status"],
				"v3 reader should fill 'status' default from v1 writer data")
			require.Equal(t, int(0), gotMap["age"],
				"v3 reader should fill 'age' default (0) from v1 writer data")

			// v1's "email" should be dropped.
			_, hasEmail := gotMap["email"]
			require.False(t, hasEmail,
				"v3 reader should drop 'email' field absent from v3 schema")
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-version cell: forward mode
// v1 reader reads v2 writer: "priority" gets default "normal" (present in v1,
// absent in v2); v2's "status" field is dropped (absent from v1 reader).
// ---------------------------------------------------------------------------

func crossVersionForward(
	t *testing.T,
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	real *realglue.Real,
	schemaName, compat string,
	schemas []string,
) {
	t.Helper()

	v1Schema := schemas[0]
	v2Schema := schemas[1]

	// Forward compat: old reader reads new writer.
	// Java produces v2, Go reads with v1 reader schema.
	t.Run("java-to-go/v2-to-v1", func(t *testing.T) {
		dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
		defer dirCancel()

		topic := schemaName + "-j2g-v2v1"

		// Java produces a v2 record.
		v2Parsed, parseErr := hambaavro.Parse(v2Schema)
		require.NoError(t, parseErr, "parse v2")
		v2Record := buildSampleRecord(v2Parsed)
		require.NotNil(t, v2Record)

		javaRecord := map[string]any{"fields": convertAvroRecordForJava(v2Record)}
		_, prodErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
			Format:        "AVRO",
			Schema:        v2Schema,
			SchemaName:    schemaName,
			Record:        javaRecord,
			Compression:   "NONE",
			Bootstrap:     broker.Bootstrap,
			Topic:         topic,
			Region:        real.Region,
			Compatibility: compat,
		})
		require.NoError(t, prodErr, "Java produce v2 (forward)")

		// Go consumes with v1 reader schema.
		framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

		cfg := buildAvroReaderSchemaConfig(real.Region, "NONE", v1Schema)
		des, desErr := deserializer.NewDeserializer(cfg)
		require.NoError(t, desErr, "NewDeserializer with v1 reader")
		t.Cleanup(func() { _ = des.Close() })

		got, desErr := des.Deserialize(topic, framed)
		require.NoError(t, desErr, "Deserialize with v1 reader")

		gotMap, ok := got.(map[string]interface{})
		require.True(t, ok, "expected map[string]interface{}, got %T", got)

		// Fields present in both v1 and v2.
		require.Equal(t, v2Record["id"], gotMap["id"], "id field")
		require.Equal(t, v2Record["total"], gotMap["total"], "total field")
		require.Equal(t, v2Record["currency"], gotMap["currency"], "currency field")

		// v1's "priority" field (removed in v2) gets its v1 default "normal".
		require.Equal(t, "normal", gotMap["priority"],
			"v1 reader should fill 'priority' default from v2 writer data")

		// v2's "status" field should NOT appear (absent from v1 reader).
		_, hasStatus := gotMap["status"]
		require.False(t, hasStatus,
			"v1 reader should drop 'status' field absent from v1 schema")
	})
}

// ---------------------------------------------------------------------------
// Cross-version cell: full mode
// v2 reader reads v1 writer: "notifications" gets default true; "theme"
// (v1-only) is dropped.
// ---------------------------------------------------------------------------

func crossVersionFull(
	t *testing.T,
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	real *realglue.Real,
	schemaName, compat string,
	schemas []string,
) {
	t.Helper()

	v1Schema := schemas[0]
	v2Schema := schemas[1]

	t.Run("java-to-go/v1-to-v2", func(t *testing.T) {
		dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
		defer dirCancel()

		topic := schemaName + "-j2g-v1v2"

		// Java produces a v1 record.
		v1Parsed, parseErr := hambaavro.Parse(v1Schema)
		require.NoError(t, parseErr, "parse v1")
		v1Record := buildSampleRecord(v1Parsed)
		require.NotNil(t, v1Record)

		javaRecord := map[string]any{"fields": convertAvroRecordForJava(v1Record)}
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
		require.NoError(t, prodErr, "Java produce v1 (full)")

		// Go consumes with v2 reader schema.
		framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

		cfg := buildAvroReaderSchemaConfig(real.Region, "NONE", v2Schema)
		des, desErr := deserializer.NewDeserializer(cfg)
		require.NoError(t, desErr, "NewDeserializer with v2 reader")
		t.Cleanup(func() { _ = des.Close() })

		got, desErr := des.Deserialize(topic, framed)
		require.NoError(t, desErr, "Deserialize with v2 reader")

		gotMap, ok := got.(map[string]interface{})
		require.True(t, ok, "expected map[string]interface{}, got %T", got)

		// Fields preserved in both v1 and v2.
		require.Equal(t, v1Record["username"], gotMap["username"], "username field")
		require.Equal(t, v1Record["verified"], gotMap["verified"], "verified field")

		// v2-added "notifications" field gets its default true.
		require.Equal(t, true, gotMap["notifications"],
			"v2 reader should fill 'notifications' default from v1 writer data")

		// v1's "theme" field (removed in v2) should NOT appear in v2 reader output.
		_, hasTheme := gotMap["theme"]
		require.False(t, hasTheme,
			"v2 reader should drop 'theme' field absent from v2 schema")
	})
}

// ---------------------------------------------------------------------------
// Helper: build config with reader schema for cross-version deserialization
// ---------------------------------------------------------------------------

func buildAvroReaderSchemaConfig(region, compression, readerSchema string) *common.Configuration {
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   compression,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey:     gsrMap,
		common.DataFormatTypeKey:    common.DataFormatAvro,
		common.AvroRecordTypeKey:    common.AvroRecordTypeGeneric,
		common.AvroReaderSchemaKey:  readerSchema,
	}
	return common.NewConfiguration(configMap)
}
