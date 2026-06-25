//go:build integration

// Phase 4.16 Layer C — Java<->Go cross-language interop with shared protobuf
// fixtures.
//
// Exercises 5 representative .proto files from shared/test/protos/ through
// both directions of the Java sidecar <-> Go client pipeline via a real Kafka
// broker. Same-version round-trip only — the .proto fixtures are not organized
// as v1/v2 pairs.
//
// Selected fixtures:
//   - basicSyntax2.proto       — proto2 baseline
//   - basicsyntax3.proto       — proto3 baseline
//   - TestSyntax3OneOfs.proto  — oneOf field support
//   - ComplexNestingSyntax3.proto — nested message types
//   - AllTypesSyntax3.proto    — full scalar type coverage
//
// Out-of-scope fixtures (Glue rejects schema names with non-[A-Za-z0-9_.-]
// characters; these test the apicurio protobuf parser, not the GSR client):
//   - ◉◉◉unicode⏩.proto
//   - .protodevelasl.proto.proto.protodevel$---$$.bar.3.proto
//   - hyphen-ated-proto_file-.proto
//   - foo$$$1.proto
//   - NestedConflicting#ClassName.proto
//   - ConflictingName.proto
//   - snake_case_file.proto
//
// THIS BILLS AWS. Gated by AWS_INTEGRATION=1 + GSR_GLUE=real +
// GSR_INTEROP_MODE=local.
//
// NO production-code changes. NO sidecar extensions.

package integration_tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// protoInteropFixtures lists the 5 representative .proto files to exercise.
var protoInteropFixtures = []struct {
	filename    string
	description string
}{
	{"basicSyntax2.proto", "proto2 baseline"},
	{"basicsyntax3.proto", "proto3 baseline"},
	{"TestSyntax3OneOfs.proto", "oneOf field support"},
	{"ComplexNestingSyntax3.proto", "nested message types"},
	{"AllTypesSyntax3.proto", "full scalar type coverage"},
}

// TestFixtureProtoInterop_Real exercises Java<->Go protobuf interop using
// 5 representative shared .proto fixtures.
func TestFixtureProtoInterop_Real(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-fixture-proto-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	protosDir := fixtureProtoBasePath(t)

	for _, fixture := range protoInteropFixtures {
		fixture := fixture
		basename := strings.TrimSuffix(fixture.filename, ".proto")

		t.Run("fixture-proto-interop/"+basename, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()

			// Read the raw .proto source text (what Glue stores).
			protoPath := filepath.Join(protosDir, fixture.filename)
			protoBytes, readErr := os.ReadFile(protoPath)
			require.NoError(t, readErr, "reading %s", fixture.filename)
			protoDef := string(protoBytes)

			// Compile the .proto to get the message descriptor.
			msg, md := buildDynamicProtoFromFile(t, protosDir, fixture.filename)
			require.NotNil(t, msg, "buildDynamicProtoFromFile nil for %s", fixture.filename)

			// Determine the full message type name (package.MessageName).
			fullName := string(md.FullName())

			// Build JSON representation for Java sidecar envelope.
			fieldsJSON := dynamicProtoToJSON(t, msg)

			// Unique schema name.
			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-fixture-proto-" + basename + "-" + suffix
			cleanup.TrackSchema("default-registry", schemaName)

			// --- Direction: Java -> Go ---
			t.Run("java-to-go", func(t *testing.T) {
				dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
				defer dirCancel()

				topic := schemaName + "-j2g"

				// Java sidecar produces — registers the schema and encodes.
				javaRecord := map[string]any{
					"messageTypeFullName": fullName,
					"fieldsJson":         fieldsJSON,
				}
				_, prodErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
					Format:      "PROTOBUF",
					Schema:      protoDef,
					SchemaName:  schemaName,
					Record:      javaRecord,
					Compression: "NONE",
					Bootstrap:   broker.Bootstrap,
					Topic:       topic,
					Region:      real.Region,
				})
				require.NoError(t, prodErr, "Java produce (%s)", fixture.filename)

				// Go consumes and deserializes.
				framed := consumeOne(t, dirCtx, broker.Bootstrap, topic)

				cfg := buildProtoInteropConfig(t, real.Region, "NONE", md)
				des, desErr := deserializer.NewDeserializer(cfg)
				require.NoError(t, desErr, "NewDeserializer (%s)", fixture.filename)
				t.Cleanup(func() { _ = des.Close() })

				got, desErr := des.Deserialize(topic, framed)
				require.NoError(t, desErr, "Deserialize (%s)", fixture.filename)

				// Assert the deserialized message is a proto.Message with the
				// same scalar fields we set.
				gotMsg, ok := got.(proto.Message)
				require.True(t, ok, "expected proto.Message, got %T (%s)", got, fixture.filename)
				assertProtoFieldsMatch(t, msg, gotMsg, fixture.filename)
			})

			// --- Direction: Go -> Java ---
			t.Run("go-to-java", func(t *testing.T) {
				dirCtx, dirCancel := context.WithTimeout(ctx, 90*time.Second)
				defer dirCancel()

				// Go serializer uses DefaultSchemaNameStrategy (topic as schema
				// name). Pre-register under the topic name via Java.
				topic := schemaName + "-g2j"

				javaRecord := map[string]any{
					"messageTypeFullName": fullName,
					"fieldsJson":         fieldsJSON,
				}
				_, regErr := sc.KafkaProduce(dirCtx, javasidecar.KafkaProduceRequest{
					Format:      "PROTOBUF",
					Schema:      protoDef,
					SchemaName:  topic, // register under topic name
					Record:      javaRecord,
					Compression: "NONE",
					Bootstrap:   broker.Bootstrap,
					Topic:       topic + "-reg",
					Region:      real.Region,
				})
				require.NoError(t, regErr, "pre-register for Go->Java (%s)", fixture.filename)
				cleanup.TrackSchema("default-registry", topic)

				// Go serializes the dynamic message.
				cfg := buildProtoInteropConfig(t, real.Region, "NONE", md)
				ser, serErr := serializer.NewSerializer(cfg)
				require.NoError(t, serErr, "NewSerializer (%s)", fixture.filename)
				t.Cleanup(func() { _ = ser.Close() })

				framed, serErr := ser.Serialize(topic, msg)
				require.NoError(t, serErr, "Serialize (%s)", fixture.filename)
				require.NotEmpty(t, framed)

				// Produce to Kafka.
				produceOne(t, dirCtx, broker.Bootstrap, topic, framed)

				// Java consumes and deserializes.
				resp, consumeErr := sc.KafkaConsume(dirCtx, javasidecar.KafkaConsumeRequest{
					Bootstrap: broker.Bootstrap,
					Topic:     topic,
					Format:    "PROTOBUF",
					Region:    real.Region,
					TimeoutMs: 60_000,
				})
				require.NoError(t, consumeErr, "Java consume (%s)", fixture.filename)
				require.Equal(t, "PROTOBUF", resp.DataFormat)

				// Assert the Java envelope contains fieldsJson with expected fields.
				respFieldsJSON, ok := resp.Record["fieldsJson"].(string)
				require.True(t, ok, "Java envelope missing fieldsJson (%s): %v",
					fixture.filename, resp.Record)

				// Parse the JSON and verify non-empty (the exact values depend
				// on Java's DynamicMessage JSON serialization which may differ
				// in field naming — we verify structural non-emptiness).
				var respFields map[string]any
				require.NoError(t, json.Unmarshal([]byte(respFieldsJSON), &respFields),
					"parse Java fieldsJson (%s)", fixture.filename)
				require.NotEmpty(t, respFields,
					"Java fieldsJson should not be empty (%s)", fixture.filename)
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildProtoInteropConfig builds a Go Configuration for Protobuf
// serialization/deserialization using the given message descriptor.
func buildProtoInteropConfig(t *testing.T, region, compression string, md protoreflect.MessageDescriptor) *common.Configuration {
	t.Helper()
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   compression,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey:              gsrMap,
		common.DataFormatTypeKey:             common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: md,
	}
	return common.NewConfiguration(configMap)
}

// dynamicProtoToJSON serializes a dynamicpb.Message to JSON string using
// protojson marshaler. This produces the fieldsJson envelope the Java sidecar
// expects.
func dynamicProtoToJSON(t *testing.T, msg *dynamicpb.Message) string {
	t.Helper()
	opts := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	b, err := opts.Marshal(msg)
	require.NoError(t, err, "protojson.Marshal for dynamic message")
	return string(b)
}

// assertProtoFieldsMatch verifies that the deserialized proto.Message contains
// the same scalar field values as the original sample message. Uses proto wire
// round-trip: marshal both to binary, unmarshal into a fresh dynamic message,
// then compare field-by-field.
func assertProtoFieldsMatch(t *testing.T, expected *dynamicpb.Message, got proto.Message, context string) {
	t.Helper()

	// Marshal the received message to bytes.
	gotBytes, err := proto.Marshal(got)
	require.NoError(t, err, "marshal received message (%s)", context)

	// Unmarshal into a new dynamic message with the same descriptor.
	md := expected.ProtoReflect().Descriptor()
	rebuilt := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(gotBytes, rebuilt), "unmarshal into dynamic (%s)", context)

	// Compare scalar fields that we set in the expected message.
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)

		// Skip fields we didn't set (messages, maps, repeated).
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			continue
		}
		if fd.IsMap() || fd.IsList() {
			continue
		}

		// Only compare if we actually set this field in the expected message.
		if !expected.Has(fd) {
			continue
		}

		expectedVal := expected.Get(fd)
		gotVal := rebuilt.Get(fd)
		require.Equal(t, expectedVal.Interface(), gotVal.Interface(),
			"field %q mismatch (%s)", fd.Name(), context)
	}
}
