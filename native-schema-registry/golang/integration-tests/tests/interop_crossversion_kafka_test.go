//go:build integration

// Phase 4.13 — Cross-language cross-version interop.
//
// Proves that a record serialized by one language at schema version N
// deserializes correctly in the other language when the schema name carries a
// BACKWARD-evolved successor version.  Bridges Phase 4.6.5 (cross-language
// same-version) and Phase 4.12 (cross-version within Go).
//
// Matrix: 3 formats × 2 compressions × 2 directions = 12 sub-tests.
//
//	Cell A (TestInterop_CrossVersion_JavaProduce_GoConsume):
//	  Java sidecar registers v1 with BACKWARD compat, produces to Kafka at v1.
//	  Test registers v2 via a second /kafka-produce to a throwaway topic.
//	  Go deserializer reads from Kafka — must decode v1 bytes even though v2
//	  is present in the registry.
//
//	Cell B (TestInterop_CrossVersion_GoProduce_JavaConsume):
//	  Both schema versions registered via Java sidecar (throwaway topics).
//	  Go serializer encodes at v1 and produces to Kafka.
//	  Java sidecar /kafka-consume decodes — must recover v1 fields correctly.
//
// THIS BILLS AWS.  Gated by AWS_INTEGRATION=1 + GSR_GLUE=real +
// GSR_INTEROP_MODE=local.  Cleanup prefix: gsr-go-it-xver- (distinct from
// the Phase 4.6.5 prefix to avoid cleanup interference).

package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
	gsravro "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
)

// ---------------------------------------------------------------------------
// Schema constants — v1 and v2 for AVRO, JSON Schema, and PROTOBUF.
// ---------------------------------------------------------------------------

const crossVersionAvroV1 = `{
	"type": "record",
	"name": "CrossVersionRecord",
	"namespace": "test",
	"fields": [
		{"name": "id",   "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "age",  "type": "int"}
	]
}`

const crossVersionAvroV2 = `{
	"type": "record",
	"name": "CrossVersionRecord",
	"namespace": "test",
	"fields": [
		{"name": "id",    "type": "string"},
		{"name": "name",  "type": "string"},
		{"name": "age",   "type": "int"},
		{"name": "email", "type": ["null", "string"], "default": null}
	]
}`

const crossVersionJSONV1 = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"type": "object",
	"properties": {
		"id":   {"type": "string"},
		"name": {"type": "string"},
		"age":  {"type": "integer"}
	},
	"required": ["id", "name", "age"],
	"additionalProperties": false
}`

const crossVersionJSONV2 = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"type": "object",
	"properties": {
		"id":    {"type": "string"},
		"name":  {"type": "string"},
		"age":   {"type": "integer"},
		"email": {"type": ["string", "null"]}
	},
	"required": ["id", "name", "age"],
	"additionalProperties": false
}`

const crossVersionProtoV1 = `syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
`

const crossVersionProtoV2 = `syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
`

// ---------------------------------------------------------------------------
// Test matrix
// ---------------------------------------------------------------------------

// crossVersionCase is one (format, compression) cell in the 6-entry matrix.
// Each cell supplies v1/v2 schema definitions, record builders for both sides,
// and assertion callbacks for each consumption direction.
type crossVersionCase struct {
	name        string
	format      string
	compression string
	v1Schema    string
	v2Schema    string
	// goV1Record builds the v1 payload for Go-side serialization (Serialize).
	goV1Record func() interface{}
	// javaV1Record builds the JSON record envelope for Java sidecar /kafka-produce.
	javaV1Record func() map[string]any
	// goCheck validates the Go-deserialized v1 payload (Cell A assertion).
	goCheck func(t *testing.T, got interface{})
	// javaEnvelopeCheck validates the Java-deserialized v1 envelope (Cell B assertion).
	javaEnvelopeCheck func(t *testing.T, env map[string]any)
}

const xverID = "xver-id"
const xverName = "xver-name"
const xverAge = 99

// crossVersionMatrix returns the 6 matrix cells (3 formats × 2 compressions).
// Exact subtest names required by AC-12:
//
//	AVRO_NONE, AVRO_ZLIB, JSON_NONE, JSON_ZLIB, PROTOBUF_NONE, PROTOBUF_ZLIB
func crossVersionMatrix() []crossVersionCase {
	jsonPayload := mustJSON(map[string]any{"id": xverID, "name": xverName, "age": xverAge})

	// --- AVRO ---

	avroGoRecord := func() interface{} {
		return &gsravro.AvroRecord{
			Schema: crossVersionAvroV1,
			Data: map[string]any{
				"id":   xverID,
				"name": xverName,
				"age":  int32(xverAge),
			},
		}
	}
	avroJavaRecord := func() map[string]any {
		return map[string]any{
			"fields": map[string]any{
				"id":   xverID,
				"name": xverName,
				"age":  xverAge,
			},
		}
	}
	avroGoCheck := func(t *testing.T, got interface{}) {
		t.Helper()
		m, ok := got.(map[string]interface{})
		require.True(t, ok, "AVRO consumer: expected map[string]any, got %T", got)
		require.Equal(t, xverID, m["id"])
		require.Equal(t, xverName, m["name"])
		age, ok := m["age"].(int32)
		require.True(t, ok, "AVRO age: expected int32, got %T (%v)", m["age"], m["age"])
		require.Equal(t, int32(xverAge), age)
	}
	avroJavaCheck := func(t *testing.T, env map[string]any) {
		t.Helper()
		fields, ok := env["fields"].(map[string]any)
		require.True(t, ok, "AVRO envelope: missing fields map (env=%v)", env)
		require.Equal(t, xverID, fields["id"])
		require.Equal(t, xverName, fields["name"])
		age, ok := fields["age"].(float64)
		require.True(t, ok, "AVRO age: expected float64 from JSON, got %T (%v)", fields["age"], fields["age"])
		require.Equal(t, xverAge, int(age))
	}

	// --- JSON Schema ---

	jsonGoCheck := func(t *testing.T, got interface{}) {
		t.Helper()
		s, ok := got.(string)
		require.True(t, ok, "JSON consumer: expected string payload, got %T", got)
		xverAssertJSONFields(t, s)
	}
	jsonJavaCheck := func(t *testing.T, env map[string]any) {
		t.Helper()
		payload, _ := env["payload"].(string)
		xverAssertJSONFields(t, payload)
	}

	// --- PROTOBUF ---

	protoGoRecord := func() interface{} {
		msg, err := buildDynamicProtoMessage(crossVersionProtoV1, map[string]interface{}{
			"id":   xverID,
			"name": xverName,
			"age":  int32(xverAge),
		})
		if err != nil {
			panic(fmt.Sprintf("buildDynamicProtoMessage: %v", err))
		}
		return msg
	}
	protoJavaRecord := func() map[string]any {
		return map[string]any{
			"messageTypeFullName": "test.CrossVersionMessage",
			"fieldsJson": mustJSON(map[string]any{
				"id":   xverID,
				"name": xverName,
				"age":  xverAge,
			}),
		}
	}
	protoGoCheck := func(t *testing.T, got interface{}) {
		t.Helper()
		m, ok := got.(proto.Message)
		require.True(t, ok, "PROTOBUF consumer: expected proto.Message, got %T", got)
		// Marshal → unmarshal into a zero-valued dynamicpb built from v1 schema
		// so we can access fields without depending on the compiled testpb type.
		raw, err := proto.Marshal(m)
		require.NoError(t, err)
		ref, err := buildDynamicProtoMessage(crossVersionProtoV1, nil)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(raw, ref))
		refRefl := ref.ProtoReflect()
		md := refRefl.Descriptor()
		require.Equal(t, xverID, refRefl.Get(md.Fields().ByName("id")).String())
		require.Equal(t, xverName, refRefl.Get(md.Fields().ByName("name")).String())
		require.Equal(t, int32(xverAge), int32(refRefl.Get(md.Fields().ByName("age")).Int()))
	}
	protoJavaCheck := func(t *testing.T, env map[string]any) {
		t.Helper()
		fieldsJSON, ok := env["fieldsJson"].(string)
		require.True(t, ok, "PROTOBUF envelope: missing fieldsJson (env=%v)", env)
		xverAssertJSONFields(t, fieldsJSON)
	}

	return []crossVersionCase{
		{
			name:              "AVRO_NONE",
			format:            "AVRO",
			compression:       "NONE",
			v1Schema:          crossVersionAvroV1,
			v2Schema:          crossVersionAvroV2,
			goV1Record:        avroGoRecord,
			javaV1Record:      avroJavaRecord,
			goCheck:           avroGoCheck,
			javaEnvelopeCheck: avroJavaCheck,
		},
		{
			name:              "AVRO_ZLIB",
			format:            "AVRO",
			compression:       "ZLIB",
			v1Schema:          crossVersionAvroV1,
			v2Schema:          crossVersionAvroV2,
			goV1Record:        avroGoRecord,
			javaV1Record:      avroJavaRecord,
			goCheck:           avroGoCheck,
			javaEnvelopeCheck: avroJavaCheck,
		},
		{
			name:        "JSON_NONE",
			format:      "JSON",
			compression: "NONE",
			v1Schema:    crossVersionJSONV1,
			v2Schema:    crossVersionJSONV2,
			goV1Record: func() interface{} {
				return &gsrjson.JsonDataWithSchema{Schema: crossVersionJSONV1, Payload: jsonPayload}
			},
			javaV1Record: func() map[string]any {
				return map[string]any{
					"schema":  crossVersionJSONV1,
					"payload": jsonPayload,
				}
			},
			goCheck:           jsonGoCheck,
			javaEnvelopeCheck: jsonJavaCheck,
		},
		{
			name:        "JSON_ZLIB",
			format:      "JSON",
			compression: "ZLIB",
			v1Schema:    crossVersionJSONV1,
			v2Schema:    crossVersionJSONV2,
			goV1Record: func() interface{} {
				return &gsrjson.JsonDataWithSchema{Schema: crossVersionJSONV1, Payload: jsonPayload}
			},
			javaV1Record: func() map[string]any {
				return map[string]any{
					"schema":  crossVersionJSONV1,
					"payload": jsonPayload,
				}
			},
			goCheck:           jsonGoCheck,
			javaEnvelopeCheck: jsonJavaCheck,
		},
		{
			name:              "PROTOBUF_NONE",
			format:            "PROTOBUF",
			compression:       "NONE",
			v1Schema:          crossVersionProtoV1,
			v2Schema:          crossVersionProtoV2,
			goV1Record:        protoGoRecord,
			javaV1Record:      protoJavaRecord,
			goCheck:           protoGoCheck,
			javaEnvelopeCheck: protoJavaCheck,
		},
		{
			name:              "PROTOBUF_ZLIB",
			format:            "PROTOBUF",
			compression:       "ZLIB",
			v1Schema:          crossVersionProtoV1,
			v2Schema:          crossVersionProtoV2,
			goV1Record:        protoGoRecord,
			javaV1Record:      protoJavaRecord,
			goCheck:           protoGoCheck,
			javaEnvelopeCheck: protoJavaCheck,
		},
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// registerV2ViaJava triggers v2 registration under an already-existing schema
// name by /kafka-produce-ing the v2 definition to a throwaway topic.  The Java
// GSR library's AlreadyExistsException → RegisterSchemaVersion fallthrough
// makes the second produce register v2 without creating a duplicate schema.
// Only the error is returned; the response body is discarded.
func registerV2ViaJava(ctx context.Context, sc *javasidecar.Sidecar, req javasidecar.KafkaProduceRequest) error {
	_, err := sc.KafkaProduce(ctx, req)
	return err
}

// buildDynamicProtoMessage parses a .proto definition string and returns a
// *dynamicpb.Message whose first declared message type is populated from
// fields.  Pass a nil fields map to get a zero-valued message (useful as a
// target for proto.Unmarshal).
//
// Uses jhump/protoreflect/desc/protoparse — the same library as the core GSR
// serializer — to produce a protoreflect.MessageDescriptor from text proto.
func buildDynamicProtoMessage(schemaDef string, fields map[string]interface{}) (*dynamicpb.Message, error) {
	parser := protoparse.Parser{
		InferImportPaths: true,
	}
	parser.Accessor = protoparse.FileContentsFromMap(map[string]string{
		"schema.proto": schemaDef,
	})

	fileDescs, err := parser.ParseFiles("schema.proto")
	if err != nil || len(fileDescs) == 0 {
		return nil, fmt.Errorf("buildDynamicProtoMessage: parse proto definition: %w", err)
	}
	msgTypes := fileDescs[0].GetMessageTypes()
	if len(msgTypes) == 0 {
		return nil, fmt.Errorf("buildDynamicProtoMessage: no message types in schema definition")
	}

	// UnwrapMessage converts the jhump *desc.MessageDescriptor to a
	// protoreflect.MessageDescriptor, which dynamicpb.NewMessage requires.
	md := msgTypes[0].UnwrapMessage()
	msg := dynamicpb.NewMessage(md)

	for name, val := range fields {
		fd := md.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return nil, fmt.Errorf("buildDynamicProtoMessage: field %q not found", name)
		}
		var rv protoreflect.Value
		switch v := val.(type) {
		case string:
			rv = protoreflect.ValueOfString(v)
		case int32:
			rv = protoreflect.ValueOfInt32(v)
		case int64:
			rv = protoreflect.ValueOfInt64(v)
		case float32:
			rv = protoreflect.ValueOfFloat32(v)
		case float64:
			rv = protoreflect.ValueOfFloat64(v)
		case bool:
			rv = protoreflect.ValueOfBool(v)
		case []byte:
			rv = protoreflect.ValueOfBytes(v)
		default:
			return nil, fmt.Errorf("buildDynamicProtoMessage: unsupported field type %T for %q", val, name)
		}
		msg.Set(fd, rv)
	}
	return msg, nil
}

// xverAssertJSONFields parses a JSON payload and asserts the 3 base fields
// match xverID, xverName, xverAge.
func xverAssertJSONFields(t *testing.T, payload string) {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(payload), &m))
	require.Equal(t, xverID, m["id"])
	require.Equal(t, xverName, m["name"])
	age, ok := m["age"].(float64)
	require.True(t, ok, "JSON age: expected float64, got %T (%v)", m["age"], m["age"])
	require.Equal(t, xverAge, int(age))
}

// ---------------------------------------------------------------------------
// Test skeletons — PBI-3 (Cell A) and PBI-4 (Cell B) fill in the bodies.
// ---------------------------------------------------------------------------

// buildGoConfigCellA builds the Go-side Configuration for Cell A (Java→Go
// direction). For PROTOBUF, the descriptor is derived at runtime from the
// crossVersionProtoV1 schema text via buildDynamicProtoMessage so the test
// does not depend on the compiled testpb package for this message type
// (AC-11). For AVRO and JSON the configuration mirrors buildGoConfig.
func buildGoConfigCellA(t *testing.T, region, schemaName, format, compression, v1Schema string) *common.Configuration {
	t.Helper()
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   compression,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey: gsrMap,
	}
	switch format {
	case "AVRO":
		configMap[common.DataFormatTypeKey] = common.DataFormatAvro
		configMap[common.AvroRecordTypeKey] = common.AvroRecordTypeGeneric
	case "JSON":
		configMap[common.DataFormatTypeKey] = common.DataFormatJSON
	case "PROTOBUF":
		// AC-11: use dynamicpb built from the v1 schema text, NOT the
		// compiled testpb descriptor, to prove wire-format agnosticism.
		zeroMsg, err := buildDynamicProtoMessage(v1Schema, nil)
		require.NoError(t, err, "buildDynamicProtoMessage for PROTOBUF config (Cell A)")
		configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
		configMap[common.ProtobufMessageDescriptorKey] = zeroMsg.ProtoReflect().Descriptor()
	default:
		t.Fatalf("buildGoConfigCellA: unsupported format %q", format)
	}
	return common.NewConfiguration(configMap)
}

// TestInterop_CrossVersion_JavaProduce_GoConsume — Cell A direction.
// Java sidecar produces at v1; Go consumer deserializes despite v2 existing.
func TestInterop_CrossVersion_JavaProduce_GoConsume(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	// Belt-and-suspenders prefix sweep: picks up any gsr-go-it-xver-* schema
	// this run created, even if an explicit TrackSchema call missed it.
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-xver-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range crossVersionMatrix() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-xver-" + tc.format + "-" + suffix
			topic := "gsr-go-it-xver-" + suffix
			throwawayTopic := "gsr-go-it-xver-reg-" + suffix
			cleanup.TrackSchema("default-registry", schemaName)

			// Step 1 — Java sidecar registers v1 AND produces a v1 record to
			// the main topic under BACKWARD compatibility.  The Java library's
			// CreateSchema path stores v1 in Glue with the requested compat.
			_, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
				Format:        tc.format,
				Schema:        tc.v1Schema,
				SchemaName:    schemaName,
				Record:        tc.javaV1Record(),
				Compression:   tc.compression,
				Bootstrap:     broker.Bootstrap,
				Topic:         topic,
				Region:        real.Region,
				Compatibility: "BACKWARD",
			})
			require.NoError(t, err, "Cell A: java produce v1 to main topic (%s)", tc.name)

			// Step 2 — Register v2 under the same schema name via a throwaway
			// topic.  The Java library's AlreadyExistsException →
			// RegisterSchemaVersion fallthrough writes v2 as a new version of
			// the existing schema rather than creating a duplicate.
			err = registerV2ViaJava(ctx, sc, javasidecar.KafkaProduceRequest{
				Format:        tc.format,
				Schema:        tc.v2Schema,
				SchemaName:    schemaName,
				Record:        tc.javaV1Record(), // payload irrelevant; just forces the schema path
				Compression:   tc.compression,
				Bootstrap:     broker.Bootstrap,
				Topic:         throwawayTopic,
				Region:        real.Region,
				Compatibility: "BACKWARD",
			})
			require.NoError(t, err, "Cell A: register v2 via throwaway topic (%s)", tc.name)

			// Step 3 — Go side consumes the v1-framed bytes from the main topic.
			framed := consumeOne(t, ctx, broker.Bootstrap, topic)

			// Step 4 — Deserialize.  For PROTOBUF, buildGoConfigCellA supplies a
			// dynamicpb descriptor derived from the v1 schema text (AC-11).
			cfg := buildGoConfigCellA(t, real.Region, schemaName, tc.format, tc.compression, tc.v1Schema)
			des, err := deserializer.NewDeserializer(cfg)
			require.NoError(t, err, "Cell A: go NewDeserializer (%s)", tc.name)
			t.Cleanup(func() { _ = des.Close() })

			// Step 5 — Decode.  The Go deserializer fetches the writer schema
			// (v1) from the GSR header version-id; v2 being present in the
			// registry must not disrupt decoding.
			got, err := des.Deserialize(topic, framed)
			require.NoError(t, err, "Cell A: go Deserialize (%s)", tc.name)

			// Step 6 — Assert v1 payload fields (AC-4).
			tc.goCheck(t, got)
		})
	}
}

// TestInterop_CrossVersion_GoProduce_JavaConsume — Cell B direction.
// Go serializer produces at v1; Java sidecar deserializes despite v2 existing.
func TestInterop_CrossVersion_GoProduce_JavaConsume(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-xver-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range crossVersionMatrix() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-xver-" + tc.format + "-" + suffix
			topic := "gsr-go-it-xver-" + suffix
			throwawayTopic := "gsr-go-it-xver-reg-" + suffix
			cleanup.TrackSchema("default-registry", schemaName)

			// PBI-4 fills in the Cell B logic here.
			_ = sc
			_ = broker
			_ = schemaName
			_ = topic
			_ = throwawayTopic
			_ = real

			t.Skip("Cell B not yet implemented")
		})
	}
}
