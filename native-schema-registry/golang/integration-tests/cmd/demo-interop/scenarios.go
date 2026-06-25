//go:build integration

// Package main — scenarios.go
// Schema constants, helper types, and the scenario runner placeholder.
//
// NOTE: The schema string constants below (crossVersionAvroV1 through
// crossVersionProtoV2) and the buildDynamicProtoMessage function are
// re-declared/copied here from
//   integration-tests/tests/interop_crossversion_kafka_test.go (lines 73-139
//   and 387-439 respectively)
// because Go does not permit importing symbols from _test.go files into a
// main package. The values are byte-for-byte identical to those in the source
// file. The source file remains the canonical definition; these copies must be
// kept in sync if the schema fixtures are ever changed.
package main

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
)

// ---------------------------------------------------------------------------
// Demo record constants (§5.4 — distinct from Phase 4.13 test values).
// ---------------------------------------------------------------------------

const demoID = "demo-id"
const demoName = "demo-name"
const demoAge = int32(42)

// ---------------------------------------------------------------------------
// Schema constants — copied byte-for-byte from
//   integration-tests/tests/interop_crossversion_kafka_test.go lines 73-139.
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
// ScenarioResult is the shared result type consumed by PBI-04's summary table.
// All 6 required fields (PBI-01 contract) are present.
// ---------------------------------------------------------------------------

// ScenarioResult carries the outcome of one scenario (one format × one
// direction). PBI-02 (Direction A) and PBI-03 (Direction B) populate slices
// of ScenarioResult; PBI-04 aggregates them into the summary table.
type ScenarioResult struct {
	Format          string // e.g. "AVRO", "JSON", "PROTOBUF"
	Direction       string // "A" (Java→Go) or "B" (Go→Java)
	Pass            bool
	Err             error
	Topic           string
	SchemaVersionID string
	// SchemaName is the Glue schema name used for this scenario.
	SchemaName string
}

// ---------------------------------------------------------------------------
// ScenarioCell carries per-format, per-direction context needed by the
// scenario runner (PBI-02/03). It is constructed once per format at demo
// start so both directions share the same schema name and random suffix.
// ---------------------------------------------------------------------------

// ScenarioCell is one (format, direction) cell in the 6-entry demo matrix.
type ScenarioCell struct {
	// Format is one of "AVRO", "JSON", "PROTOBUF".
	Format string
	// Direction is "A" (Java→Go) or "B" (Go→Java).
	Direction string
	// SchemaName is the Glue schema name, e.g. "demo-4.15-avro-a1b2c3d4".
	// Both Direction A and Direction B share the same SchemaName for a given
	// format so Direction B can reuse the versions registered by Direction A.
	SchemaName string
	// Topic is the Kafka topic for this cell,
	// e.g. "demo-4.15-avro-a-a1b2c3d4" or "demo-4.15-avro-b-a1b2c3d4".
	Topic string
	// ThrowawayTopic is used for the v2 registration step (Direction A) or
	// the pre-registration step (Direction B).
	ThrowawayTopic string
	// V1Schema is the schema definition string for version 1.
	V1Schema string
	// V2Schema is the schema definition string for version 2 (evolved).
	V2Schema string
	// RecordFields maps field names to their demo values (demoID, demoName, demoAge).
	RecordFields map[string]interface{}
	// Region is the AWS region used for this cell.
	Region string
	// RegistryName is the Glue registry name.
	RegistryName string
}

// ---------------------------------------------------------------------------
// buildDynamicProtoMessage — copied from
//   integration-tests/tests/interop_crossversion_kafka_test.go lines 387-439.
// The testing.T parameter has been removed; callers receive errors directly.
// ---------------------------------------------------------------------------

// buildDynamicProtoMessage parses schemaDef as a proto3 definition and
// constructs a *dynamicpb.Message with the given field values. It uses
// jhump/protoreflect/desc/protoparse — the same library as the core GSR
// serializer — to produce a protoreflect.MessageDescriptor from text proto.
func buildDynamicProtoMessage(schemaDef string, fields map[string]interface{}) (*dynamicpb.Message, error) {
	parser := protoparse.Parser{
		InferImportPaths: true,
	}
	parser.Accessor = protoparse.FileContentsFromMap(map[string]string{
		"schema.proto": schemaDef,
	})

	fileDescs, err := parser.ParseFiles("schema.proto")
	if err != nil {
		return nil, fmt.Errorf("buildDynamicProtoMessage: parse proto definition: %w", err)
	}
	if len(fileDescs) == 0 {
		return nil, fmt.Errorf("buildDynamicProtoMessage: parse proto definition: empty descriptor list")
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

// ---------------------------------------------------------------------------
// runAllScenarios — placeholder; populated by PBI-02 and PBI-03.
// ---------------------------------------------------------------------------

// runAllScenarios orchestrates the full 6-pair demo (3 formats × 2 directions).
// This is a placeholder that returns an empty slice. PBI-02 will implement
// Direction A (Java→Go) cells and PBI-03 will implement Direction B (Go→Java)
// cells by replacing this function with a real implementation.
func runAllScenarios(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
) []ScenarioResult {
	return []ScenarioResult{}
}
