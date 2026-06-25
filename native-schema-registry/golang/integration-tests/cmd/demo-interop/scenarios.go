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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
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
// ---------------------------------------------------------------------------
// random 8-hex-char suffix generator
// ---------------------------------------------------------------------------

// randomSuffix returns a random 8-char hex string used for topic/schema names.
func randomSuffix() (string, error) {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("randomSuffix: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// ---------------------------------------------------------------------------
// buildGoConfigCellA — mirrors buildGoConfigCellA from
//   integration-tests/tests/interop_crossversion_kafka_test.go (lines 458-491)
// The testing.T dependency has been removed; errors are returned directly.
// ---------------------------------------------------------------------------

// buildDemoConfigCellA builds the Go-side Configuration for Direction A
// (Java produces v1, Go consumes). For PROTOBUF, the descriptor is derived
// at runtime from the v1 schema text via buildDynamicProtoMessage so the demo
// does not depend on compiled testpb types.
func buildDemoConfigCellA(region, format, v1Schema string) (*common.Configuration, error) {
	gsrMap := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   "NONE",
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
		zeroMsg, err := buildDynamicProtoMessage(v1Schema, nil)
		if err != nil {
			return nil, fmt.Errorf("buildDemoConfigCellA: build proto descriptor: %w", err)
		}
		configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
		configMap[common.ProtobufMessageDescriptorKey] = zeroMsg.ProtoReflect().Descriptor()
	default:
		return nil, fmt.Errorf("buildDemoConfigCellA: unsupported format %q", format)
	}
	return common.NewConfiguration(configMap), nil
}

// ---------------------------------------------------------------------------
// consumeOneRaw — reads one raw message from Kafka using sarama.
// Mirrors consumeOne from interop_kafka_roundtrip_test.go but without
// testing.T — errors are returned directly.
// ---------------------------------------------------------------------------

func consumeOneRaw(ctx context.Context, bootstrap, topic string) ([]byte, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer([]string{bootstrap}, cfg)
	if err != nil {
		return nil, fmt.Errorf("consumeOneRaw: sarama.NewConsumer: %w", err)
	}
	defer consumer.Close() //nolint:errcheck

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("consumeOneRaw: ConsumePartition(%s): %w", topic, err)
	}
	defer pc.Close() //nolint:errcheck

	// Drain errors in background so they don't block Messages().
	go func() {
		for range pc.Errors() {
		}
	}()

	select {
	case msg := <-pc.Messages():
		return msg.Value, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("consumeOneRaw: timed out waiting for message on %s: %w", topic, ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// formatConfigForPrint builds the human-readable key list for printGoConfig.
// ---------------------------------------------------------------------------

func configMapForFormat(region, format string) map[string]string {
	m := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   "NONE",
		"schemaAutoRegistrationEnabled": "true",
	}
	switch format {
	case "AVRO":
		m["dataFormat"] = "AVRO"
		m["avroRecordType"] = "GENERIC_RECORD"
	case "JSON":
		m["dataFormat"] = "JSON"
	case "PROTOBUF":
		m["dataFormat"] = "PROTOBUF"
		m["protobufMessageDescriptor"] = "<dynamicpb from v1 schema text>"
	}
	return m
}

// ---------------------------------------------------------------------------
// mustJSONString marshals v to a compact JSON string, panicking on error.
// Used for building Java sidecar record envelopes (internal helper only).
// ---------------------------------------------------------------------------

func mustJSONString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("mustJSONString: %v", err))
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// javaRecordForFormat builds the per-format Record envelope for
// sc.KafkaProduce.  Mirrors the javaV1Record closures in crossVersionMatrix.
// ---------------------------------------------------------------------------

func javaRecordForFormat(format string) map[string]any {
	switch format {
	case "AVRO":
		return map[string]any{
			"fields": map[string]any{
				"id":   demoID,
				"name": demoName,
				"age":  demoAge,
			},
		}
	case "JSON":
		payload := mustJSONString(map[string]any{
			"id":   demoID,
			"name": demoName,
			"age":  demoAge,
		})
		return map[string]any{
			"schema":  "", // will be overridden by v1Schema at call site
			"payload": payload,
		}
	case "PROTOBUF":
		return map[string]any{
			"messageTypeFullName": "test.CrossVersionMessage",
			"fieldsJson": mustJSONString(map[string]any{
				"id":   demoID,
				"name": demoName,
				"age":  demoAge,
			}),
		}
	default:
		panic(fmt.Sprintf("javaRecordForFormat: unsupported format %q", format))
	}
}

// ---------------------------------------------------------------------------
// verifyDecodedResult checks that the deserialized Go value matches the
// expected demo values, returning true if all fields match.
// ---------------------------------------------------------------------------

func verifyDecodedResult(format string, got interface{}) (bool, error) {
	switch format {
	case "AVRO":
		m, ok := got.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("AVRO: expected map[string]interface{}, got %T", got)
		}
		idVal, _ := m["id"].(string)
		nameVal, _ := m["name"].(string)
		// hamba/avro/v2 decodes Avro int32 into Go int when target is interface{}.
		ageRaw := m["age"]
		ageVal, ageOK := ageRaw.(int)
		if !ageOK {
			return false, fmt.Errorf("AVRO: age expected int, got %T (%v)", ageRaw, ageRaw)
		}
		pass := idVal == demoID && nameVal == demoName && ageVal == int(demoAge)
		return pass, nil

	case "JSON":
		s, ok := got.(string)
		if !ok {
			return false, fmt.Errorf("JSON: expected string payload, got %T", got)
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(s), &parsed); err != nil {
			return false, fmt.Errorf("JSON: parse payload: %w", err)
		}
		idVal, _ := parsed["id"].(string)
		nameVal, _ := parsed["name"].(string)
		ageRaw := parsed["age"]
		ageFloat, ageOK := ageRaw.(float64)
		if !ageOK {
			return false, fmt.Errorf("JSON: age expected float64, got %T (%v)", ageRaw, ageRaw)
		}
		pass := idVal == demoID && nameVal == demoName && int(ageFloat) == int(demoAge)
		return pass, nil

	case "PROTOBUF":
		m, ok := got.(proto.Message)
		if !ok {
			return false, fmt.Errorf("PROTOBUF: expected proto.Message, got %T", got)
		}
		raw, err := proto.Marshal(m)
		if err != nil {
			return false, fmt.Errorf("PROTOBUF: marshal: %w", err)
		}
		ref, err := buildDynamicProtoMessage(crossVersionProtoV1, nil)
		if err != nil {
			return false, fmt.Errorf("PROTOBUF: build ref: %w", err)
		}
		if err := proto.Unmarshal(raw, ref); err != nil {
			return false, fmt.Errorf("PROTOBUF: unmarshal: %w", err)
		}
		refl := ref.ProtoReflect()
		md := refl.Descriptor()
		idVal := refl.Get(md.Fields().ByName("id")).String()
		nameVal := refl.Get(md.Fields().ByName("name")).String()
		ageVal := int32(refl.Get(md.Fields().ByName("age")).Int())
		pass := idVal == demoID && nameVal == demoName && ageVal == demoAge
		return pass, nil

	default:
		return false, fmt.Errorf("verifyDecodedResult: unsupported format %q", format)
	}
}

// ---------------------------------------------------------------------------
// narrateVerify prints [VERIFY] output for a given format / result.
// ---------------------------------------------------------------------------

func narrateVerify(format string, got interface{}, pass bool, err error) {
	printStage("VERIFY", "Equality check:")
	if err != nil {
		fmt.Printf("  Error decoding result: %v\n", err)
		printStage("VERIFY", "FAIL — decode error.")
		return
	}
	switch format {
	case "AVRO":
		if m, ok := got.(map[string]interface{}); ok {
			printEqualityCheck(demoID, m["id"], "id")
			printEqualityCheck(demoName, m["name"], "name")
			printEqualityCheck(int(demoAge), m["age"], "age")
		}
	case "JSON":
		if s, ok := got.(string); ok {
			var parsed map[string]any
			if jerr := json.Unmarshal([]byte(s), &parsed); jerr == nil {
				printEqualityCheck(demoID, parsed["id"], "id")
				printEqualityCheck(demoName, parsed["name"], "name")
				if ageFloat, ok := parsed["age"].(float64); ok {
					printEqualityCheck(int(demoAge), int(ageFloat), "age")
				}
			}
		}
	case "PROTOBUF":
		if m, ok := got.(proto.Message); ok {
			raw, rerr := proto.Marshal(m)
			if rerr == nil {
				ref, rerr2 := buildDynamicProtoMessage(crossVersionProtoV1, nil)
				if rerr2 == nil && proto.Unmarshal(raw, ref) == nil {
					refl := ref.ProtoReflect()
					md := refl.Descriptor()
					printEqualityCheck(demoID, refl.Get(md.Fields().ByName("id")).String(), "id")
					printEqualityCheck(demoName, refl.Get(md.Fields().ByName("name")).String(), "name")
					printEqualityCheck(demoAge, int32(refl.Get(md.Fields().ByName("age")).Int()), "age")
				}
			}
		}
	}
	if pass {
		fmt.Printf("  PASS — Java v1 → Go decode (v2 registered) succeeded.\n")
	} else {
		fmt.Printf("  FAIL — one or more field mismatches.\n")
	}
	fmt.Println()
}

// ---------------------------------------------------------------------------
// runDirectionAFormat runs Direction A for a single format.
// ---------------------------------------------------------------------------

// runDirectionAFormat executes Direction A (Java produces v1, Go consumes) for
// one format. It:
//  1. Registers v1 schema via Java sidecar (BACKWARD compat), narrated [SCHEMA-V1].
//  2. Registers v2 schema via a throwaway topic, narrated [SCHEMA-V2].
//  3. Java sidecar produces a v1 record to the main Direction A topic, [JAVA-PRODUCE].
//  4. Go deserializer consumes + decodes the framed bytes, [GO-CONSUME].
//  5. Verifies decoded fields against demo constants, [VERIFY].
//
// Returns a ScenarioResult for the format.
func runDirectionAFormat(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cell ScenarioCell,
) ScenarioResult {
	result := ScenarioResult{
		Format:    cell.Format,
		Direction: "A",
		Topic:     cell.Topic,
	}

	// ── Section header ───────────────────────────────────────────────────────
	printSectionHeader(fmt.Sprintf("FORMAT: %s | Direction A: Java produces at v1, Go consumes", cell.Format))

	// ── Step 1: Register v1 via Java sidecar, main topic ────────────────────

	printStage("SCHEMA-V1", fmt.Sprintf("Registering schema v1 via Java sidecar..."))
	fmt.Printf("  Schema name:   %s\n", cell.SchemaName)
	fmt.Printf("  Registry:      %s\n", cell.RegistryName)
	fmt.Printf("  Compatibility: BACKWARD\n")
	fmt.Printf("  Schema body:\n")
	for _, line := range strings.Split(cell.V1Schema, "\n") {
		fmt.Printf("    %s\n", line)
	}
	fmt.Println()

	record := javaRecordForFormat(cell.Format)
	// For JSON, inject the v1 schema into the record envelope.
	if cell.Format == "JSON" {
		record["schema"] = cell.V1Schema
	}

	v1Resp, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
		Format:        cell.Format,
		Schema:        cell.V1Schema,
		SchemaName:    cell.SchemaName,
		Record:        record,
		Compression:   "NONE",
		Bootstrap:     broker.Bootstrap,
		Topic:         cell.Topic,
		Region:        cell.Region,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		result.Err = fmt.Errorf("SCHEMA-V1 / java produce: %w", err)
		printStage("SCHEMA-V1", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("SCHEMA-V1", fmt.Sprintf("Glue schema-version-id: %s", v1Resp.SchemaVersionID))
	fmt.Println()
	result.SchemaVersionID = v1Resp.SchemaVersionID

	// ── Step 2: Register v2 via throwaway topic ──────────────────────────────

	printStage("SCHEMA-V2", "Registering schema v2 (evolution: added optional \"email\" field)...")
	fmt.Printf("  Schema name: %s (same schema, new version)\n", cell.SchemaName)
	fmt.Printf("  Schema body:\n")
	for _, line := range strings.Split(cell.V2Schema, "\n") {
		fmt.Printf("    %s\n", line)
	}
	fmt.Println()

	throwawayRecord := javaRecordForFormat(cell.Format)
	if cell.Format == "JSON" {
		throwawayRecord["schema"] = cell.V1Schema // payload irrelevant; forces schema path
	}

	v2Resp, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
		Format:        cell.Format,
		Schema:        cell.V2Schema,
		SchemaName:    cell.SchemaName,
		Record:        throwawayRecord,
		Compression:   "NONE",
		Bootstrap:     broker.Bootstrap,
		Topic:         cell.ThrowawayTopic,
		Region:        cell.Region,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		result.Err = fmt.Errorf("SCHEMA-V2 / register v2: %w", err)
		printStage("SCHEMA-V2", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("SCHEMA-V2", fmt.Sprintf("Glue schema-version-id: %s", v2Resp.SchemaVersionID))
	fmt.Println()

	// ── Step 3: Java sidecar produces v1 record (already done in step 1) ────
	// The v1 record was already produced to cell.Topic in Step 1 above.
	// Narrate that produce step now.

	printStage("JAVA-PRODUCE", "Java sidecar producing v1 record to Kafka...")
	fmt.Printf("  Topic:       %s\n", cell.Topic)
	switch cell.Format {
	case "AVRO":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "JSON":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "PROTOBUF":
		fmt.Printf("  Record:      CrossVersionMessage{id: %q, name: %q, age: %d}\n", demoID, demoName, demoAge)
	}
	fmt.Printf("  Compression: NONE\n")
	fmt.Println()

	printStage("JAVA-PRODUCE", "Produced. Framed bytes (hex):")
	printHexDump("Wire bytes", v1Resp.Bytes)

	// ── Step 4: Go deserializer consumes raw bytes from Kafka ────────────────

	printStage("GO-CONSUME", fmt.Sprintf("Go deserializer consuming from Kafka topic..."))
	fmt.Printf("  Topic: %s\n", cell.Topic)
	fmt.Println()

	goConfig := configMapForFormat(cell.Region, cell.Format)
	printGoConfig(goConfig)

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	framed, err := consumeOneRaw(consumeCtx, broker.Bootstrap, cell.Topic)
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("GO-CONSUME / read kafka: %w", err)
		printStage("GO-CONSUME", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// ── Step 5: Deserialize via Go GSR client ────────────────────────────────

	cfg, err := buildDemoConfigCellA(cell.Region, cell.Format, cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("GO-CONSUME / build config: %w", err)
		printStage("GO-CONSUME", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	des, err := deserializer.NewDeserializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("GO-CONSUME / NewDeserializer: %w", err)
		printStage("GO-CONSUME", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer des.Close() //nolint:errcheck

	got, err := des.Deserialize(cell.Topic, framed)
	if err != nil {
		result.Err = fmt.Errorf("GO-CONSUME / Deserialize: %w", err)
		printStage("GO-CONSUME", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("GO-CONSUME", "Deserialized successfully.")
	fmt.Printf("  Schema version retrieved: %s (v1)\n", v1Resp.SchemaVersionID)
	fmt.Printf("  Decoded Go value:\n")
	switch cell.Format {
	case "AVRO":
		if m, ok := got.(map[string]interface{}); ok {
			fmt.Printf("    map[string]interface{}{\n")
			fmt.Printf("      \"id\":   %q  (string)\n", m["id"])
			fmt.Printf("      \"name\": %q  (string)\n", m["name"])
			fmt.Printf("      \"age\":  %v   (int)\n", m["age"])
			fmt.Printf("    }\n")
		}
	case "JSON":
		if s, ok := got.(string); ok {
			fmt.Printf("    (string) %s\n", s)
		}
	case "PROTOBUF":
		if m, ok := got.(proto.Message); ok {
			raw, _ := proto.Marshal(m)
			ref, _ := buildDynamicProtoMessage(crossVersionProtoV1, nil)
			if proto.Unmarshal(raw, ref) == nil {
				refl := ref.ProtoReflect()
				md := refl.Descriptor()
				fmt.Printf("    CrossVersionMessage{\n")
				fmt.Printf("      id:   %q\n", refl.Get(md.Fields().ByName("id")).String())
				fmt.Printf("      name: %q\n", refl.Get(md.Fields().ByName("name")).String())
				fmt.Printf("      age:  %d\n", refl.Get(md.Fields().ByName("age")).Int())
				fmt.Printf("    }\n")
			}
		}
	}
	fmt.Println()

	// ── Step 6: Verify ───────────────────────────────────────────────────────

	pass, verifyErr := verifyDecodedResult(cell.Format, got)
	narrateVerify(cell.Format, got, pass, verifyErr)

	result.Pass = pass
	if verifyErr != nil {
		result.Err = verifyErr
	}
	result.SchemaName = cell.SchemaName
	return result
}

// ---------------------------------------------------------------------------
// runDirectionA — runs Direction A for all 3 formats.
// ---------------------------------------------------------------------------

// runDirectionA orchestrates Direction A (Java produces v1, Go consumes) for
// Avro, JSON Schema, and Protobuf. It generates a shared random 8-hex suffix
// for schema/topic naming, constructs ScenarioCell values for each format,
// runs each scenario in sequence, and returns the three ScenarioResults.
//
// Per spec §6.3: schema name = "demo-4.15-<format>-<suffix>" (no direction),
// so Direction B (PBI-03) can reuse the same registered schema.
//
// The cleanup argument is used to register each schema for deferred deletion.
func runDirectionA(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) []ScenarioResult {
	suffix, err := randomSuffix()
	if err != nil {
		printStage("ERROR", fmt.Sprintf("runDirectionA: generate suffix: %v", err))
		return []ScenarioResult{}
	}

	formats := []struct {
		key      string // "AVRO", "JSON", "PROTOBUF"
		fmtLabel string // "avro", "json", "proto"
		v1Schema string
		v2Schema string
	}{
		{"AVRO", "avro", crossVersionAvroV1, crossVersionAvroV2},
		{"JSON", "json", crossVersionJSONV1, crossVersionJSONV2},
		{"PROTOBUF", "proto", crossVersionProtoV1, crossVersionProtoV2},
	}

	var results []ScenarioResult
	for _, f := range formats {
		schemaName := fmt.Sprintf("%s%s-%s", demoPrefix, f.fmtLabel, suffix)
		topic := fmt.Sprintf("%s%s-a-%s", demoPrefix, f.fmtLabel, suffix)
		throwawayTopic := fmt.Sprintf("%s%s-reg-%s", demoPrefix, f.fmtLabel, suffix)

		cleanup.TrackSchema(demoRegistryName, schemaName)

		cell := ScenarioCell{
			Format:         f.key,
			Direction:      "A",
			SchemaName:     schemaName,
			Topic:          topic,
			ThrowawayTopic: throwawayTopic,
			V1Schema:       f.v1Schema,
			V2Schema:       f.v2Schema,
			RecordFields: map[string]interface{}{
				"id":   demoID,
				"name": demoName,
				"age":  demoAge,
			},
			Region:       region,
			RegistryName: demoRegistryName,
		}

		res := runDirectionAFormat(ctx, sc, broker, cell)
		results = append(results, res)
	}
	return results
}

// ---------------------------------------------------------------------------
// runAllScenarios — wires Direction A (PBI-02); Direction B added by PBI-03.
// ---------------------------------------------------------------------------

// runAllScenarios orchestrates the full 6-pair demo (3 formats × 2 directions).
// PBI-02 implements Direction A (Java→Go). PBI-03 will add Direction B (Go→Java).
// PBI-04 will add the summary table and exit-code logic.
func runAllScenarios(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) []ScenarioResult {
	return runDirectionA(ctx, sc, broker, cleanup, region)
}
