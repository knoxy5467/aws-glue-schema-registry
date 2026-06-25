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

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/realglue"
	gsravro "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer/json"
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
option go_package = "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
}
`

const crossVersionProtoV2 = `syntax = "proto3";
package test;
option go_package = "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb";
message CrossVersionMessage {
  string id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
`

// ---------------------------------------------------------------------------
// ScenarioResult is the shared result type consumed by PBI-04's summary table.
// All required fields (PBI-01 contract) are present.
// ---------------------------------------------------------------------------

// ScenarioResult carries the outcome of one scenario (one format × one
// direction × one compression). PBI-02 (Direction A) and PBI-03 (Direction B)
// populate slices of ScenarioResult; PBI-04 aggregates them into the summary
// table.
type ScenarioResult struct {
	Format          string // e.g. "AVRO", "JSON", "PROTOBUF"
	Direction       string // "A" (Java→Go) or "B" (Go→Java)
	Compression     string // "NONE" or "ZLIB"
	Pass            bool
	Err             error
	Topic           string
	SchemaVersionID string
	// SchemaName is the Glue schema name used for this scenario.
	SchemaName string
}

// ---------------------------------------------------------------------------
// ScenarioCell carries per-format, per-direction, per-compression context
// needed by the scenario runner (PBI-02/03). It is constructed once per
// (format, compression) at demo start so both directions share the same
// schema name and random suffix.
// ---------------------------------------------------------------------------

// ScenarioCell is one (format, direction, compression) cell in the 12-entry
// demo matrix.
type ScenarioCell struct {
	// Format is one of "AVRO", "JSON", "PROTOBUF".
	Format string
	// Direction is "A" (Java→Go) or "B" (Go→Java).
	Direction string
	// Compression is "NONE" or "ZLIB".
	Compression string
	// SchemaName is the Glue schema name, e.g. "demo-4.15-avro-a1b2c3d4".
	// Both Direction A and Direction B share the same SchemaName for a given
	// format so Direction B can reuse the versions registered by Direction A.
	SchemaName string
	// Topic is the Kafka topic for this cell,
	// e.g. "demo-4.15-avro-a-none-a1b2c3d4" or "demo-4.15-avro-b-zlib-a1b2c3d4".
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
func buildDemoConfigCellA(region, format, compression, v1Schema string) (*common.Configuration, error) {
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

func configMapForFormat(region, format, compression string) map[string]string {
	m := map[string]string{
		"region":                        region,
		"registry.name":                 "default-registry",
		"compression":                   compression,
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
// narrateVerify prints [verdict] output for a given format / result.
// ---------------------------------------------------------------------------

func narrateVerify(format string, got interface{}, pass bool, err error) {
	printStage("verdict", "Equality check:")
	if err != nil {
		fmt.Printf("  Error decoding result: %v\n", err)
		printStage("verdict", "FAIL — decode error.")
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
// runDirectionAFormat runs Direction A for a single format × compression cell.
// ---------------------------------------------------------------------------

// runDirectionAFormat executes Direction A (Java produces v1, Go consumes) for
// one format × compression. It:
//  1. Registers v1 schema via Java sidecar (BACKWARD compat), narrated [glue] + [schema-evolution].
//  2. Registers v2 schema via a throwaway topic, narrated [glue] + [schema-evolution].
//  3. Java sidecar produces a v1 record to the main Direction A topic, [java-producer].
//  4. Go deserializer consumes + decodes the framed bytes, [go-consumer].
//  5. Verifies decoded fields against demo constants, [verdict].
//
// Returns a ScenarioResult for the cell.
func runDirectionAFormat(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cell ScenarioCell,
) ScenarioResult {
	result := ScenarioResult{
		Format:      cell.Format,
		Direction:   "A",
		Compression: cell.Compression,
		Topic:       cell.Topic,
	}

	// ── Section header ───────────────────────────────────────────────────────
	printSectionHeader(fmt.Sprintf("FORMAT: %s | Direction A: Java produces at v1, Go consumes | Compression: %s", cell.Format, cell.Compression))

	// ── Step 1: Register v1 via Java sidecar, main topic ────────────────────

	printStage("glue", fmt.Sprintf("CreateSchema schema=%s registry=%s compatibility=BACKWARD", cell.SchemaName, cell.RegistryName))
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
		Compression:   cell.Compression,
		Bootstrap:     broker.Bootstrap,
		Topic:         cell.Topic,
		Region:        cell.Region,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		result.Err = fmt.Errorf("glue / java produce: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("schema-evolution", fmt.Sprintf("v1 registered: %s", v1Resp.SchemaVersionID))
	fmt.Println()
	result.SchemaVersionID = v1Resp.SchemaVersionID

	// ── Step 2: Register v2 via throwaway topic ──────────────────────────────

	printStage("glue", fmt.Sprintf("RegisterSchemaVersion schema=%s (v2 evolution: added optional \"email\" field)", cell.SchemaName))
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
		Compression:   cell.Compression,
		Bootstrap:     broker.Bootstrap,
		Topic:         cell.ThrowawayTopic,
		Region:        cell.Region,
		Compatibility: "BACKWARD",
	})
	if err != nil {
		result.Err = fmt.Errorf("glue / register v2: %w", err)
		printStage("glue", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("schema-evolution", fmt.Sprintf("v2 registered (BACKWARD-evolution): %s", v2Resp.SchemaVersionID))
	fmt.Println()

	// ── Step 3: Java sidecar produces v1 record (already done in step 1) ────
	// The v1 record was already produced to cell.Topic in Step 1 above.
	// Narrate that produce step now.

	printStage("java-producer", "Java sidecar producing v1 record to Kafka...")
	fmt.Printf("  Topic:       %s\n", cell.Topic)
	switch cell.Format {
	case "AVRO":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "JSON":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "PROTOBUF":
		fmt.Printf("  Record:      CrossVersionMessage{id: %q, name: %q, age: %d}\n", demoID, demoName, demoAge)
	}
	fmt.Printf("  Compression: %s\n", cell.Compression)
	fmt.Println()

	printStage("java-producer", "Produced. Framed bytes (hex):")
	printHexDump("Wire bytes", v1Resp.Bytes)

	// ── Step 4: Go deserializer consumes raw bytes from Kafka ────────────────

	printStage("go-consumer", "Go deserializer consuming from Kafka topic...")
	fmt.Printf("  Topic: %s\n", cell.Topic)
	fmt.Println()

	goConfig := configMapForFormat(cell.Region, cell.Format, cell.Compression)
	printGoConfig(goConfig)

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	framed, err := consumeOneRaw(consumeCtx, broker.Bootstrap, cell.Topic)
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("go-consumer / read kafka: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// ── Step 5: Deserialize via Go GSR client ────────────────────────────────

	cfg, err := buildDemoConfigCellA(cell.Region, cell.Format, cell.Compression, cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("go-consumer / build config: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	des, err := deserializer.NewDeserializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("go-consumer / NewDeserializer: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer des.Close() //nolint:errcheck

	got, err := des.Deserialize(cell.Topic, framed)
	if err != nil {
		result.Err = fmt.Errorf("go-consumer / Deserialize: %w", err)
		printStage("go-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("go-consumer", "Deserialized successfully.")
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
// runDirectionAWithSuffix — runs Direction A for all 3 formats × 2
// compressions, returns suffix.
// ---------------------------------------------------------------------------

// runDirectionAWithSuffix orchestrates Direction A (Java produces v1, Go
// consumes) for Avro, JSON Schema, and Protobuf across both NONE and ZLIB
// compression. It generates a shared random 8-hex suffix for schema/topic
// naming, constructs ScenarioCell values for each (format, compression),
// runs each scenario in sequence, and returns the ScenarioResults plus the
// suffix so Direction B can reuse the same schema names.
//
// Per spec §6.3: schema name = "demo-4.15-<format>-<suffix>" (no direction),
// so Direction B (PBI-03) can reuse the same registered schema. Compression
// does not affect schema registration — the same schema versions are shared
// across NONE and ZLIB cells.
//
// The cleanup argument is used to register each schema for deferred deletion.
func runDirectionAWithSuffix(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) ([]ScenarioResult, string) {
	suffix, err := randomSuffix()
	if err != nil {
		printStage("demo", fmt.Sprintf("runDirectionA: generate suffix: %v", err))
		return []ScenarioResult{}, ""
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

	compressions := []string{"NONE", "ZLIB"}

	var results []ScenarioResult
	for _, f := range formats {
		schemaName := fmt.Sprintf("%s%s-%s", demoPrefix, f.fmtLabel, suffix)
		cleanup.TrackSchema(demoRegistryName, schemaName)

		for _, comp := range compressions {
			compLabel := strings.ToLower(comp)
			topic := fmt.Sprintf("%s%s-a-%s-%s", demoPrefix, f.fmtLabel, compLabel, suffix)
			throwawayTopic := fmt.Sprintf("%s%s-reg-%s-%s", demoPrefix, f.fmtLabel, compLabel, suffix)

			cell := ScenarioCell{
				Format:         f.key,
				Direction:      "A",
				Compression:    comp,
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
	}
	return results, suffix
}

// ---------------------------------------------------------------------------
// buildDemoConfigCellB — mirrors buildGoConfigCellB from
//   integration-tests/tests/interop_crossversion_kafka_test.go (lines 606-634)
// The testing.T dependency has been removed; errors are returned directly.
// ---------------------------------------------------------------------------

// buildDemoConfigCellB builds the Go-side Configuration for Direction B
// (Go produces v1, Java consumes). The serializer needs schemaAutoRegistration
// enabled and the correct format configuration.
func buildDemoConfigCellB(region, format, compression, v1Schema string) (*common.Configuration, error) {
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
		zeroMsg, err := buildDynamicProtoMessage(v1Schema, nil)
		if err != nil {
			return nil, fmt.Errorf("buildDemoConfigCellB: build proto descriptor: %w", err)
		}
		configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
		configMap[common.ProtobufMessageDescriptorKey] = zeroMsg.ProtoReflect().Descriptor()
	default:
		return nil, fmt.Errorf("buildDemoConfigCellB: unsupported format %q", format)
	}
	return common.NewConfiguration(configMap), nil
}

// ---------------------------------------------------------------------------
// goRecordForFormat builds the Go-typed record for ser.Serialize per format.
// ---------------------------------------------------------------------------

func goRecordForFormat(format, v1Schema string) (interface{}, error) {
	switch format {
	case "AVRO":
		return &gsravro.AvroRecord{
			Schema: v1Schema,
			Data: map[string]any{
				"id":   demoID,
				"name": demoName,
				"age":  demoAge,
			},
		}, nil
	case "JSON":
		payload := mustJSONString(map[string]any{
			"id":   demoID,
			"name": demoName,
			"age":  demoAge,
		})
		return &gsrjson.JsonDataWithSchema{
			Schema:  v1Schema,
			Payload: payload,
		}, nil
	case "PROTOBUF":
		msg, err := buildDynamicProtoMessage(v1Schema, map[string]interface{}{
			"id":   demoID,
			"name": demoName,
			"age":  demoAge,
		})
		if err != nil {
			return nil, fmt.Errorf("goRecordForFormat(PROTOBUF): %w", err)
		}
		return msg, nil
	default:
		return nil, fmt.Errorf("goRecordForFormat: unsupported format %q", format)
	}
}

// ---------------------------------------------------------------------------
// produceOneToKafka — produces raw framed bytes to Kafka using sarama.
// Mirrors produceOne from interop_kafka_roundtrip_test.go without testing.T.
// ---------------------------------------------------------------------------

func produceOneToKafka(ctx context.Context, bootstrap, topic string, value []byte) error {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{bootstrap}, cfg)
	if err != nil {
		return fmt.Errorf("produceOneToKafka: sarama.NewSyncProducer: %w", err)
	}
	defer producer.Close() //nolint:errcheck

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		_, _, sendErr := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		})
		done <- result{err: sendErr}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return fmt.Errorf("produceOneToKafka: send: %w", r.err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("produceOneToKafka: timed out producing to %s: %w", topic, ctx.Err())
	}
}

// ---------------------------------------------------------------------------
// verifyJavaConsumeResult checks the Java sidecar's consumed Record envelope
// against the expected demo values, returning true if all fields match.
// ---------------------------------------------------------------------------

func verifyJavaConsumeResult(format string, record map[string]any) (bool, error) {
	switch format {
	case "AVRO":
		fields, ok := record["fields"].(map[string]any)
		if !ok {
			return false, fmt.Errorf("AVRO: expected fields map, got %T", record["fields"])
		}
		idVal, _ := fields["id"].(string)
		nameVal, _ := fields["name"].(string)
		ageRaw := fields["age"]
		ageFloat, ageOK := ageRaw.(float64)
		if !ageOK {
			return false, fmt.Errorf("AVRO: age expected float64 from JSON, got %T (%v)", ageRaw, ageRaw)
		}
		pass := idVal == demoID && nameVal == demoName && int(ageFloat) == int(demoAge)
		return pass, nil

	case "JSON":
		payload, ok := record["payload"].(string)
		if !ok {
			return false, fmt.Errorf("JSON: expected payload string, got %T", record["payload"])
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
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
		fieldsJSON, ok := record["fieldsJson"].(string)
		if !ok {
			return false, fmt.Errorf("PROTOBUF: expected fieldsJson string, got %T", record["fieldsJson"])
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(fieldsJSON), &parsed); err != nil {
			return false, fmt.Errorf("PROTOBUF: parse fieldsJson: %w", err)
		}
		idVal, _ := parsed["id"].(string)
		nameVal, _ := parsed["name"].(string)
		ageRaw := parsed["age"]
		ageFloat, ageOK := ageRaw.(float64)
		if !ageOK {
			return false, fmt.Errorf("PROTOBUF: age expected float64, got %T (%v)", ageRaw, ageRaw)
		}
		pass := idVal == demoID && nameVal == demoName && int(ageFloat) == int(demoAge)
		return pass, nil

	default:
		return false, fmt.Errorf("verifyJavaConsumeResult: unsupported format %q", format)
	}
}

// ---------------------------------------------------------------------------
// narrateVerifyJava prints [verdict] output for Direction B (Java consumed).
// ---------------------------------------------------------------------------

func narrateVerifyJava(format string, record map[string]any, pass bool, err error) {
	printStage("verdict", "Equality check (Java sidecar envelope):")
	if err != nil {
		fmt.Printf("  Error verifying result: %v\n", err)
		printStage("verdict", "FAIL - verification error.")
		return
	}
	switch format {
	case "AVRO":
		if fields, ok := record["fields"].(map[string]any); ok {
			printEqualityCheck(demoID, fields["id"], "id")
			printEqualityCheck(demoName, fields["name"], "name")
			if ageFloat, ok := fields["age"].(float64); ok {
				printEqualityCheck(int(demoAge), int(ageFloat), "age")
			}
		}
	case "JSON":
		if payload, ok := record["payload"].(string); ok {
			var parsed map[string]any
			if jerr := json.Unmarshal([]byte(payload), &parsed); jerr == nil {
				printEqualityCheck(demoID, parsed["id"], "id")
				printEqualityCheck(demoName, parsed["name"], "name")
				if ageFloat, ok := parsed["age"].(float64); ok {
					printEqualityCheck(int(demoAge), int(ageFloat), "age")
				}
			}
		}
	case "PROTOBUF":
		if fieldsJSON, ok := record["fieldsJson"].(string); ok {
			var parsed map[string]any
			if jerr := json.Unmarshal([]byte(fieldsJSON), &parsed); jerr == nil {
				printEqualityCheck(demoID, parsed["id"], "id")
				printEqualityCheck(demoName, parsed["name"], "name")
				if ageFloat, ok := parsed["age"].(float64); ok {
					printEqualityCheck(int(demoAge), int(ageFloat), "age")
				}
			}
		}
	}
	if pass {
		fmt.Printf("  PASS - Go v1 encode -> Java decode (v2 registered) succeeded.\n")
	} else {
		fmt.Printf("  FAIL - one or more field mismatches.\n")
	}
	fmt.Println()
}

// ---------------------------------------------------------------------------
// runDirectionBFormat runs Direction B for a single format × compression cell.
// ---------------------------------------------------------------------------

// runDirectionBFormat executes Direction B (Go produces v1, Java consumes) for
// one format × compression. It:
//  1. Reuses Direction A's schema registrations (no re-registration).
//  2. Builds Go serializer with the schema config.
//  3. Go serializer encodes a v1 record, narrated [go-producer].
//  4. Produces framed bytes to the Direction B Kafka topic, [kafka].
//  5. Java sidecar consumes via KafkaConsume, [java-consumer].
//  6. Verifies Java's deserialized result matches source record, [verdict].
//
// Returns a ScenarioResult for the cell.
func runDirectionBFormat(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cell ScenarioCell,
) ScenarioResult {
	result := ScenarioResult{
		Format:      cell.Format,
		Direction:   "B",
		Compression: cell.Compression,
		Topic:       cell.Topic,
		SchemaName:  cell.SchemaName,
	}

	// ── Section header ───────────────────────────────────────────────────────
	printSectionHeader(fmt.Sprintf("FORMAT: %s | Direction B: Go produces at v1, Java consumes | Compression: %s", cell.Format, cell.Compression))

	// ── Step 1: Confirm schema reuse (no registration) ──────────────────────

	printStage("glue", "Reusing schema registered by Direction A (no re-registration).")
	fmt.Printf("  Schema name:   %s\n", cell.SchemaName)
	fmt.Printf("  Registry:      %s\n", cell.RegistryName)
	fmt.Printf("  v1 + v2 already present from Direction A.\n")
	fmt.Println()

	// ── Step 2: Build Go serializer ─────────────────────────────────────────

	printStage("go-producer", "Building Go serializer...")
	goConfig := configMapForFormat(cell.Region, cell.Format, cell.Compression)
	printGoConfig(goConfig)

	cfg, err := buildDemoConfigCellB(cell.Region, cell.Format, cell.Compression, cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("go-producer / build config: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	ser, err := serializer.NewSerializer(cfg)
	if err != nil {
		result.Err = fmt.Errorf("go-producer / NewSerializer: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	defer ser.Close() //nolint:errcheck

	// ── Step 3: Encode v1 record via Go serializer ──────────────────────────

	printStage("go-producer", "Encoding v1 record via Go serializer...")
	switch cell.Format {
	case "AVRO":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "JSON":
		fmt.Printf("  Record:      {\"id\": %q, \"name\": %q, \"age\": %d}\n", demoID, demoName, demoAge)
	case "PROTOBUF":
		fmt.Printf("  Record:      CrossVersionMessage{id: %q, name: %q, age: %d}\n", demoID, demoName, demoAge)
	}
	fmt.Printf("  Compression: %s\n", cell.Compression)
	fmt.Println()

	goRecord, err := goRecordForFormat(cell.Format, cell.V1Schema)
	if err != nil {
		result.Err = fmt.Errorf("go-producer / build record: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	// Serialize with schemaName as the topic parameter so
	// DefaultSchemaNameStrategy resolves to Direction A's registered schema.
	framed, err := ser.Serialize(cell.SchemaName, goRecord)
	if err != nil {
		result.Err = fmt.Errorf("go-producer / Serialize: %w", err)
		printStage("go-producer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("go-producer", "Encoded. Framed bytes (hex):")
	printHexDump("Wire bytes", framed)

	// ── Step 4: Produce to Kafka ────────────────────────────────────────────

	printStage("kafka", fmt.Sprintf("Producing framed bytes to Kafka topic: %s", cell.Topic))
	produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
	err = produceOneToKafka(produceCtx, broker.Bootstrap, cell.Topic, framed)
	produceCancel()
	if err != nil {
		result.Err = fmt.Errorf("kafka / produce: %w", err)
		printStage("kafka", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}
	printStage("kafka", "Produced successfully.")
	fmt.Println()

	// ── Step 5: Java sidecar consumes from Kafka ────────────────────────────

	printStage("java-consumer", fmt.Sprintf("Java sidecar consuming from topic: %s", cell.Topic))
	fmt.Printf("  Format:    %s\n", cell.Format)
	fmt.Printf("  Region:    %s\n", cell.Region)
	fmt.Printf("  Timeout:   60s\n")
	fmt.Println()

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 60*time.Second)
	resp, err := sc.KafkaConsume(consumeCtx, javasidecar.KafkaConsumeRequest{
		Bootstrap: broker.Bootstrap,
		Topic:     cell.Topic,
		Format:    cell.Format,
		Region:    cell.Region,
		TimeoutMs: 60_000,
	})
	consumeCancel()
	if err != nil {
		result.Err = fmt.Errorf("java-consumer / KafkaConsume: %w", err)
		printStage("java-consumer", fmt.Sprintf("FAIL: %v", result.Err))
		return result
	}

	printStage("java-consumer", "Java sidecar deserialized successfully.")
	fmt.Printf("  DataFormat:        %s\n", resp.DataFormat)
	fmt.Printf("  SchemaVersionID:   %s\n", resp.SchemaVersionID)
	fmt.Printf("  Record envelope:   %v\n", resp.Record)
	fmt.Println()
	result.SchemaVersionID = resp.SchemaVersionID

	// ── Step 6: Verify ──────────────────────────────────────────────────────

	pass, verifyErr := verifyJavaConsumeResult(cell.Format, resp.Record)
	narrateVerifyJava(cell.Format, resp.Record, pass, verifyErr)

	result.Pass = pass
	if verifyErr != nil {
		result.Err = verifyErr
	}
	return result
}

// ---------------------------------------------------------------------------
// runDirectionB — runs Direction B for all 3 formats × 2 compressions.
// ---------------------------------------------------------------------------

// runDirectionB orchestrates Direction B (Go produces v1, Java consumes) for
// Avro, JSON Schema, and Protobuf across both NONE and ZLIB compression. It
// reuses the schema registrations from Direction A (same schema names) but
// produces to different Kafka topics.
//
// The suffix parameter must match Direction A's suffix so schema names align.
func runDirectionB(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	suffix string,
	region string,
) []ScenarioResult {
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

	compressions := []string{"NONE", "ZLIB"}

	var results []ScenarioResult
	for _, f := range formats {
		// Schema name matches Direction A: no direction suffix in schema name.
		schemaName := fmt.Sprintf("%s%s-%s", demoPrefix, f.fmtLabel, suffix)

		for _, comp := range compressions {
			compLabel := strings.ToLower(comp)
			// Direction B Kafka topic uses "-b-" to avoid reading Direction A's messages.
			topic := fmt.Sprintf("%s%s-b-%s-%s", demoPrefix, f.fmtLabel, compLabel, suffix)

			cell := ScenarioCell{
				Format:      f.key,
				Direction:   "B",
				Compression: comp,
				SchemaName:  schemaName,
				Topic:       topic,
				V1Schema:    f.v1Schema,
				V2Schema:    f.v2Schema,
				RecordFields: map[string]interface{}{
					"id":   demoID,
					"name": demoName,
					"age":  demoAge,
				},
				Region:       region,
				RegistryName: demoRegistryName,
			}

			res := runDirectionBFormat(ctx, sc, broker, cell)
			results = append(results, res)
		}
	}
	return results
}

// ---------------------------------------------------------------------------
// runAllScenarios — wires Direction A (PBI-02) + Direction B (PBI-03).
// ---------------------------------------------------------------------------

// runAllScenarios orchestrates the full 12-cell demo (3 formats × 2
// compressions × 2 directions). Direction A (Java->Go) runs first to register
// schemas in Glue. Direction B (Go->Java) reuses those registrations by
// sharing the same random suffix.
func runAllScenarios(
	ctx context.Context,
	sc *javasidecar.Sidecar,
	broker *kafkaharness.Broker,
	cleanup *realglue.Cleanup,
	region string,
) []ScenarioResult {
	resultsA, suffix := runDirectionAWithSuffix(ctx, sc, broker, cleanup, region)
	if suffix == "" {
		// Direction A failed to even start — no schemas registered.
		return resultsA
	}

	fmt.Println()
	printStage("demo", "Direction A complete. Starting Direction B (Go->Java)...")
	fmt.Println()

	resultsB := runDirectionB(ctx, sc, broker, suffix, region)
	return append(resultsA, resultsB...)
}
