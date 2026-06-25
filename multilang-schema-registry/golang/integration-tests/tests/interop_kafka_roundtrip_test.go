//go:build integration

// Phase 4.6.5 — Java<->Go Kafka-in-the-loop interop.
//
// Both sides drive their full Kafka customer API:
//   - Java sidecar: GlueSchemaRegistryKafkaSerializer.serialize(topic,
//     record) and GlueSchemaRegistryKafkaDeserializer.deserialize(topic,
//     bytes). Same library entry point a real Java Kafka customer uses.
//   - Go test: pkg/gsrserde-go/serializer.Serializer.Serialize(topic,
//     data) and pkg/gsrserde-go/deserializer.Deserializer.Deserialize.
//     The Kafka transport is plain sarama on the Go side and plain
//     KafkaProducer<byte[],byte[]> on the Java side; the GSR ser/des
//     produces/consumes bytes — transport is independent.
//
// Two scenarios, four matrix cells each:
//   1. Java produces (sidecar /kafka-produce), Go consumes via sarama +
//      Deserializer. Assert payload field-level equality.
//   2. Go produces via Serializer + sarama, Java consumes (sidecar
//      /kafka-consume). Assert payload field-level equality.
//
// THIS BILLS AWS. Gated by AWS_INTEGRATION=1 + GSR_GLUE=real +
// GSR_INTEROP_MODE=local. Schemas registered use the gsr-go-it- prefix so
// realglue.Cleanup deletes them at test teardown.

package integration_tests

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/realglue"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb"
	gsravro "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer/json"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// requireKafkaInterop combines requireInterop + AWS_INTEGRATION + GSR_GLUE=real
// + local mode (the in-container JVM can't reach the testcontainers Kafka
// broker over the host-mapped port).
func requireKafkaInterop(t *testing.T) {
	t.Helper()
	mode, err := javasidecar.ModeFromEnv()
	if err != nil {
		t.Skipf("javasidecar: %v", err)
	}
	if mode == javasidecar.ModeLocal {
		if _, err := exec.LookPath("java"); err != nil {
			t.Skipf("kafka-roundtrip interop: local mode needs `java` on PATH")
		}
	}
	if os.Getenv("AWS_INTEGRATION") != "1" {
		t.Skip("kafka-roundtrip interop tests require AWS_INTEGRATION=1")
	}
	if strings.ToLower(os.Getenv("GSR_GLUE")) != "real" {
		t.Skip("kafka-roundtrip interop tests require GSR_GLUE=real (they bill AWS)")
	}
	if mode == javasidecar.ModeContainer {
		t.Skip("kafka-roundtrip interop needs GSR_INTEROP_MODE=local")
	}
}

// kafkaInteropCase represents one (format, compression) cell. Each case
// supplies both the Go-side typed record AND the matching JSON envelope
// the Java sidecar expects — round-trippable in both directions.
type kafkaInteropCase struct {
	name           string
	format         string
	compression    string
	schema         string
	schemaNameBase string
	// goRecord builds the typed Go record for serializer.Serializer.Serialize.
	goRecord func() interface{}
	// javaRecord builds the JSON envelope the sidecar expects on
	// /kafka-produce — same logical record as goRecord.
	javaRecord func() map[string]any
	// goCheck validates the Go-deserialized record matches what the Java
	// side produced. Called with the result of Deserializer.Deserialize.
	goCheck func(t *testing.T, got interface{})
	// javaEnvelopeCheck validates the JSON envelope the sidecar returned
	// matches what the Go side produced.
	javaEnvelopeCheck func(t *testing.T, env map[string]any)
}

const (
	interopAvroSchema = `{
		"type": "record",
		"name": "InteropRecord",
		"namespace": "test",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`
	interopJSONSchema = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["id", "name", "age"]
	}`
)

const interopID = "interop-id"
const interopName = "interop-name"
const interopAge = 42

func kafkaInteropMatrix() []kafkaInteropCase {
	jsonBody := mustJSON(map[string]any{"id": interopID, "name": interopName, "age": interopAge})
	cases := []kafkaInteropCase{
		{
			name:           "AVRO/NONE",
			format:         "AVRO",
			compression:    "NONE",
			schema:         interopAvroSchema,
			schemaNameBase: "avro",
			goRecord: func() interface{} {
				return &gsravro.AvroRecord{
					Schema: interopAvroSchema,
					Data: map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  int32(interopAge),
					},
				}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"fields": map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  interopAge,
					},
				}
			},
			goCheck: func(t *testing.T, got interface{}) {
				m, ok := got.(map[string]interface{})
				require.True(t, ok, "AVRO consumer: expected map[string]any, got %T", got)
				require.Equal(t, interopID, m["id"])
				require.Equal(t, interopName, m["name"])
			},
			javaEnvelopeCheck: javaAvroEnvelopeCheck,
		},
		{
			name:           "AVRO/ZLIB",
			format:         "AVRO",
			compression:    "ZLIB",
			schema:         interopAvroSchema,
			schemaNameBase: "avro",
			goRecord: func() interface{} {
				return &gsravro.AvroRecord{
					Schema: interopAvroSchema,
					Data: map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  int32(interopAge),
					},
				}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"fields": map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  interopAge,
					},
				}
			},
			goCheck: func(t *testing.T, got interface{}) {
				m, ok := got.(map[string]interface{})
				require.True(t, ok, "AVRO consumer: expected map[string]any, got %T", got)
				require.Equal(t, interopID, m["id"])
				require.Equal(t, interopName, m["name"])
			},
			javaEnvelopeCheck: javaAvroEnvelopeCheck,
		},
		{
			name:           "JSON/NONE",
			format:         "JSON",
			compression:    "NONE",
			schema:         interopJSONSchema,
			schemaNameBase: "json",
			goRecord: func() interface{} {
				return &gsrjson.JsonDataWithSchema{Schema: interopJSONSchema, Payload: jsonBody}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"schema":  interopJSONSchema,
					"payload": jsonBody,
				}
			},
			goCheck: javaJSONGoCheck,
			javaEnvelopeCheck: func(t *testing.T, env map[string]any) {
				payload, _ := env["payload"].(string)
				assertJSONFieldsMatch(t, payload)
			},
		},
		{
			name:           "JSON/ZLIB",
			format:         "JSON",
			compression:    "ZLIB",
			schema:         interopJSONSchema,
			schemaNameBase: "json",
			goRecord: func() interface{} {
				return &gsrjson.JsonDataWithSchema{Schema: interopJSONSchema, Payload: jsonBody}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"schema":  interopJSONSchema,
					"payload": jsonBody,
				}
			},
			goCheck: javaJSONGoCheck,
			javaEnvelopeCheck: func(t *testing.T, env map[string]any) {
				payload, _ := env["payload"].(string)
				assertJSONFieldsMatch(t, payload)
			},
		},
		{
			name:           "PROTOBUF/NONE",
			format:         "PROTOBUF",
			compression:    "NONE",
			schemaNameBase: "protobuf",
			schema:         interopProtoSchema(),
			goRecord: func() interface{} {
				return &testpb.TestMessage{Id: interopID, Name: interopName, Age: interopAge}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"messageTypeFullName": "test.TestMessage",
					"fieldsJson": mustJSON(map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  interopAge,
					}),
				}
			},
			goCheck:           protoGoCheck,
			javaEnvelopeCheck: protoJavaEnvelopeCheck,
		},
		{
			name:           "PROTOBUF/ZLIB",
			format:         "PROTOBUF",
			compression:    "ZLIB",
			schemaNameBase: "protobuf",
			schema:         interopProtoSchema(),
			goRecord: func() interface{} {
				return &testpb.TestMessage{Id: interopID, Name: interopName, Age: interopAge}
			},
			javaRecord: func() map[string]any {
				return map[string]any{
					"messageTypeFullName": "test.TestMessage",
					"fieldsJson": mustJSON(map[string]any{
						"id":   interopID,
						"name": interopName,
						"age":  interopAge,
					}),
				}
			},
			goCheck:           protoGoCheck,
			javaEnvelopeCheck: protoJavaEnvelopeCheck,
		},
	}
	return cases
}

// javaAvroEnvelopeCheck validates a Java-consumed AVRO envelope ({"fields":
// {...}}) recovered our record's fields. Field types Java emits depend on
// the Avro schema, but for our test schema (string/string/int) they're
// stable.
func javaAvroEnvelopeCheck(t *testing.T, env map[string]any) {
	t.Helper()
	fields, ok := env["fields"].(map[string]any)
	require.True(t, ok, "AVRO envelope: missing fields map (env=%v)", env)
	require.Equal(t, interopID, fields["id"])
	require.Equal(t, interopName, fields["name"])
	// JSON parsing turns numbers into float64; equality compare via int().
	age, ok := fields["age"].(float64)
	require.True(t, ok, "AVRO age field: expected float64 (from JSON), got %T (%v)", fields["age"], fields["age"])
	require.Equal(t, interopAge, int(age))
}

// javaJSONGoCheck validates a Go-deserialized JSON record returns a string
// (the Go JSON deserializer returns the payload string verbatim).
func javaJSONGoCheck(t *testing.T, got interface{}) {
	t.Helper()
	s, ok := got.(string)
	require.True(t, ok, "JSON consumer: expected string payload, got %T", got)
	assertJSONFieldsMatch(t, s)
}

func protoGoCheck(t *testing.T, got interface{}) {
	t.Helper()
	// Go deserializer returns a proto.Message — could be dynamicpb or the
	// concrete *testpb.TestMessage depending on config. Round-trip via
	// Marshal/Unmarshal so the assertions work either way.
	m, ok := got.(proto.Message)
	require.True(t, ok, "PROTOBUF consumer: expected proto.Message, got %T", got)
	data, err := proto.Marshal(m)
	require.NoError(t, err)
	concrete := &testpb.TestMessage{}
	require.NoError(t, proto.Unmarshal(data, concrete))
	require.Equal(t, interopID, concrete.GetId())
	require.Equal(t, interopName, concrete.GetName())
	require.Equal(t, int32(interopAge), concrete.GetAge())
}

func protoJavaEnvelopeCheck(t *testing.T, env map[string]any) {
	t.Helper()
	fieldsJSON, ok := env["fieldsJson"].(string)
	require.True(t, ok, "PROTOBUF envelope: missing fieldsJson (env=%v)", env)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(fieldsJSON), &m))
	require.Equal(t, interopID, m["id"])
	require.Equal(t, interopName, m["name"])
	age, ok := m["age"].(float64)
	require.True(t, ok, "PROTOBUF age: expected float64 from JSON, got %T", m["age"])
	require.Equal(t, interopAge, int(age))
}

// assertJSONFieldsMatch parses a JSON payload and asserts our 3 fields.
func assertJSONFieldsMatch(t *testing.T, payload string) {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(payload), &m))
	require.Equal(t, interopID, m["id"])
	require.Equal(t, interopName, m["name"])
	age, ok := m["age"].(float64)
	require.True(t, ok, "JSON age: expected float64, got %T", m["age"])
	require.Equal(t, interopAge, int(age))
}

// interopProtoSchema returns the .proto text the Go-side serializer emits
// for *testpb.TestMessage. The Java sidecar parses this with
// FileDescriptorUtils.protoFileToFileDescriptor.
func interopProtoSchema() string {
	// Match what ProtobufSerializer.GetSchemaDefinition emits at runtime —
	// jhump protoprint output for the testpb file descriptor.
	return `syntax = "proto3";

package test;

option go_package = "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb";

message TestMessage {
  string id = 1;

  string name = 2;

  int32 age = 3;

  string email = 4;

  repeated string tags = 5;
}
`
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("mustJSON: %v", err))
	}
	return string(b)
}

func uniqueInteropSuffix(t *testing.T) string {
	t.Helper()
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(buf[:])
}

// buildGoConfig builds the Go-side Configuration that NewSerializer/
// NewDeserializer expect: real region, default registry, schema
// auto-registration on, and per-format DataFormatType / descriptor.
func buildGoConfig(t *testing.T, region, schemaName, format, compression string) *common.Configuration {
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
		configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
		configMap[common.ProtobufMessageDescriptorKey] = (&testpb.TestMessage{}).ProtoReflect().Descriptor()
	default:
		t.Fatalf("buildGoConfig: unsupported format %q", format)
	}
	return common.NewConfiguration(configMap)
}

// TestInterop_KafkaJavaProduce_GoConsume — direction 1.
func TestInterop_KafkaJavaProduce_GoConsume(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	// Belt-and-suspenders: subtests TrackSchema explicitly with their
	// computed schemaName, BUT the Go-produce direction's
	// DefaultSchemaNameStrategy uses topic-as-schema-name so the
	// registered Glue name can diverge from what the test predicts. The
	// prefix scan picks up any gsr-go-it-* schema this run created and
	// deletes them at teardown, even if the explicit Track missed.
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-interop-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range kafkaInteropMatrix() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueInteropSuffix(t)
			schemaName := "gsr-go-it-interop-" + tc.schemaNameBase + "-" + suffix
			topic := "gsr-go-it-interop-" + suffix
			cleanup.TrackSchema("default-registry", schemaName)

			// Java side produces via GlueSchemaRegistryKafkaSerializer.
			_, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
				Format:      tc.format,
				Schema:      tc.schema,
				SchemaName:  schemaName,
				Record:      tc.javaRecord(),
				Compression: tc.compression,
				Bootstrap:   broker.Bootstrap,
				Topic:       topic,
				Region:      real.Region,
			})
			require.NoError(t, err, "java kafka-produce")

			// Go side consumes via sarama (transport) +
			// deserializer.Deserializer (real Glue lookup + decode).
			framed := consumeOne(t, ctx, broker.Bootstrap, topic)
			des, err := deserializer.NewDeserializer(buildGoConfig(t, real.Region, schemaName, tc.format, tc.compression))
			require.NoError(t, err, "go NewDeserializer")
			t.Cleanup(func() { _ = des.Close() })

			got, err := des.Deserialize(topic, framed)
			require.NoError(t, err, "go Deserialize")
			tc.goCheck(t, got)
		})
	}
}

// TestInterop_KafkaGoProduce_JavaConsume — direction 2.
func TestInterop_KafkaGoProduce_JavaConsume(t *testing.T) {
	requireKafkaInterop(t)

	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	sc := startInteropSidecar(startCtx, t)

	broker := kafkaharness.Start(startCtx, t)

	real, err := realglue.New(startCtx)
	require.NoError(t, err, "realglue.New")
	cleanup := real.NewCleanup()
	// Belt-and-suspenders: subtests TrackSchema explicitly with their
	// computed schemaName, BUT the Go-produce direction's
	// DefaultSchemaNameStrategy uses topic-as-schema-name so the
	// registered Glue name can diverge from what the test predicts. The
	// prefix scan picks up any gsr-go-it-* schema this run created and
	// deletes them at teardown, even if the explicit Track missed.
	cleanup.TrackSchemaPrefix("default-registry", "gsr-go-it-interop-")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range kafkaInteropMatrix() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueInteropSuffix(t)
			// On the Go-produce side, the Go serializer's
			// DefaultSchemaNameStrategy uses topic-as-schema-name, so the
			// schema registered in Glue is the topic, not whatever name we
			// might prefer. Track the topic so cleanup deletes it.
			topic := "gsr-go-it-interop-" + tc.schemaNameBase + "-" + suffix
			schemaName := topic
			cleanup.TrackSchema("default-registry", schemaName)

			// Go side produces via serializer.Serializer (real Glue
			// register + format-specific encode) + sarama (transport).
			ser, err := serializer.NewSerializer(buildGoConfig(t, real.Region, schemaName, tc.format, tc.compression))
			require.NoError(t, err, "go NewSerializer")
			t.Cleanup(func() { _ = ser.Close() })

			framed, err := ser.Serialize(topic, tc.goRecord())
			require.NoError(t, err, "go Serialize")
			require.NotEmpty(t, framed)
			produceOne(t, ctx, broker.Bootstrap, topic, framed)

			// Java side consumes via GlueSchemaRegistryKafkaDeserializer.
			resp, err := sc.KafkaConsume(ctx, javasidecar.KafkaConsumeRequest{
				Bootstrap: broker.Bootstrap,
				Topic:     topic,
				Format:    tc.format,
				Region:    real.Region,
				TimeoutMs: 60_000,
			})
			require.NoError(t, err, "java kafka-consume")
			require.Equal(t, tc.format, resp.DataFormat)
			tc.javaEnvelopeCheck(t, resp.Record)
		})
	}
}

// consumeOne reads one message from topic using sarama. Bounded by ctx.
//
// Transient partition-consumer errors (e.g. broker leader election during
// container churn, or KAFKA_BROKER reuse on a not-quite-ready broker) are
// drained in a background goroutine and logged via t.Logf rather than
// failing the test. Only ctx expiry produces a fatal — that's the real
// signal "no message ever arrived".
func consumeOne(t *testing.T, ctx context.Context, bootstrap, topic string) []byte {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer([]string{bootstrap}, cfg)
	require.NoError(t, err, "sarama.NewConsumer")
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	require.NoError(t, err, "ConsumePartition")
	defer pc.Close()

	// Drain errors in the background so they don't race with Messages().
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case e, ok := <-pc.Errors():
				if !ok {
					return
				}
				t.Logf("consumeOne(%s): transient partition error: %v", topic, e)
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case msg := <-pc.Messages():
		return msg.Value
	case <-ctx.Done():
		t.Fatalf("timed out waiting for kafka message on %s: %v", topic, ctx.Err())
	}
	return nil // unreachable
}

// produceOne sends one message via sarama's sync producer.
func produceOne(t *testing.T, ctx context.Context, bootstrap, topic string, value []byte) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{bootstrap}, cfg)
	require.NoError(t, err, "sarama.NewSyncProducer")
	defer producer.Close()

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		})
		done <- result{err: err}
	}()
	select {
	case r := <-done:
		require.NoError(t, r.err, "produce")
	case <-ctx.Done():
		t.Fatalf("timed out producing to %s: %v", topic, ctx.Err())
	}
}
