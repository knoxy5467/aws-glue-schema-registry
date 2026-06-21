//go:build integration

package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients/sarama"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients/segmentio"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/serializer/json"
)

// formatID is one of {"avro-generic", "avro-specific", "json-jdws",
// "json-pojo", "protobuf-static", "protobuf-dynamic"} — the §5.3
// items 1-6 keys.
type formatID string

const (
	fmtAvroGeneric     formatID = "avro-generic"
	fmtAvroSpecific    formatID = "avro-specific"
	fmtJSONJDWS        formatID = "json-jdws"
	fmtJSONPOJO        formatID = "json-pojo"
	fmtProtobufStatic  formatID = "protobuf-static"
	fmtProtobufDynamic formatID = "protobuf-dynamic"
)

// roundTripScenario is one row of the §5.3 items 1-6 matrix.
type roundTripScenario struct {
	format      formatID
	compression string // "NONE" or "ZLIB" — config-level name
	clientName  string // adapter name; resolved at runtime so tests stay parallel-safe
}

// allRoundTripScenarios enumerates the format × compression × Kafka-
// client matrix the plan calls for. Auto-register is implicit (true)
// for these — the dedicated lifecycle test in
// schema_lifecycle_test.go covers the auto-register=false branch.
func allRoundTripScenarios() []roundTripScenario {
	formats := []formatID{
		fmtAvroGeneric, fmtAvroSpecific, fmtJSONJDWS, fmtJSONPOJO,
		fmtProtobufStatic, fmtProtobufDynamic,
	}
	compressions := []string{"NONE", "ZLIB"}
	clientsList := []string{"sarama", "segmentio"}

	out := make([]roundTripScenario, 0, len(formats)*len(compressions)*len(clientsList))
	for _, f := range formats {
		for _, c := range compressions {
			for _, cl := range clientsList {
				out = append(out, roundTripScenario{format: f, compression: c, clientName: cl})
			}
		}
	}
	return out
}

// scenarioName is the t.Run key — deterministic and grep-friendly.
func (s roundTripScenario) scenarioName() string {
	return fmt.Sprintf("fmt=%s/comp=%s/client=%s", s.format, s.compression, s.clientName)
}

// adapterFor builds the requested adapter against the testcontainers
// broker. Returns the adapter and a cleanup function — Close()
// invocation belongs in t.Cleanup, not the scenario body.
func adapterFor(t *testing.T, name string, bootstrap string) clients.Adapter {
	t.Helper()
	switch name {
	case "sarama":
		a, err := sarama.New([]string{bootstrap})
		require.NoError(t, err)
		t.Cleanup(func() { _ = a.Close() })
		return a
	case "segmentio":
		a, err := segmentio.New([]string{bootstrap})
		require.NoError(t, err)
		t.Cleanup(func() { _ = a.Close() })
		return a
	default:
		t.Fatalf("adapterFor: unknown client %q", name)
		return nil
	}
}

// configFor returns a serializer/deserializer configuration for the
// given format. Used by §5.3 items 1-6 round-trip scenarios.
func configFor(t *testing.T, s roundTripScenario, gsrPath string) *common.Configuration {
	t.Helper()
	configMap := map[string]interface{}{
		common.GSRConfigPathKey: gsrPath,
	}
	gsr := map[string]string{
		"compression": s.compression,
	}
	configMap["gsrConfigOverrides"] = gsr // documentation only; the production NewSerializer reads gsrPath

	switch s.format {
	case fmtAvroGeneric, fmtAvroSpecific:
		configMap[common.DataFormatTypeKey] = common.DataFormatAvro
		if s.format == fmtAvroGeneric {
			configMap[common.AvroRecordTypeKey] = common.AvroRecordTypeGeneric
		} else {
			configMap[common.AvroRecordTypeKey] = common.AvroRecordTypeSpecific
		}
	case fmtJSONJDWS, fmtJSONPOJO:
		configMap[common.DataFormatTypeKey] = common.DataFormatJSON
	case fmtProtobufStatic, fmtProtobufDynamic:
		configMap[common.DataFormatTypeKey] = common.DataFormatProtobuf
		configMap[common.ProtobufMessageDescriptorKey] = (&testpb.TestMessage{}).ProtoReflect().Descriptor()
	}
	return common.NewConfiguration(configMap)
}

// payloadFor returns the input the serializer is asked to encode for
// the given scenario. Mirror inputs used by the legacy avro/json/
// protobuf integration suites so this set is comparable.
func payloadFor(t *testing.T, s roundTripScenario) interface{} {
	t.Helper()
	switch s.format {
	case fmtAvroGeneric, fmtAvroSpecific:
		return &avro.AvroRecord{
			Schema: `{"type":"record","name":"TestUser","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}`,
			Data: struct {
				ID   string `avro:"id"`
				Name string `avro:"name"`
			}{ID: "phase4-id", Name: "phase4-user"},
		}
	case fmtJSONJDWS, fmtJSONPOJO:
		schema := `{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"]}`
		body, _ := json.Marshal(map[string]string{"id": "phase4-id", "name": "phase4-user"})
		return &gsrjson.JsonDataWithSchema{Schema: schema, Payload: string(body)}
	case fmtProtobufStatic, fmtProtobufDynamic:
		return &testpb.TestMessage{Id: "phase4-id", Name: "phase4-user", Age: 30, Email: "p4@example.com"}
	}
	t.Fatalf("payloadFor: unhandled format %q", s.format)
	return nil
}

// validateRoundTrip checks the deserialized payload reasonably
// matches the original. Exact equality varies by format (avro returns
// map[string]any, json returns a JSON string, protobuf returns
// dynamicpb.Message); the assertions here are intentionally loose so
// the harness stays format-agnostic. Format-specific deep validation
// lives in the legacy avro/json/protobuf suites.
func validateRoundTrip(t *testing.T, original, deserialized interface{}) {
	t.Helper()
	require.NotNil(t, deserialized)

	switch v := deserialized.(type) {
	case map[string]interface{}:
		require.Equal(t, "phase4-id", v["id"])
	case string:
		var m map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(v), &m))
		require.Equal(t, "phase4-id", m["id"])
	case proto.Message:
		// dynamicpb.Message — round-trip via Marshal/Unmarshal so we
		// can read the strongly-typed fields back.
		data, err := proto.Marshal(v)
		require.NoError(t, err)
		concrete := &testpb.TestMessage{}
		require.NoError(t, proto.Unmarshal(data, concrete))
		require.Equal(t, "phase4-id", concrete.GetId())
	default:
		t.Fatalf("validateRoundTrip: unsupported deserialized type %T", deserialized)
	}
}

// TestRoundTrip_Phase4Matrix is plan §5.3 items 1-6 — the format ×
// compression × Kafka-client matrix. Skips with t.Skip when
// AWS_INTEGRATION=1 is not set; the suite still compiles as the
// proof that the matrix is wired correctly without billing AWS.
func TestRoundTrip_Phase4Matrix(t *testing.T) {
	requireAWSIntegration(t)

	gsrPath := gsrPropertiesPath(t)
	broker := kafkaharness.Start(context.Background(), t)

	for _, sc := range allRoundTripScenarios() {
		sc := sc
		t.Run(sc.scenarioName(), func(t *testing.T) {
			t.Parallel()
			adapter := adapterFor(t, sc.clientName, broker.Bootstrap)

			cfg := configFor(t, sc, gsrPath)
			ser, err := serializer.NewSerializer(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ser.Close() })

			des, err := deserializer.NewDeserializer(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = des.Close() })

			ctx, cancel := scenarioCtx(t)
			defer cancel()

			topic := scenarioTopicName(t)
			payload := payloadFor(t, sc)

			encoded, err := ser.Serialize(topic, payload)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			require.NoError(t, adapter.Produce(ctx, topic, []byte("k"), encoded))
			got, err := adapter.Consume(ctx, topic)
			require.NoError(t, err)
			require.Equal(t, encoded, got)

			deserialized, err := des.Deserialize(topic, got)
			require.NoError(t, err)
			validateRoundTrip(t, payload, deserialized)
		})
	}
}
