//go:build integration

package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/clients"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/avro"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"
	gsrjson "github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer/json"
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

// adapterCtors is the single source of truth for which Kafka clients
// the §5.3 round-trip matrix exercises. Each adapter package
// registers itself via init() in a *_register_test.go file:
//
//   - tests/round_trip_sarama_register_test.go
//   - tests/round_trip_segmentio_register_test.go
//   - tests/round_trip_confluent_test.go (build-tag gated)
//
// Adding franz-go (a future plan §4 table item) is one new file
// under tests/ — no switch to edit, no separate "extra" mechanism,
// and the matrix size automatically tracks len(adapterCtors). Order
// is deterministic via sorted keys so subtest names stay stable.
var adapterCtors = map[string]func(t *testing.T, bootstrap string) clients.Adapter{}

// registerAdapter is called from each adapter's *_register_test.go
// init(). Panics on duplicate registration so a copy-paste error
// surfaces at the first `go test` invocation rather than silently
// overwriting an entry.
func registerAdapter(name string, ctor func(t *testing.T, bootstrap string) clients.Adapter) {
	if _, exists := adapterCtors[name]; exists {
		panic("registerAdapter: duplicate registration for " + name)
	}
	adapterCtors[name] = ctor
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

	// Sort adapter names so the matrix produces deterministic t.Run
	// keys regardless of init() ordering.
	clientsList := make([]string, 0, len(adapterCtors))
	for name := range adapterCtors {
		clientsList = append(clientsList, name)
	}
	sort.Strings(clientsList)

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

// adapterFor looks up the requested adapter in the registry and
// builds it. t.Cleanup is the constructor's responsibility.
func adapterFor(t *testing.T, name string, bootstrap string) clients.Adapter {
	t.Helper()
	ctor, ok := adapterCtors[name]
	if !ok {
		t.Fatalf("adapterFor: unknown client %q (registered: %v)", name, sortedKeys(adapterCtors))
	}
	return ctor(t, bootstrap)
}

func sortedKeys(m map[string]func(t *testing.T, bootstrap string) clients.Adapter) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// configFor returns a serializer/deserializer configuration for the
// given format. Used by §5.3 items 1-6 round-trip scenarios.
//
// common.NewConfiguration accepts a map[string]string under
// common.GSRConfigPathKey and threads it verbatim into the GsrEncoder
// constructor (see common/configuration.go validateAndSetGsrConfig).
// That's the seam scenarios use to override `compression`, `region`,
// `schemaAutoRegistrationEnabled`, etc.
func configFor(t *testing.T, s roundTripScenario) *common.Configuration {
	t.Helper()
	gsrMap := map[string]string{
		"region":                        defaultAWSRegion,
		"registry.name":                 testRegistryName,
		"compression":                   s.compression,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.GSRConfigPathKey: gsrMap,
	}

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
	const avroSchema = `{"type":"record","name":"TestUser","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}`
	const jsonSchema = `{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"]}`
	switch s.format {
	case fmtAvroGeneric:
		// Generic Avro: payload is a map keyed by field name — hamba/
		// avro's map-driven path, distinct from struct tag-mapping.
		return &avro.AvroRecord{
			Schema: avroSchema,
			Data:   map[string]any{"id": "phase4-id", "name": "phase4-user"},
		}
	case fmtAvroSpecific:
		// Specific Avro: payload is a Go struct with `avro:` tags —
		// hamba's reflection-driven path.
		return &avro.AvroRecord{
			Schema: avroSchema,
			Data: struct {
				ID   string `avro:"id"`
				Name string `avro:"name"`
			}{ID: "phase4-id", Name: "phase4-user"},
		}
	case fmtJSONJDWS:
		// JsonDataWithSchema wrapper: caller supplies the JSON schema
		// + body separately. (The current Go JSON serializer ONLY
		// accepts this wrapper; the POJO-direct path is a §2.2 gap
		// tracked separately. See fmtJSONPOJO below.)
		body, _ := json.Marshal(map[string]string{"id": "phase4-id", "name": "phase4-user"})
		return &gsrjson.JsonDataWithSchema{Schema: jsonSchema, Payload: string(body)}
	case fmtJSONPOJO:
		// POJO-style: a struct-shaped JSON body. The Go JSON serializer
		// only accepts JsonDataWithSchema today (a §2.2 stub gap), so
		// we round-trip a struct THROUGH the wrapper. The distinct
		// shape — struct-with-tags rather than map[string]string —
		// catches body-shape regressions even though the wire-format
		// code path is shared with fmtJSONJDWS until the POJO-direct
		// serializer lands.
		type pojo struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		body, _ := json.Marshal(pojo{ID: "phase4-id", Name: "phase4-user"})
		return &gsrjson.JsonDataWithSchema{Schema: jsonSchema, Payload: string(body)}
	case fmtProtobufStatic:
		// Static: concrete *testpb.TestMessage — pb-generated type
		// driving the proto.Marshal fast path.
		return &testpb.TestMessage{Id: "phase4-id", Name: "phase4-user", Age: 30, Email: "p4@example.com"}
	case fmtProtobufDynamic:
		// Dynamic: build a dynamicpb.Message from the static type's
		// descriptor and populate it via reflection. This exercises
		// the dynamicpb codec path that's distinct from the
		// generated-type fast path.
		md := (&testpb.TestMessage{}).ProtoReflect().Descriptor()
		dyn := dynamicpb.NewMessage(md)
		fields := md.Fields()
		setStringField := func(name, val string) {
			f := fields.ByName(protoreflect.Name(name))
			require.NotNil(t, f, "protobuf-dynamic: missing field %q in descriptor", name)
			dyn.Set(f, protoreflect.ValueOfString(val))
		}
		setInt32Field := func(name string, val int32) {
			f := fields.ByName(protoreflect.Name(name))
			require.NotNil(t, f, "protobuf-dynamic: missing field %q in descriptor", name)
			dyn.Set(f, protoreflect.ValueOfInt32(val))
		}
		setStringField("id", "phase4-id")
		setStringField("name", "phase4-user")
		setInt32Field("age", 30)
		setStringField("email", "p4@example.com")
		return dyn
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

	broker := kafkaharness.Start(context.Background(), t)

	for _, sc := range allRoundTripScenarios() {
		sc := sc
		t.Run(sc.scenarioName(), func(t *testing.T) {
			t.Parallel()
			adapter := adapterFor(t, sc.clientName, broker.Bootstrap)

			cfg := configFor(t, sc)
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
