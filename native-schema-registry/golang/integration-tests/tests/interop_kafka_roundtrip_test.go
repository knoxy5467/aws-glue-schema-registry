//go:build integration

// Phase 4.6 deliverable 4b — Kafka-in-the-loop interop.
//
// These tests model the real producer/consumer split where the two
// sides are written in different languages. Two scenarios:
//
//  1. Java produces, Go consumes.
//     - Java sidecar /kafka-produce: register schema with real Glue,
//       frame the payload with the GSR header, publish to a Kafka topic.
//     - Go: drain the topic via sarama, run gsrserde.NewGsrDecoder
//       against real Glue to resolve the UUID, assert payload equality.
//
//  2. Go produces, Java consumes.
//     - Go: NewGsrEncoder registers the schema with real Glue, frames,
//       publish via sarama.
//     - Java sidecar /kafka-consume: poll the topic, parse the header,
//       resolve the UUID against real Glue, return payload + schema
//       metadata. Assert payload equality on the Go side.
//
// THIS BILLS AWS. Gated by AWS_INTEGRATION=1 + GSR_GLUE=real;
// otherwise t.Skip. Sidecar must be in local mode because the Java
// process talks to the testcontainers-go Kafka broker over the host
// network — container mode would put the Java JVM on a Docker network
// where the kafka container's host-side mapped port is not reachable.
//
// Schemas registered by these tests use the "gsr-go-it-" prefix so
// realglue.Cleanup deletes them at test teardown.

package integration_tests

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/javasidecar"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/realglue"
)

// requireKafkaInterop layers requireInterop's checks with the extra
// real-Glue + container-mode-prohibited rules these tests need.
func requireKafkaInterop(t *testing.T) {
	t.Helper()
	requireInterop(t)
	if os.Getenv("AWS_INTEGRATION") != "1" {
		t.Skip("kafka-roundtrip interop tests require AWS_INTEGRATION=1")
	}
	if strings.ToLower(os.Getenv("GSR_GLUE")) != "real" {
		t.Skip("kafka-roundtrip interop tests require GSR_GLUE=real (they bill AWS)")
	}
	mode, _ := javasidecar.ModeFromEnv()
	if mode == javasidecar.ModeContainer {
		t.Skip("kafka-roundtrip interop tests need GSR_INTEROP_MODE=local: the in-container JVM cannot reach the host-mapped Kafka port")
	}
}

// kafkaInteropCase is a flattened §5.2 cell for the Kafka-in-the-loop
// matrix. Format axis only — record-type is moot because both sides
// just pass bytes around the §5.5 contract.
type kafkaInteropCase struct {
	name        string
	format      string
	schema      string
	compression string
}

var kafkaInteropMatrix = []kafkaInteropCase{
	{name: "AVRO/NONE", format: "AVRO", schema: avroSchema, compression: "NONE"},
	{name: "AVRO/ZLIB", format: "AVRO", schema: avroSchema, compression: "ZLIB"},
	{name: "JSON/NONE", format: "JSON", schema: jsonSchema, compression: "NONE"},
	{name: "PROTOBUF/ZLIB", format: "PROTOBUF", schema: protoSchema, compression: "ZLIB"},
}

// uniqueSuffix produces a short collision-safe suffix for schema and
// topic names so concurrent test runs in the same beta account don't
// collide.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(buf[:])
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range kafkaInteropMatrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueSuffix(t)
			schemaName := "gsr-go-it-interop-" + strings.ToLower(tc.format) + "-" + suffix
			topic := "gsr-go-it-interop-" + suffix
			payload := []byte("java-produce-go-consume:" + tc.name)

			cleanup.TrackSchema("default-registry", schemaName)

			produced, err := sc.KafkaProduce(ctx, javasidecar.KafkaProduceRequest{
				Format:      tc.format,
				Schema:      tc.schema,
				SchemaName:  schemaName,
				Payload:     payload,
				Compression: tc.compression,
				Bootstrap:   broker.Bootstrap,
				Topic:       topic,
				Region:      real.Region,
			})
			require.NoError(t, err, "java /kafka-produce")
			require.NotEmpty(t, produced.SchemaVersionID)

			framed := consumeOne(t, ctx, broker.Bootstrap, topic)

			dec, err := gsrcore.NewGsrDecoder(map[string]string{
				"region":        real.Region,
				"registry.name": "default-registry",
			})
			require.NoError(t, err, "NewGsrDecoder")
			t.Cleanup(func() { _ = dec.Close() })

			decoded, err := dec.Decode(framed)
			require.NoError(t, err, "go Decode of java-produced bytes")
			require.Equal(t, payload, decoded)
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cleanup.Run(ctx)
	})

	for _, tc := range kafkaInteropMatrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			suffix := uniqueSuffix(t)
			schemaName := "gsr-go-it-interop-" + strings.ToLower(tc.format) + "-" + suffix
			topic := "gsr-go-it-interop-" + suffix
			payload := []byte("go-produce-java-consume:" + tc.name)

			cleanup.TrackSchema("default-registry", schemaName)

			enc, err := gsrcore.NewGsrEncoder(map[string]string{
				"region":                           real.Region,
				"registry.name":                    "default-registry",
				"compressionType":                  tc.compression,
				"schemaAutoRegistrationEnabled":    "true",
			})
			require.NoError(t, err, "NewGsrEncoder")
			t.Cleanup(func() { _ = enc.Close() })

			schema := &gsrcore.Schema{
				SchemaDefinition: tc.schema,
				DataFormat:       tc.format,
				SchemaName:       schemaName,
			}
			framed, err := enc.Encode(payload, topic, schema)
			require.NoError(t, err, "go encode + register")

			produceOne(t, ctx, broker.Bootstrap, topic, framed)

			resp, err := sc.KafkaConsume(ctx, javasidecar.KafkaConsumeRequest{
				Bootstrap: broker.Bootstrap,
				Topic:     topic,
				Region:    real.Region,
				TimeoutMs: 60_000,
			})
			require.NoError(t, err, "java /kafka-consume")
			require.Equal(t, payload, resp.Payload)
			require.Equal(t, tc.format, resp.DataFormat)
			require.Equal(t, tc.schema, resp.SchemaDefinition)
		})
	}
}

// consumeOne reads one message from topic using sarama. Bounded by ctx.
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

	select {
	case msg := <-pc.Messages():
		return msg.Value
	case err := <-pc.Errors():
		t.Fatalf("partition consumer error: %v", err)
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

	type result struct {
		err error
	}
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

// Silence unused-import warnings when the file builds but the gates
// short-circuit on env. fmt is used in a sprintf elsewhere — keep below
// to avoid a false-positive on a future refactor.
var _ = fmt.Sprintf
