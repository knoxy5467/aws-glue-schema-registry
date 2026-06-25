//go:build integration
// +build integration

package integration_tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/pkg/kafkaharness"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/integration-tests/testpb"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/common"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/deserializer"
	"github.com/awslabs/aws-glue-schema-registry/multilang-schema-registry/golang/pkg/gsrserde-go/serializer"
)

// TestMultiThreadedShared_NoRaces_NoDoubleRegister exercises spec §5 S4
// (plan §5.3 item 30) at full scale. One *Serializer and one
// *Deserializer instance are shared across N=16 goroutines, each
// producing+consuming M=8 messages through the existing Sarama
// transport against a testcontainers-managed Kafka broker plus real
// AWS Glue in account 850995546034.
//
// Pass criteria:
//  1. `go test -race` reports no data races (C7 / C12 / R3).
//  2. Every goroutine round-trips successfully (totalEncoded == 128).
//  3. Each of the 3 unique schema names produces exactly one Glue
//     schema version — verified post-run via ListSchemaVersions —
//     proving the encoder's singleflight + already-exists recovery
//     paths collapse concurrent first-encodes onto a single
//     CreateSchema call per schema (C7 / spec §10 second invariant).
//
// Gating: scenarioGate(t, true, false) — requires GSR_GLUE=real and
// AWS_INTEGRATION=1. Without real Glue the no-double-register
// assertion is meaningless (fakeglue does not enforce
// AlreadyExistsException semantics the same way Glue does), so we
// skip rather than degrade silently.
//
// Schema cleanup: every unique schema name is registered with
// realglue.Cleanup.TrackSchema BEFORE the first encode for that name,
// per spec §6 hard requirement. The newGlueHandle-installed t.Cleanup
// hook runs Cleanup.Run on test exit so no schemas leak in account
// 850995546034.
func TestMultiThreadedShared_NoRaces_NoDoubleRegister(t *testing.T) {
	scenarioGate(t, true, false) // requires GSR_GLUE=real + AWS_INTEGRATION=1
	h := newGlueHandle(t)

	// Kafka harness — same testcontainers-go pattern as the other
	// Tier-2 suites. TestMain pre-starts a shared broker when
	// AWS_INTEGRATION=1 and exposes it via KAFKA_BROKER, so this
	// Start call short-circuits on the second invocation.
	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	broker := kafkaharness.Start(startCtx, t)
	bootstrap := resolveKafkaBroker(broker)

	// Build the Configuration shared by serializer + deserializer.
	// Inline-map pattern matches the working real-AWS integration tests
	// (round_trip_test.go, interop_kafka_roundtrip_test.go,
	// interop_crossversion_kafka_test.go). The earlier `filepath.Abs(./gsr.properties)`
	// pattern silently failed: validateAndSetGsrConfig only honors
	// map[string]string under GSRConfigPathKey, so the string path was
	// dropped by the type assertion and SchemaAutoRegistrationEnabled
	// defaulted to false — making encode fail on first hit. The strict
	// auto-register check at encoder.go:292 was added by Phase 4.5 and
	// surfaced this latent bug under real-Glue.
	gsrMap := map[string]string{
		"region":                        defaultAWSRegion,
		"registry.name":                 testRegistryName,
		"schemaAutoRegistrationEnabled": "true",
	}
	configMap := map[string]interface{}{
		common.DataFormatTypeKey:            common.DataFormatProtobuf,
		common.ProtobufMessageDescriptorKey: (&testpb.TestMessage{}).ProtoReflect().Descriptor(),
		common.GSRConfigPathKey:             gsrMap,
	}
	cfg := common.NewConfiguration(configMap)

	// Single shared serializer + deserializer across all goroutines.
	// This is the contract under test — if either instance were
	// per-goroutine the no-race / no-double-register guarantees
	// would be trivially satisfied.
	ser, err := serializer.NewSerializer(cfg)
	require.NoError(t, err, "NewSerializer")
	t.Cleanup(func() { _ = ser.Close() })

	des, err := deserializer.NewDeserializer(cfg)
	require.NoError(t, err, "NewDeserializer")
	t.Cleanup(func() { _ = des.Close() })

	const goroutines = 16
	const perGoroutine = 8

	// Three distinct schema names cycled across all 128 round-trips so
	// multiple goroutines first-encode the SAME schema concurrently,
	// forcing the singleflight + AlreadyExistsException recovery
	// paths into the hot loop. The "gsr-go-it-shared-" prefix matches
	// the leak-check pattern in integration-tests/REAL-AWS-RUNBOOK.md.
	// The DefaultSchemaNameStrategy uses topic-as-schema-name (see
	// pkg/gsrserde-go/core/schema_name_strategy.go) so the topic is
	// the schema name.
	schemaNames := []string{
		randomGlueName(t, "shared-a"),
		randomGlueName(t, "shared-b"),
		randomGlueName(t, "shared-c"),
	}
	// Spec §6 hard requirement: TrackSchema BEFORE any encode for that
	// name. Tracking all three up front (before launching goroutines)
	// guarantees the order regardless of which goroutine wins the
	// first-encode race.
	for _, n := range schemaNames {
		h.Cleanup.TrackSchema(testRegistryName, n)
	}

	// One Kafka topic per schema name. Created up front so the
	// goroutines don't race the broker on first produce.
	for _, n := range schemaNames {
		createSharedTopic(t, bootstrap, n)
	}
	t.Cleanup(func() {
		for _, n := range schemaNames {
			deleteSharedTopic(t, bootstrap, n)
		}
	})

	var totalEncoded atomic.Int64
	errs := make(chan error, goroutines*perGoroutine)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				schemaName := schemaNames[(g*perGoroutine+i)%len(schemaNames)]
				topic := schemaName // topic-as-schema-name strategy

				msg := &testpb.TestMessage{
					Id:    fmt.Sprintf("g%d-i%d-%d", g, i, time.Now().UnixNano()),
					Name:  fmt.Sprintf("g%d-i%d", g, i),
					Age:   int32(20 + g),
					Email: fmt.Sprintf("g%d@example.com", g),
					Tags:  []string{"multithreaded", "shared", "phase-4.11"},
				}

				encoded, err := ser.Serialize(topic, msg)
				if err != nil {
					errs <- fmt.Errorf("g%d i%d serialize: %w", g, i, err)
					return
				}

				consumed, err := sharedSaramaRoundTrip(bootstrap, topic, encoded)
				if err != nil {
					errs <- fmt.Errorf("g%d i%d sarama: %w", g, i, err)
					return
				}

				got, err := des.Deserialize(topic, consumed)
				if err != nil {
					errs <- fmt.Errorf("g%d i%d deserialize: %w", g, i, err)
					return
				}
				// Sanity check: deserialize returns a proto.Message we
				// can round-trip to the concrete TestMessage. A
				// type-assertion failure here means the deserializer
				// returned something unexpected under load.
				if _, ok := got.(proto.Message); !ok {
					errs <- fmt.Errorf("g%d i%d deserialize: got %T, want proto.Message", g, i, got)
					return
				}
				totalEncoded.Add(1)
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(goroutines*perGoroutine), totalEncoded.Load(),
		"every goroutine × message must round-trip successfully")

	// No-double-register: ListSchemaVersions for each unique schema
	// name must return exactly one version. The encoder's singleflight
	// guard (pkg/gsrserde-go/core/encoder.go) collapses the N
	// concurrent first-encodes onto a single CreateSchema; the
	// AlreadyExistsException recovery path catches the rare race
	// where a sibling Test or external producer already registered
	// the same name. Either way, exactly one version per name is the
	// contract.
	listCtx, listCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer listCancel()
	for _, name := range schemaNames {
		versions := listAllSchemaVersions(t, listCtx, h.Real.Client, testRegistryName, name)
		require.Lenf(t, versions, 1,
			"schema %q must have exactly one version after %d concurrent encodes; got %d (double-register?)",
			name, goroutines*perGoroutine, len(versions))
	}
}

// sharedSaramaRoundTrip produces one message and consumes one back from
// the given topic using Sarama, mirroring the transport pattern used by
// SaramaIntegrationSuite. Per-call producer/consumer construction is
// intentional: the test is exercising the SHARED *Serializer +
// *Deserializer, not a shared transport pipeline, and SyncProducer
// instances are themselves goroutine-safe but constructed cheap enough
// that pooling adds complexity without payoff at N=128.
func sharedSaramaRoundTrip(bootstrap, topic string, value []byte) ([]byte, error) {
	pcfg := sarama.NewConfig()
	pcfg.Producer.RequiredAcks = sarama.WaitForAll
	pcfg.Producer.Retry.Max = 3
	pcfg.Producer.Return.Successes = true
	pcfg.Producer.Idempotent = true
	pcfg.Net.MaxOpenRequests = 1
	producer, err := sarama.NewSyncProducer([]string{bootstrap}, pcfg)
	if err != nil {
		return nil, fmt.Errorf("NewSyncProducer: %w", err)
	}
	defer producer.Close()

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	})
	if err != nil {
		return nil, fmt.Errorf("SendMessage: %w", err)
	}

	ccfg := sarama.NewConfig()
	ccfg.Consumer.Return.Errors = true
	ccfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer([]string{bootstrap}, ccfg)
	if err != nil {
		return nil, fmt.Errorf("NewConsumer: %w", err)
	}
	defer consumer.Close()

	// Consume from the partition we produced to so the consumer
	// definitely sees our message (no rebalancing latency).
	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, fmt.Errorf("ConsumePartition: %w", err)
	}
	defer pc.Close()

	select {
	case msg := <-pc.Messages():
		return msg.Value, nil
	case err := <-pc.Errors():
		return nil, fmt.Errorf("partition consumer error: %w", err)
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timed out consuming from %s partition %d offset %d", topic, partition, offset)
	}
}

// createSharedTopic creates a single-partition Kafka topic up front so
// the concurrent goroutines don't race the broker on first produce.
// Existing-topic errors are silently swallowed — the topic might
// already exist from a prior run that left -no-cleanup.
func createSharedTopic(t *testing.T, bootstrap, topic string) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin([]string{bootstrap}, cfg)
	require.NoError(t, err, "NewClusterAdmin")
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		// Topic already exists is fine. Anything else is logged but
		// non-fatal — produce will surface the real error.
		t.Logf("createSharedTopic %q: %v (continuing — produce will surface real failures)", topic, err)
	}
}

// deleteSharedTopic is the inverse of createSharedTopic. Errors are
// logged, not failed — leftover topics don't break the test, and the
// broker is testcontainers-managed so it'll be torn down anyway.
func deleteSharedTopic(t *testing.T, bootstrap, topic string) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin([]string{bootstrap}, cfg)
	if err != nil {
		t.Logf("deleteSharedTopic NewClusterAdmin: %v (continuing)", err)
		return
	}
	defer admin.Close()
	if err := admin.DeleteTopic(topic); err != nil {
		t.Logf("deleteSharedTopic %q: %v (continuing — testcontainers will reap broker)", topic, err)
	}
}

// listAllSchemaVersions paginates ListSchemaVersions for (registry, name)
// and returns the full version list. Glue's API caps results per page
// at 100 by default; we ask for the maximum so a single page suffices
// for any realistic test (the no-double-register contract asserts
// len(versions) == 1).
func listAllSchemaVersions(t *testing.T, ctx context.Context, client *glue.Client, registry, name string) []types.SchemaVersionListItem {
	t.Helper()
	var versions []types.SchemaVersionListItem
	var nextToken *string
	for {
		out, err := client.ListSchemaVersions(ctx, &glue.ListSchemaVersionsInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(registry),
				SchemaName:   aws.String(name),
			},
			MaxResults: aws.Int32(100),
			NextToken:  nextToken,
		})
		require.NoError(t, err, "ListSchemaVersions(%s/%s)", registry, name)
		versions = append(versions, out.Schemas...)
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}
	return versions
}
