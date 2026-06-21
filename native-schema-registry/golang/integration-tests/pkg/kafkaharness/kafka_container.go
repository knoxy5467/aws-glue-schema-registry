//go:build integration

// Package kafkaharness owns the Kafka container lifecycle for the Phase 4
// integration suite. It replaces the docker-compose path that Phase 0
// shipped — testcontainers-go drives container startup directly from the
// test process, which removes the "docker compose up && wait for ready"
// out-of-band step and lets each suite get its own broker if it wants
// hermeticity.
//
// docker-compose.yml is intentionally kept under integration-tests/ as a
// fallback for environments where the test process cannot speak to
// /var/run/docker.sock (some CI runners). The KAFKA_BROKER env var still
// wins over the testcontainers-spawned broker so docker-compose-based
// runs keep working unchanged.
package kafkaharness

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Default Confluent KRaft image used by testcontainers-go's Kafka module.
// Pinned so test runs stay reproducible across machines.
const defaultKafkaImage = "confluentinc/confluent-local:7.6.1"

// Broker bundles the listener addresses a test needs to connect.
//
// Bootstrap is the host-side address (used by sarama, segmentio, confluent
// clients running in the same process as the test). The testcontainers-go
// Kafka module also exposes container-network listeners but Phase 4 tests
// always run in-process, so we only surface the host listener.
type Broker struct {
	Bootstrap string
	Container *kafka.KafkaContainer
}

// Start brings up a Kafka KRaft container and returns a Broker. The
// container is torn down via t.Cleanup so callers don't have to think
// about lifecycle. If KAFKA_BROKER is set, Start short-circuits and
// returns that address without launching a container — this keeps the
// docker-compose path usable for CI environments that pre-provision Kafka.
//
// Start is safe to call from SetupSuite or from a single TestMain. Don't
// call it per-subtest unless the test genuinely needs broker isolation —
// each container takes ~5-10s to come up.
func Start(ctx context.Context, t testing.TB) *Broker {
	t.Helper()

	if override := os.Getenv("KAFKA_BROKER"); override != "" {
		t.Logf("kafkaharness: KAFKA_BROKER=%s — skipping testcontainers and using the supplied broker", override)
		return &Broker{Bootstrap: override}
	}

	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	container, err := kafka.Run(startCtx,
		defaultKafkaImage,
		kafka.WithClusterID("phase4-integration-cluster"),
	)
	require.NoError(t, err, "kafkaharness: failed to start Kafka testcontainer")

	brokers, err := container.Brokers(startCtx)
	require.NoError(t, err, "kafkaharness: failed to read broker addresses")
	require.NotEmpty(t, brokers, "kafkaharness: Brokers() returned no addresses")

	t.Cleanup(func() {
		// Use a fresh context — the parent context may already be cancelled
		// by the time Cleanup runs.
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(stopCtx); err != nil {
			t.Logf("kafkaharness: container teardown returned error (non-fatal): %v", err)
		}
	})

	return &Broker{Bootstrap: brokers[0], Container: container}
}
