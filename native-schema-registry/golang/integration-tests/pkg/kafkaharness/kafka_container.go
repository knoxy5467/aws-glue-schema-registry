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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var errNoBrokers = errors.New("kafkaharness: Brokers() returned no addresses")

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
// Prefer StartShared (called from TestMain) over Start (called from each
// SetupSuite). Start is kept for tests that genuinely need broker
// isolation, but the default Phase 4 + 5 flow uses a single
// package-level container — see TestMain in tests/main_test.go.
func Start(ctx context.Context, t testing.TB) *Broker {
	t.Helper()

	if override := os.Getenv("KAFKA_BROKER"); override != "" {
		t.Logf("kafkaharness: KAFKA_BROKER=%s — skipping testcontainers and using the supplied broker", override)
		return &Broker{Bootstrap: override}
	}

	broker, stop, err := startContainer(ctx)
	require.NoError(t, err, "kafkaharness: failed to start Kafka testcontainer")
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Logf("kafkaharness: container teardown returned error (non-fatal): %v", err)
		}
	})
	return broker
}

// StartShared is the TestMain-friendly variant. It does the same work
// as Start but returns a stop func instead of registering t.Cleanup,
// because TestMain doesn't have a testing.TB. KAFKA_BROKER short-
// circuit applies the same way.
//
// Typical usage from a package-level TestMain:
//
//	func TestMain(m *testing.M) {
//	    broker, stop, err := kafkaharness.StartShared(context.Background())
//	    if err != nil {
//	        log.Fatalf("kafkaharness: start: %v", err)
//	    }
//	    defer stop()
//	    if broker.Bootstrap != "" {
//	        os.Setenv("KAFKA_BROKER", broker.Bootstrap)
//	    }
//	    os.Exit(m.Run())
//	}
//
// Subsequent Start calls from SetupSuite see KAFKA_BROKER and reuse
// the same broker rather than spinning up a fresh container per
// suite.
func StartShared(ctx context.Context) (*Broker, func() error, error) {
	if override := os.Getenv("KAFKA_BROKER"); override != "" {
		return &Broker{Bootstrap: override}, func() error { return nil }, nil
	}
	return startContainer(ctx)
}

// startContainer is the testing.TB-free workhorse shared by Start and
// StartShared. Returns the broker and a stop closure that terminates
// the container with a 30s fresh-context timeout.
func startContainer(ctx context.Context) (*Broker, func() error, error) {
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	container, err := kafka.Run(startCtx,
		defaultKafkaImage,
		kafka.WithClusterID("phase4-integration-cluster"),
	)
	if err != nil {
		return nil, nil, err
	}

	brokers, err := container.Brokers(startCtx)
	if err != nil {
		return nil, nil, err
	}
	if len(brokers) == 0 {
		return nil, nil, errNoBrokers
	}

	stop := func() error {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return container.Terminate(stopCtx)
	}
	return &Broker{Bootstrap: brokers[0], Container: container}, stop, nil
}
