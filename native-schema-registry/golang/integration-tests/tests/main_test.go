//go:build integration

package integration_tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/kafkaharness"
)

// TestMain owns the package-level Kafka container so all suites and the
// §5.3 matrix share ONE broker per `go test` invocation. Before this,
// BaseIntegrationSuite (used by 5 suites), MultiThreadedIntegrationSuite,
// and TestRoundTrip_Phase4Matrix each called kafkaharness.Start, spinning
// up 7 separate testcontainers Kafka clusters at ~5-10s each.
//
// The Start path in each SetupSuite still works unchanged: kafkaharness
// honors KAFKA_BROKER as a short-circuit, and we set that env var here
// to the shared bootstrap so per-suite Start calls return without
// launching a fresh container.
//
// Skip the shared startup when no integration test will actually run
// (AWS_INTEGRATION!=1 AND no KAFKA_BROKER pre-set): the wire-format
// and Tier-1-style integration tests don't need Kafka, so launching a
// container in that case is pure waste.
func TestMain(m *testing.M) {
	if os.Getenv("KAFKA_BROKER") == "" && os.Getenv("AWS_INTEGRATION") == "1" {
		broker, stop, err := kafkaharness.StartShared(context.Background())
		if err != nil {
			log.Fatalf("integration TestMain: kafkaharness.StartShared: %v", err)
		}
		defer func() {
			if err := stop(); err != nil {
				// Stderr only; log.Fatalf would mask m.Run's exit code.
				fmt.Fprintf(os.Stderr, "integration TestMain: container teardown (non-fatal): %v\n", err)
			}
		}()
		if broker != nil && broker.Bootstrap != "" {
			_ = os.Setenv("KAFKA_BROKER", broker.Bootstrap)
		}
	}
	os.Exit(m.Run())
}
