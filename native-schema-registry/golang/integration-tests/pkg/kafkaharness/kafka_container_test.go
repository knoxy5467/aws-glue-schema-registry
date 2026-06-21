//go:build integration

package kafkaharness

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStart_RespectsKAFKABROKEROverride exercises the no-container path:
// when KAFKA_BROKER is set, Start must return that address verbatim and
// must NOT touch testcontainers. This is the only Tier-1-style test we
// can run on Start without a docker daemon, and it's exactly the path the
// docker-compose fallback exercises in CI.
func TestStart_RespectsKAFKABROKEROverride(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "broker-from-env:19092")

	broker := Start(context.Background(), t)
	require.Equal(t, "broker-from-env:19092", broker.Bootstrap)
	require.Nil(t, broker.Container, "Container should be nil when override is used")
}
