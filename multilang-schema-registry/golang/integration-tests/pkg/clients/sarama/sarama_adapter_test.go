//go:build integration

package sarama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNew_EmptyBrokers ensures the constructor refuses obviously bad
// input. The actual produce/consume paths require a live broker so are
// only exercised under AWS_INTEGRATION=1 + testcontainers; the
// constructor path runs everywhere.
func TestNew_EmptyBrokers(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)

	_, err = New([]string{})
	require.Error(t, err)
}

// TestNew_HappyPath confirms the adapter returns ok with a non-empty
// list and exposes the expected Name. Pinned so renames to the Name
// const are caught by `go test` rather than only by a live container
// run.
func TestNew_HappyPath(t *testing.T) {
	a, err := New([]string{"localhost:9092"})
	require.NoError(t, err)
	require.Equal(t, "sarama", a.Name())
	require.NoError(t, a.Close())
}
