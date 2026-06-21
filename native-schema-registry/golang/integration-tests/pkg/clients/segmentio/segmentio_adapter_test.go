//go:build integration

package segmentio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_EmptyBrokers(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)

	_, err = New([]string{})
	require.Error(t, err)
}

func TestNew_HappyPath(t *testing.T) {
	a, err := New([]string{"localhost:9092"})
	require.NoError(t, err)
	require.Equal(t, "segmentio", a.Name())
	require.NoError(t, a.Close())
}
