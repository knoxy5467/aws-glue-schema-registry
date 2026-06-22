//go:build integration

package integration_tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients/sarama"
)

func init() {
	registerAdapter("sarama", func(t *testing.T, bootstrap string) clients.Adapter {
		t.Helper()
		a, err := sarama.New([]string{bootstrap})
		require.NoError(t, err)
		t.Cleanup(func() { _ = a.Close() })
		return a
	})
}
