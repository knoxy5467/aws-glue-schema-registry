//go:build integration && confluent

package integration_tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients/confluent"
)

// confluent registration. Behind `//go:build integration && confluent`
// so the matrix only carries the confluent client when librdkafka is
// available; otherwise this file isn't compiled in and the matrix
// silently omits the confluent leg.
func init() {
	registerAdapter("confluent", func(t *testing.T, bootstrap string) clients.Adapter {
		t.Helper()
		a, err := confluent.New([]string{bootstrap})
		require.NoError(t, err)
		t.Cleanup(func() { _ = a.Close() })
		return a
	})
}
