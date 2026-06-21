//go:build integration && confluent

package integration_tests

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients/confluent"
)

// confluentClientName is added to the §5.3 matrix's client axis only
// when the `confluent` build tag is set. The default build (just
// //go:build integration) keeps the matrix at {sarama, segmentio} so
// the suite stays runnable on hosts without librdkafka. The
// matching constructor lives in newConfluentAdapter below; adapterFor
// in round_trip_test.go dispatches by name and looks up this entry.
const confluentClientName = "confluent"

func newConfluentAdapter(t *testing.T, bootstrap string) clients.Adapter {
	t.Helper()
	a, err := confluent.New([]string{bootstrap})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func init() {
	extraClientNames = append(extraClientNames, confluentClientName)
	extraAdapterCtors[confluentClientName] = newConfluentAdapter
}
