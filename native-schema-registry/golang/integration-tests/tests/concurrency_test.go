//go:build integration

package integration_tests

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/fakeglue"
	gsrcore "github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/pkg/gsrserde-go/core"
)

// Concurrency scenarios from plan §5.3 items 30-31.
//
// Item 30 (multithreaded produce/consume with shared serializer/
// deserializer) is already covered end-to-end by the legacy
// multithreaded_integration_suite_test.go against real Kafka + real
// Glue. The scenario below is the corresponding §5.3-row regression
// at the core/ level: it asserts that the same shared encoder/
// decoder instance survives N parallel goroutines without races or
// stale-cache surprises.
//
// Item 31 (singleflight: N concurrent first-encodes of the same
// schema call CreateSchema once) is the more interesting assertion.
// The core/ singleflight test already locks down the encoder side;
// this version pins the orchestrator-level invariant through fakeglue.

// §5.3 item 30 — multithreaded produce/consume with shared serializer
// /deserializer instances.
func TestConcurrency_SharedInstancesDoNotRace(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(f, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	const goroutines = 16
	const perGoroutine = 8

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				schemaName := "shared-" + scenarioRegistrySuffix()
				schema := &gsrcore.Schema{
					SchemaDefinition: avroSchemaLifecycle,
					SchemaName:       schemaName,
					DataFormat:       "AVRO",
				}
				encoded, err := enc.Encode([]byte("payload"), schemaName, schema)
				if err != nil {
					errs <- err
					return
				}
				if _, err := dec.Decode(encoded); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// §5.3 item 31 — concurrent first-encode of the same schema only
// calls CreateSchema once. The encoder's singleflight wrapping
// guarantees this; we pin the orchestrator-level invariant by
// running N goroutines against the same schemaName + definition and
// asserting CreateSchema's count is 1.
func TestConcurrency_SingleflightFirstEncode(t *testing.T) {
	t.Parallel()
	f := fakeglue.New()
	enc, err := gsrcore.NewGsrEncoderForTest(f, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	const goroutines = 32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			_, _ = enc.Encode([]byte("payload"), "sf-31", &gsrcore.Schema{
				SchemaDefinition: avroSchemaLifecycle,
				SchemaName:       "sf-31",
				DataFormat:       "AVRO",
			})
		}()
	}
	wg.Wait()

	require.Equal(t, 1, f.CallCounts["CreateSchema"],
		"N concurrent first-encodes of the same schema must collapse to one CreateSchema call (singleflight)")
}
