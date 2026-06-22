//go:build integration

package integration_tests

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

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
// /deserializer instances. Backend-agnostic on the race/cache property;
// the CallCounts assertion at the end is fake-only and lives under a
// nil-guard on h.Fake.
func TestConcurrency_SharedInstancesDoNotRace(t *testing.T) {
	t.Parallel()
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)
	dec, err := gsrcore.NewGsrDecoderForTest(h.Client, gsrcore.GsrDecoderOptions{
		RegistryName: "default-registry",
	})
	require.NoError(t, err)

	const goroutines = 16
	const perGoroutine = 8

	// Rotate from a small pool of schema names so multiple goroutines
	// hit the SAME cache key concurrently. An earlier draft generated
	// a unique scenarioRegistrySuffix() per call, which produced 128
	// distinct cache keys — the cache fast-path under concurrent
	// readers was never exercised. The pool size (4) is comfortably
	// smaller than goroutines*perGoroutine so the cache-hit branch
	// runs many times across the test.
	pool := []string{
		randomGlueName(t, "shared-pool-a"),
		randomGlueName(t, "shared-pool-b"),
		randomGlueName(t, "shared-pool-c"),
		randomGlueName(t, "shared-pool-d"),
	}
	if h.Cleanup != nil {
		for _, name := range pool {
			h.Cleanup.TrackSchema("default-registry", name)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				schemaName := pool[(g*perGoroutine+i)%len(pool)]
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

	// With 4 pool entries and singleflight, CreateSchema should fire
	// at most once per pool entry — proves the cache fast-path is
	// actually exercised under concurrency. Real Glue has no
	// equivalent introspection so we only assert this on fake.
	if h.Fake != nil {
		require.LessOrEqual(t, h.Fake.Count("CreateSchema"), len(pool),
			"singleflight + cache must collapse concurrent first-encodes per pool entry; saw %d CreateSchema calls for %d distinct names",
			h.Fake.Count("CreateSchema"), len(pool))
	}
}

// §5.3 item 31 — concurrent first-encode of the same schema only
// calls CreateSchema once. The encoder's singleflight wrapping
// guarantees this; we pin the orchestrator-level invariant by
// running N goroutines against the same schemaName + definition and
// asserting CreateSchema's count is 1.
//
// Phase 4.7: requiresFake=true. The "exactly one CreateSchema"
// assertion is fake-only.
func TestConcurrency_SingleflightFirstEncode(t *testing.T) {
	t.Parallel()
	scenarioGate(t, false, true)
	h := newGlueHandle(t)
	enc, err := gsrcore.NewGsrEncoderForTest(h.Client, gsrcore.GsrEncoderOptions{
		RegistryName:                  "default-registry",
		SchemaAutoRegistrationEnabled: true,
	})
	require.NoError(t, err)

	const goroutines = 32
	schemaName := randomGlueName(t, "sf-31")

	// Collect per-goroutine errors so a regression that makes the
	// cached-path branch nil-deref (or any other latent encoder bug)
	// surfaces alongside the CreateSchema count. An earlier draft
	// discarded the error with `_, _ = enc.Encode(...)`; the review
	// flagged that pattern as "all 32 goroutines could be erroring
	// and the test would still pass as long as CreateSchema fires
	// exactly once".
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			_, err := enc.Encode([]byte("payload"), schemaName, &gsrcore.Schema{
				SchemaDefinition: avroSchemaLifecycle,
				SchemaName:       schemaName,
				DataFormat:       "AVRO",
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "singleflight encoder must not error on the cached-path branch")
	}

	require.Equal(t, 1, h.Fake.CallCounts["CreateSchema"],
		"N concurrent first-encodes of the same schema must collapse to one CreateSchema call (singleflight)")
}
