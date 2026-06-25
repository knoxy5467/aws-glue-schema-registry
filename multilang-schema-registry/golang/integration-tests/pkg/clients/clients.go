//go:build integration

// Package clients is the Phase 4 adapter layer between the integration
// suite and the supported Kafka client libraries (sarama, segmentio,
// confluent). The shape mirrors bufbuild/bsr-kafka-serde-go: a central
// SerDe core (provided by gsrserde-go/{serializer,deserializer}) plus
// thin per-client adapter packages, with NO unified Producer/Consumer
// abstraction beyond what the scenario harness needs.
//
// The Adapter interface is the only thing scenario code talks to. Each
// adapter implementation lives in its own subpackage so users of the
// library can take a dependency on (e.g.) the sarama adapter without
// pulling librdkafka into their build. The confluent adapter is gated
// behind `//go:build integration && confluent` because it requires CGO
// + librdkafka, which is not always available on dev hosts.
package clients

import (
	"context"
	"time"
)

// Adapter is the minimum Kafka client surface the integration suite
// needs: produce one message, then consume one message. Anything more
// (transactions, batches, headers) belongs in the suite itself, not
// here — the adapter exists to absorb client-library API differences
// so scenarios stay readable.
type Adapter interface {
	// Name identifies the underlying client library — used in t.Run
	// subtest names. Must be lowercase, no spaces.
	Name() string

	// Produce sends a single message with the given key/value to topic
	// and blocks until the broker acks. Implementations MUST surface
	// delivery errors as a returned error rather than logging them; the
	// scenario harness checks the error to decide whether to fail the
	// subtest.
	Produce(ctx context.Context, topic string, key, value []byte) error

	// Consume reads ONE message from topic and returns its value.
	// Implementations MUST respect ctx cancellation/timeout — the
	// harness uses a 30s ctx and an exceeded deadline fails the test.
	// Each call should be independent (e.g. fresh consumer group),
	// because scenarios use per-test topic names.
	Consume(ctx context.Context, topic string) ([]byte, error)

	// Close releases any underlying client resources. Idempotent.
	Close() error
}

// DefaultConsumeTimeout is the wall-clock budget a scenario gives an
// Adapter.Consume call before failing the test. Kept here so all
// adapters have a consistent default and so the value is overridable
// without grepping for hardcoded durations.
const DefaultConsumeTimeout = 30 * time.Second
