//go:build integration

// Package segmentio is the segmentio/kafka-go adapter for the Phase 4
// scenario harness. Pure-Go like sarama; this adapter is kept around
// because the legacy suite was built on segmentio and ripping it out
// before validating sarama parity would break the existing tests.
package segmentio

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
)

// Adapter is the segmentio-backed clients.Adapter.
type Adapter struct {
	brokers []string
}

// New constructs an Adapter against the given Kafka bootstrap list.
func New(brokers []string) (*Adapter, error) {
	if len(brokers) == 0 {
		return nil, errors.New("segmentio adapter: no brokers supplied")
	}
	return &Adapter{brokers: brokers}, nil
}

// Name implements clients.Adapter.
func (a *Adapter) Name() string { return "segmentio" }

// Produce uses a fresh kafka.Writer per call — the Writer is cheap and
// keeping it scoped avoids state leaking across topics.
func (a *Adapter) Produce(ctx context.Context, topic string, key, value []byte) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(a.brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	return w.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

// Consume reads one message via a Reader with a per-call random group
// so consume re-runs against the same topic don't share state.
func (a *Adapter) Consume(ctx context.Context, topic string) ([]byte, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: a.brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("segmentio-adapter-%d", time.Now().UnixNano()),
	})
	defer r.Close()

	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clients.DefaultConsumeTimeout)
		defer cancel()
	}

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("segmentio: read: %w", err)
	}
	return msg.Value, nil
}

// Close is a no-op; per-call Writer/Reader release themselves via defer.
func (a *Adapter) Close() error { return nil }

// Compile-time assertion that *Adapter satisfies clients.Adapter.
var _ clients.Adapter = (*Adapter)(nil)
