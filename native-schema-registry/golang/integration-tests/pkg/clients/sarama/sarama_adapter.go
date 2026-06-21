//go:build integration

// Package sarama is the IBM/sarama adapter for the Phase 4 scenario
// harness. Pure-Go, no CGO, no librdkafka — this is the "always works"
// path that every scenario can rely on.
package sarama

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
)

// Adapter is the sarama-backed clients.Adapter. Carries the broker
// addresses through both producer and consumer paths so each Produce/
// Consume call creates a fresh sarama client — sarama clients are not
// cheap to reuse across topics, and the harness uses per-scenario
// topics anyway.
type Adapter struct {
	brokers []string
}

// New constructs an Adapter against the given Kafka bootstrap list.
// Returns an error only when brokers is empty — sarama itself will
// surface broker-reachability errors lazily on the first call.
func New(brokers []string) (*Adapter, error) {
	if len(brokers) == 0 {
		return nil, errors.New("sarama adapter: no brokers supplied")
	}
	return &Adapter{brokers: brokers}, nil
}

// Name implements clients.Adapter.
func (a *Adapter) Name() string { return "sarama" }

// Produce sends a single message synchronously. Uses idempotent
// producer config so retries don't cause duplicates — the harness
// asserts byte-identical round-trip and duplicates would defeat that.
func (a *Adapter) Produce(ctx context.Context, topic string, key, value []byte) error {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(a.brokers, cfg)
	if err != nil {
		return fmt.Errorf("sarama: new producer: %w", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Honor ctx via a goroutine — sarama's SyncProducer doesn't accept
	// a context directly.
	done := make(chan error, 1)
	go func() {
		_, _, err := producer.SendMessage(msg)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("sarama: send: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("sarama: produce cancelled: %w", ctx.Err())
	}
}

// Consume reads one message from topic and returns the value. Uses a
// partition consumer at OffsetOldest so the message published by
// Produce above always shows up regardless of test ordering.
func (a *Adapter) Consume(ctx context.Context, topic string) ([]byte, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(a.brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("sarama: new consumer: %w", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("sarama: list partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("sarama: topic %q has no partitions", topic)
	}

	pc, err := consumer.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("sarama: consume partition: %w", err)
	}
	defer pc.Close()

	// Apply DefaultConsumeTimeout if the caller hasn't set their own.
	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clients.DefaultConsumeTimeout)
		defer cancel()
	}
	// Hard cap as defense-in-depth — if ctx somehow has no deadline
	// (shouldn't happen given the block above), still bound the wait.
	timer := time.NewTimer(clients.DefaultConsumeTimeout)
	defer timer.Stop()

	select {
	case msg := <-pc.Messages():
		return msg.Value, nil
	case err := <-pc.Errors():
		return nil, fmt.Errorf("sarama: consumer error: %w", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("sarama: consume cancelled: %w", ctx.Err())
	case <-timer.C:
		return nil, fmt.Errorf("sarama: consume timed out after %s", clients.DefaultConsumeTimeout)
	}
}

// Close is a no-op; per-call clients release themselves via defer.
func (a *Adapter) Close() error { return nil }

// Compile-time assertion that *Adapter satisfies clients.Adapter.
var _ clients.Adapter = (*Adapter)(nil)
