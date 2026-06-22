//go:build integration && confluent

// Package confluent is the confluent-kafka-go adapter. Behind the
// extra `confluent` build tag because confluent-kafka-go requires CGO
// + a librdkafka shared library, which we don't want to force on every
// dev host. To enable: install librdkafka, then run
// `go test -tags 'integration confluent' ./...`.
//
// Java's KafkaProducer-based canary uses confluent-kafka-go's wire
// behavior in spirit (same librdkafka), so we keep the adapter here
// for parity with the §5.2 axis labeled "Kafka client".
package confluent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"

	"github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/pkg/clients"
)

// Adapter is the confluent-kafka-go-backed clients.Adapter.
type Adapter struct {
	brokers []string
}

// New constructs an Adapter against the given Kafka bootstrap list.
func New(brokers []string) (*Adapter, error) {
	if len(brokers) == 0 {
		return nil, errors.New("confluent adapter: no brokers supplied")
	}
	return &Adapter{brokers: brokers}, nil
}

// Name implements clients.Adapter.
func (a *Adapter) Name() string { return "confluent" }

// Produce sends one message and blocks on its delivery report.
func (a *Adapter) Produce(ctx context.Context, topic string, key, value []byte) error {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":                     a.brokers[0],
		"acks":                                  "all",
		"retries":                               3,
		"max.in.flight.requests.per.connection": 1,
		"enable.idempotence":                    true,
	}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("confluent: new producer: %w", err)
	}
	defer producer.Close()

	topicCopy := topic
	// Do NOT close(deliveryChan) explicitly. The deferred producer.Close()
	// runs a synchronous flush that can invoke librdkafka's delivery-report
	// callback after this function has returned (e.g. on ctx-cancel and
	// time.After branches below). Closing the channel before the callback
	// fires would panic 'send on closed channel'. Letting GC reclaim the
	// channel after the goroutine that produced it goes away is the safe
	// idiom; the buffered channel ensures the callback never blocks.
	deliveryChan := make(chan kafka.Event, 1)

	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicCopy, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, deliveryChan); err != nil {
		return fmt.Errorf("confluent: produce: %w", err)
	}

	// ctx is the sole authority for the delivery wait. If the caller
	// didn't set a deadline, apply DefaultConsumeTimeout so a wedged
	// broker can't block forever; the previous min(30s, ctx) cap
	// silently shrank a caller's longer ctx and is removed.
	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clients.DefaultConsumeTimeout)
		defer cancel()
	}

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*kafka.Message)
		if !ok {
			return fmt.Errorf("confluent: unexpected delivery event %T", ev)
		}
		if msg.TopicPartition.Error != nil {
			return fmt.Errorf("confluent: delivery: %w", msg.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("confluent: produce cancelled: %w", ctx.Err())
	}
}

// Consume subscribes and reads one message.
func (a *Adapter) Consume(ctx context.Context, topic string) ([]byte, error) {
	// uuid for the group ID — UnixNano collides on coarse-clock
	// platforms under t.Parallel, causing one reader to hang.
	cfg := &kafka.ConfigMap{
		"bootstrap.servers": a.brokers[0],
		"group.id":          "confluent-adapter-" + uuid.NewString(),
		"auto.offset.reset": "earliest",
	}
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("confluent: new consumer: %w", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, fmt.Errorf("confluent: subscribe: %w", err)
	}

	if _, deadlineSet := ctx.Deadline(); !deadlineSet {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clients.DefaultConsumeTimeout)
		defer cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("confluent: consume cancelled: %w", ctx.Err())
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if kErr, ok := err.(kafka.Error); ok && kErr.Code() == kafka.ErrTimedOut {
					continue
				}
				return nil, fmt.Errorf("confluent: read: %w", err)
			}
			return msg.Value, nil
		}
	}
}

// Close is a no-op; per-call clients release themselves via defer.
func (a *Adapter) Close() error { return nil }

// Compile-time assertion that *Adapter satisfies clients.Adapter.
var _ clients.Adapter = (*Adapter)(nil)
