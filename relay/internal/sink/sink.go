// Package sink is where a relay's batches go. Pubsub publishes for real. Simulated
// stands in for Pub/Sub with a fixed cost per batch, so a benchmark measures the
// database and relay path with a realistic publish round trip instead of a broker.
package sink

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Sink interface {
	// PublishAll publishes every message and returns once each one is acknowledged.
	PublishAll(ctx context.Context, msgs []*pubsub.Message) error
	Close()
}

type Config struct {
	Kind      string        // simulated | pubsub
	Latency   time.Duration // simulated: cost of one publish batch
	Project   string
	Topic     string
	BatchSize int
}

func New(ctx context.Context, cfg Config) (Sink, error) {
	switch cfg.Kind {
	case "simulated":
		log.Printf("sink=simulated: %s per publish batch, nothing is sent", cfg.Latency)
		return simulated{latency: cfg.Latency}, nil
	case "pubsub":
		ps, err := pubsub.NewClient(ctx, cfg.Project)
		if err != nil {
			return nil, fmt.Errorf("pubsub client: %w", err)
		}
		t := ps.Topic(cfg.Topic)
		ok, err := t.Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("topic exists: %w", err)
		}
		if !ok {
			created, err := ps.CreateTopic(ctx, cfg.Topic)
			switch {
			case err == nil:
				t = created
				log.Printf("created topic %s", cfg.Topic)
			case status.Code(err) == codes.AlreadyExists:
				// another relay created it between Exists and CreateTopic
			default:
				return nil, fmt.Errorf("create topic: %w", err)
			}
		}
		t.EnableMessageOrdering = true
		t.PublishSettings.CountThreshold = cfg.BatchSize
		t.PublishSettings.DelayThreshold = 50 * time.Millisecond
		return &pubsubSink{client: ps, topic: t}, nil
	}
	return nil, fmt.Errorf("unknown sink %q (pubsub | simulated)", cfg.Kind)
}

type pubsubSink struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

func (s *pubsubSink) PublishAll(ctx context.Context, msgs []*pubsub.Message) error {
	results := make([]*pubsub.PublishResult, len(msgs))
	for i, m := range msgs {
		results[i] = s.topic.Publish(ctx, m)
	}
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			// With message ordering on, a failed publish pauses the key until the
			// publisher is told to resume; the batch is retried as a whole, so resume now.
			if k := msgs[i].OrderingKey; k != "" {
				s.topic.ResumePublish(k)
			}
			return err
		}
	}
	return nil
}

func (s *pubsubSink) Close() { s.topic.Stop(); s.client.Close() }

type simulated struct{ latency time.Duration }

func (s simulated) PublishAll(ctx context.Context, msgs []*pubsub.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	select {
	case <-time.After(s.latency):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (simulated) Close() {}
