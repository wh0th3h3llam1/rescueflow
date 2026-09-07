package platform

import (
	"fmt"
	"sync"
	"time"

	"github.com/rescueflow/rescueflow/internal/events"
)

type Handler func(events.Envelope) error

type DeadLetter struct {
	Original       events.Envelope `json:"original_event"`
	FailureReason  string          `json:"failure_reason"`
	Consumer       string          `json:"consumer"`
	AttemptCount   int             `json:"attempt_count"`
	FirstFailureAt time.Time       `json:"first_failure_at"`
	LastFailureAt  time.Time       `json:"most_recent_failure_at"`
}

type subscription struct {
	name    string
	handler Handler
}

// Bus is the local deterministic adapter for the event broker. The Kafka adapter uses
// the same envelope and handler contract in deployed environments.
type Bus struct {
	mu          sync.Mutex
	subs        map[string][]subscription
	processed   map[string]map[string]bool
	dlq         []DeadLetter
	maxAttempts int
	observers   []func(events.Envelope)
}

func NewBus(maxAttempts int) *Bus {
	if maxAttempts < 1 {
		maxAttempts = 3
	}
	return &Bus{subs: map[string][]subscription{}, processed: map[string]map[string]bool{}, maxAttempts: maxAttempts}
}

func (b *Bus) Subscribe(topic, consumer string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], subscription{name: consumer, handler: handler})
	if b.processed[consumer] == nil {
		b.processed[consumer] = map[string]bool{}
	}
}

func (b *Bus) Observe(fn func(events.Envelope)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.observers = append(b.observers, fn)
}

func (b *Bus) Publish(e events.Envelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	subs := append([]subscription(nil), b.subs[e.EventType]...)
	observers := append([]func(events.Envelope){}, b.observers...)
	b.mu.Unlock()
	for _, observe := range observers {
		observe(e)
	}
	for _, sub := range subs {
		b.mu.Lock()
		done := b.processed[sub.name][e.EventID]
		b.mu.Unlock()
		if done {
			continue
		}
		first := time.Time{}
		var err error
		for attempt := 1; attempt <= b.maxAttempts; attempt++ {
			err = sub.handler(e)
			if err == nil {
				b.mu.Lock()
				b.processed[sub.name][e.EventID] = true
				b.mu.Unlock()
				break
			}
			if first.IsZero() {
				first = time.Now().UTC()
			}
			if attempt < b.maxAttempts {
				time.Sleep(time.Duration(1<<(attempt-1)) * time.Millisecond)
			}
		}
		if err != nil {
			now := time.Now().UTC()
			b.mu.Lock()
			b.dlq = append(b.dlq, DeadLetter{Original: e, FailureReason: err.Error(), Consumer: sub.name, AttemptCount: b.maxAttempts, FirstFailureAt: first, LastFailureAt: now})
			// A durable broker consumer commits after the DLQ publish succeeds. Marking
			// this delivery handled models that atomic hand-off and prevents DLQ storms.
			b.processed[sub.name][e.EventID] = true
			b.mu.Unlock()
			return fmt.Errorf("consumer %s exhausted retries: %w", sub.name, err)
		}
	}
	return nil
}

func (b *Bus) DeadLetters() []DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]DeadLetter(nil), b.dlq...)
}

func (b *Bus) Replay(index int) error {
	b.mu.Lock()
	if index < 0 || index >= len(b.dlq) {
		b.mu.Unlock()
		return fmt.Errorf("dead letter not found")
	}
	letter := b.dlq[index]
	e := letter.Original
	delete(b.processed[letter.Consumer], e.EventID)
	b.dlq = append(b.dlq[:index], b.dlq[index+1:]...)
	b.mu.Unlock()
	return b.Publish(e)
}
