package platform

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rescueflow/rescueflow/internal/domain"
	"github.com/rescueflow/rescueflow/internal/events"
)

func TestOutboxPublishesAfterBrokerRecovers(t *testing.T) {
	s := NewStore()
	e := events.New(events.IncidentCreated, events.NewID(), "corr", "", "incident-service", map[string]string{"state": "created"})
	s.Outbox = append(s.Outbox, OutboxRecord{Event: e})
	available := false
	publish := func(events.Envelope) error {
		if !available {
			return errors.New("broker unavailable")
		}
		return nil
	}
	if got := s.DrainOutbox(publish); got != 0 || s.OutboxBacklog() != 1 {
		t.Fatal("outbox was lost during outage")
	}
	available = true
	if got := s.DrainOutbox(publish); got != 1 || s.OutboxBacklog() != 0 {
		t.Fatal("outbox did not publish after recovery")
	}
}

func TestCrashAfterSideEffectBeforeAcknowledgementIsSafe(t *testing.T) {
	b := NewBus(3)
	committed := map[string]bool{}
	mutations := 0
	b.Subscribe(events.IncidentCreated, "crashy-consumer", func(e events.Envelope) error {
		if !committed[e.EventID] {
			committed[e.EventID] = true
			mutations++
			return errors.New("simulated crash after commit")
		}
		return nil
	})
	e := events.New(events.IncidentCreated, events.NewID(), "corr", "", "test", map[string]string{"ok": "true"})
	if err := b.Publish(e); err != nil {
		t.Fatal(err)
	}
	if mutations != 1 {
		t.Fatalf("side effect applied %d times", mutations)
	}
}

func TestExpiredReservationIsReleased(t *testing.T) {
	s := NewStore()
	past := time.Now().Add(-time.Second)
	s.Resources["one"] = &domain.Resource{ID: "one", Kind: "ambulance", Available: false, ReservedFor: "incident", ReservationExpiresAt: &past, Version: 2}
	expired := s.ExpireReservations(time.Now())
	if len(expired) != 1 || !expired[0].Available || expired[0].ReservedFor != "" || expired[0].Version != 3 {
		t.Fatalf("reservation not safely expired: %+v", expired)
	}
}

func TestCompleteWorkflowAndIdempotency(t *testing.T) {
	w := NewWorkflow()
	in := CreateIncidentInput{Type: "medical", Severity: 4, Latitude: 47.61, Longitude: -122.33, Description: "synthetic scenario"}
	first, isNew, err := w.CreateIncident(in, "same-key", "corr-1")
	if err != nil || !isNew {
		t.Fatalf("create: %v", err)
	}
	got, ok := w.Store.GetIncident(first.ID)
	if !ok || got.Status != domain.Assigned {
		t.Fatalf("workflow ended at %s", got.Status)
	}
	second, isNew, err := w.CreateIncident(in, "same-key", "corr-2")
	if err != nil || isNew || second.ID != first.ID {
		t.Fatal("idempotency key created a duplicate")
	}
	w.Store.mu.RLock()
	defer w.Store.mu.RUnlock()
	if len(w.Store.Assignments) != 1 {
		t.Fatalf("got %d assignments", len(w.Store.Assignments))
	}
}

func TestDuplicateEventDoesNotDuplicateAssignment(t *testing.T) {
	w := NewWorkflow()
	now := time.Now().UTC()
	i := domain.Incident{ID: events.NewID(), Type: "fire", Severity: 3, Latitude: 47.6, Longitude: -122.3, Description: "synthetic", Status: domain.AssignmentPending, CreatedAt: now, UpdatedAt: now, Version: 3}
	w.Store.Incidents[i.ID] = &i
	e := events.New(events.AssignmentRequested, i.ID, "corr", "", "test", map[string]string{"incident_id": i.ID})
	if err := w.Bus.Publish(e); err != nil {
		t.Fatal(err)
	}
	if err := w.Bus.Publish(e); err != nil {
		t.Fatal(err)
	}
	w.Store.mu.RLock()
	defer w.Store.mu.RUnlock()
	if len(w.Store.Assignments) != 1 {
		t.Fatalf("duplicate event created %d assignments", len(w.Store.Assignments))
	}
}

func TestConcurrentReservationHasOneWinner(t *testing.T) {
	s := NewStore()
	s.Resources["one"] = &domain.Resource{ID: "one", Kind: "ambulance", Available: true, Version: 1}
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for _, incident := range []string{"a", "b"} {
		go func(id string) { defer wg.Done(); _, err := s.Reserve("one", id, 1, time.Minute); results <- err }(incident)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("want one winner, got %d", success)
	}
}

func TestNotificationRetriesReachDLQ(t *testing.T) {
	w := NewWorkflow()
	w.FailNotificationsFor = 10
	_, _, err := w.CreateIncident(CreateIncidentInput{Type: "medical", Severity: 2, Latitude: 47.6, Longitude: -122.3, Description: "failure injection"}, "", "corr")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Bus.DeadLetters()) != 1 {
		t.Fatalf("want one dead letter, got %d", len(w.Bus.DeadLetters()))
	}
	if len(w.Store.Attempts) != 3 {
		t.Fatalf("want three attempts, got %d", len(w.Store.Attempts))
	}
}

func TestResponderRejectionCompensatesAndReassigns(t *testing.T) {
	w := NewWorkflow()
	i, _, err := w.CreateIncident(CreateIncidentInput{Type: "rescue", Severity: 3, Latitude: 47.6, Longitude: -122.3, Description: "rejection scenario"}, "", "corr")
	if err != nil {
		t.Fatal(err)
	}
	w.Store.mu.RLock()
	old := w.Store.AssignmentByIncident[i.ID]
	w.Store.mu.RUnlock()
	if err := w.RejectAssignment(i.ID, "corr"); err != nil {
		t.Fatal(err)
	}
	w.Store.mu.RLock()
	next := w.Store.AssignmentByIncident[i.ID]
	w.Store.mu.RUnlock()
	if old == next || next == "" {
		t.Fatalf("assignment was not replaced: old=%s new=%s", old, next)
	}
}
