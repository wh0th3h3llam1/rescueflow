package platform

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/rescueflow/rescueflow/internal/domain"
	"github.com/rescueflow/rescueflow/internal/events"
)

type OutboxRecord struct {
	Event       events.Envelope `json:"event"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// Store is a concurrency-safe local adapter. Its boundaries mirror the independently
// owned schemas used by the PostgreSQL migrations.
type Store struct {
	mu                   sync.RWMutex
	Incidents            map[string]*domain.Incident
	History              map[string][]domain.StatusHistory
	Idempotency          map[string]string
	Responders           map[string]*domain.Responder
	Assignments          map[string]*domain.Assignment
	AssignmentByIncident map[string]string
	Resources            map[string]*domain.Resource
	Attempts             []domain.NotificationAttempt
	Outbox               []OutboxRecord
	Events               map[string][]events.Envelope
}

func NewStore() *Store {
	return &Store{Incidents: map[string]*domain.Incident{}, History: map[string][]domain.StatusHistory{}, Idempotency: map[string]string{}, Responders: map[string]*domain.Responder{}, Assignments: map[string]*domain.Assignment{}, AssignmentByIncident: map[string]string{}, Resources: map[string]*domain.Resource{}, Events: map[string][]events.Envelope{}}
}

func cloneIncident(i *domain.Incident) domain.Incident { return *i }

func (s *Store) CreateIncident(i domain.Incident, key string, event events.Envelope) (domain.Incident, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != "" {
		if id := s.Idempotency[key]; id != "" {
			return cloneIncident(s.Incidents[id]), false
		}
	}
	s.Incidents[i.ID] = &i
	s.History[i.ID] = append(s.History[i.ID], domain.StatusHistory{IncidentID: i.ID, To: i.Status, Reason: "incident created", OccurredAt: i.CreatedAt})
	s.Outbox = append(s.Outbox, OutboxRecord{Event: event})
	if key != "" {
		s.Idempotency[key] = i.ID
	}
	return cloneIncident(&i), true
}

func (s *Store) Transition(id string, to domain.IncidentStatus, reason string, event events.Envelope) (domain.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.Incidents[id]
	if i == nil {
		return domain.Incident{}, errors.New("incident not found")
	}
	from := i.Status
	if err := i.Transition(to); err != nil {
		return domain.Incident{}, err
	}
	if from != to {
		s.History[id] = append(s.History[id], domain.StatusHistory{IncidentID: id, From: from, To: to, Reason: reason, OccurredAt: i.UpdatedAt})
	}
	if event.EventID != "" {
		s.Outbox = append(s.Outbox, OutboxRecord{Event: event})
	}
	return cloneIncident(i), nil
}

func (s *Store) GetIncident(id string) (domain.Incident, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	i := s.Incidents[id]
	if i == nil {
		return domain.Incident{}, false
	}
	return cloneIncident(i), true
}

func (s *Store) ListIncidents(status domain.IncidentStatus, severity, offset, limit int) []domain.Incident {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]domain.Incident, 0, len(s.Incidents))
	for _, i := range s.Incidents {
		if status != "" && i.Status != status {
			continue
		}
		if severity > 0 && i.Severity != severity {
			continue
		}
		all = append(all, cloneIncident(i))
	}
	sort.Slice(all, func(a, b int) bool { return all[a].CreatedAt.After(all[b].CreatedAt) })
	if offset >= len(all) {
		return []domain.Incident{}
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end]
}

func (s *Store) Timeline(id string) []domain.StatusHistory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]domain.StatusHistory(nil), s.History[id]...)
}

func (s *Store) RecordEvent(e events.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events[e.AggregateID] = append(s.Events[e.AggregateID], e)
}
func (s *Store) IncidentEvents(id string) []events.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]events.Envelope(nil), s.Events[id]...)
}

func (s *Store) DrainOutbox(publish func(events.Envelope) error) int {
	published := 0
	for idx := 0; ; idx++ {
		s.mu.RLock()
		if idx >= len(s.Outbox) {
			s.mu.RUnlock()
			break
		}
		record := s.Outbox[idx]
		s.mu.RUnlock()
		if record.PublishedAt != nil {
			continue
		}
		if err := publish(record.Event); err != nil {
			continue
		}
		now := time.Now().UTC()
		s.mu.Lock()
		if s.Outbox[idx].PublishedAt == nil {
			s.Outbox[idx].PublishedAt = &now
			published++
		}
		s.mu.Unlock()
	}
	return published
}

func (s *Store) OutboxBacklog() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, r := range s.Outbox {
		if r.PublishedAt == nil {
			n++
		}
	}
	return n
}

func (s *Store) Reserve(resourceID, incidentID string, expectedVersion int64, ttl time.Duration) (domain.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.Resources[resourceID]
	if r == nil {
		return domain.Resource{}, errors.New("resource not found")
	}
	if r.Version != expectedVersion {
		return domain.Resource{}, errors.New("optimistic concurrency conflict")
	}
	if !r.Available || r.ReservedFor != "" {
		return domain.Resource{}, errors.New("resource unavailable")
	}
	expires := time.Now().UTC().Add(ttl)
	r.Available = false
	r.ReservedFor = incidentID
	r.ReservationExpiresAt = &expires
	r.Version++
	return *r, nil
}

func (s *Store) Release(resourceID, incidentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.Resources[resourceID]
	if r == nil {
		return errors.New("resource not found")
	}
	if incidentID != "" && r.ReservedFor != incidentID {
		return errors.New("resource is reserved for another incident")
	}
	r.Available = true
	r.ReservedFor = ""
	r.ReservationExpiresAt = nil
	r.Version++
	return nil
}

func (s *Store) ExpireReservations(now time.Time) []domain.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Resource
	for _, r := range s.Resources {
		if r.ReservationExpiresAt != nil && !r.ReservationExpiresAt.After(now) {
			r.Available = true
			r.ReservedFor = ""
			r.ReservationExpiresAt = nil
			r.Version++
			out = append(out, *r)
		}
	}
	return out
}
