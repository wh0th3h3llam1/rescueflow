package platform

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rescueflow/rescueflow/internal/domain"
	"github.com/rescueflow/rescueflow/internal/events"
)

type CreateIncidentInput struct {
	Type        string  `json:"incident_type"`
	Severity    int     `json:"severity"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	Description string  `json:"description"`
}

type Metrics struct {
	HTTPRequests, HTTPErrors, Events, Retries, ReservationConflicts atomic.Int64
	HTTPDurationNanos                                               atomic.Int64
}

type Workflow struct {
	Store                *Store
	Bus                  *Bus
	Metrics              *Metrics
	failureMu            sync.Mutex
	notificationFailures map[string]int
	FailNotificationsFor int
	Hub                  *Hub
}

func NewWorkflow() *Workflow {
	s := NewStore()
	b := NewBus(3)
	w := &Workflow{Store: s, Bus: b, Metrics: &Metrics{}, notificationFailures: map[string]int{}, Hub: NewHub()}
	w.seed()
	b.Observe(func(e events.Envelope) {
		s.RecordEvent(e)
		w.Metrics.Events.Add(1)
		w.Hub.Broadcast(e)
	})
	w.register()
	return w
}

func (w *Workflow) seed() {
	w.Store.Responders["rsp-1"] = &domain.Responder{ID: "rsp-1", Name: "Morgan Lee", Capabilities: []string{"medical", "fire"}, Available: true, Latitude: 47.6101, Longitude: -122.2015}
	w.Store.Responders["rsp-2"] = &domain.Responder{ID: "rsp-2", Name: "Sam Rivera", Capabilities: []string{"fire", "rescue"}, Available: true, Latitude: 47.6205, Longitude: -122.3493}
	w.Store.Responders["rsp-3"] = &domain.Responder{ID: "rsp-3", Name: "Alex Chen", Capabilities: []string{"medical", "rescue"}, Available: true, Latitude: 47.5952, Longitude: -122.3316}
	w.Store.Resources["res-ambulance-1"] = &domain.Resource{ID: "res-ambulance-1", Kind: "ambulance", Available: true, Version: 1}
	w.Store.Resources["res-engine-1"] = &domain.Resource{ID: "res-engine-1", Kind: "fire_engine", Available: true, Version: 1}
	w.Store.Resources["res-kit-1"] = &domain.Resource{ID: "res-kit-1", Kind: "rescue_kit", Available: true, Version: 1}
}

func (w *Workflow) register() {
	w.Bus.Subscribe(events.IncidentCreated, "incident-validator", w.onIncidentCreated)
	w.Bus.Subscribe(events.AssignmentRequested, "dispatch-matcher", w.onAssignmentRequested)
	w.Bus.Subscribe(events.ResourceReservationRequested, "inventory-reserver", w.onReservationRequested)
	w.Bus.Subscribe(events.ResourceReserved, "notification-requester", w.onResourceReserved)
	w.Bus.Subscribe(events.NotificationRequested, "notification-sender", w.onNotificationRequested)
	w.Bus.Subscribe(events.NotificationSent, "incident-assignment-finalizer", w.onNotificationSent)
	w.Bus.Subscribe(events.ResourceReservationFailed, "assignment-compensator", w.onReservationFailed)
	w.Bus.Subscribe(events.ResponderRejected, "rejection-compensator", w.onResponderRejected)
}

func (w *Workflow) CreateIncident(in CreateIncidentInput, key, correlation string) (domain.Incident, bool, error) {
	if in.Type == "" || in.Description == "" || in.Severity < 1 || in.Severity > 5 || in.Latitude < -90 || in.Latitude > 90 || in.Longitude < -180 || in.Longitude > 180 {
		return domain.Incident{}, false, errors.New("invalid incident input")
	}
	if correlation == "" {
		correlation = events.NewID()
	}
	now := time.Now().UTC()
	i := domain.Incident{ID: events.NewID(), Type: in.Type, Severity: in.Severity, Latitude: in.Latitude, Longitude: in.Longitude, Description: in.Description, Status: domain.Created, CreatedAt: now, UpdatedAt: now, Version: 1}
	e := events.New(events.IncidentCreated, i.ID, correlation, "", "incident-service", i)
	created, wasNew := w.Store.CreateIncident(i, key, e)
	if wasNew {
		w.Flush()
		created, _ = w.Store.GetIncident(i.ID)
	}
	return created, wasNew, nil
}

func (w *Workflow) Flush() {
	for w.Store.DrainOutbox(w.Bus.Publish) > 0 {
	}
}

func (w *Workflow) outboxTransition(id string, to domain.IncidentStatus, reason string, parent events.Envelope) error {
	payload := map[string]any{"incident_id": id, "status": to, "reason": reason}
	e := events.New(events.IncidentStatusUpdated, id, parent.CorrelationID, parent.EventID, "incident-service", payload)
	_, err := w.Store.Transition(id, to, reason, e)
	return err
}

func (w *Workflow) onIncidentCreated(e events.Envelope) error {
	if err := w.outboxTransition(e.AggregateID, domain.Validated, "synthetic validation passed", e); err != nil {
		return err
	}
	if err := w.outboxTransition(e.AggregateID, domain.AssignmentPending, "dispatch requested", e); err != nil {
		return err
	}
	req := events.New(events.AssignmentRequested, e.AggregateID, e.CorrelationID, e.EventID, "incident-service", map[string]any{"incident_id": e.AggregateID})
	w.Store.mu.Lock()
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: req})
	w.Store.mu.Unlock()
	return nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func capability(incidentType string) string {
	switch incidentType {
	case "fire":
		return "fire"
	case "medical":
		return "medical"
	default:
		return "rescue"
	}
}
func distance(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.
	p1 := lat1 * math.Pi / 180
	p2 := lat2 * math.Pi / 180
	dp := (lat2 - lat1) * math.Pi / 180
	dl := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dp/2)*math.Sin(dp/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	return r * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func (w *Workflow) onAssignmentRequested(e events.Envelope) error {
	w.Store.mu.Lock()
	defer w.Store.mu.Unlock()
	if id := w.Store.AssignmentByIncident[e.AggregateID]; id != "" && w.Store.Assignments[id].Status == "PENDING" {
		return nil
	}
	incident := w.Store.Incidents[e.AggregateID]
	if incident == nil {
		return errors.New("incident not found")
	}
	type candidate struct {
		r     *domain.Responder
		score float64
	}
	var cs []candidate
	need := capability(incident.Type)
	for _, r := range w.Store.Responders {
		if r.Available && contains(r.Capabilities, need) {
			d := distance(incident.Latitude, incident.Longitude, r.Latitude, r.Longitude)
			cs = append(cs, candidate{r, 100 - (d * 2) - float64(r.Workload*15)})
		}
	}
	if len(cs) == 0 {
		return errors.New("no suitable responder")
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].score == cs[j].score {
			return cs[i].r.ID < cs[j].r.ID
		}
		return cs[i].score > cs[j].score
	})
	chosen := cs[0]
	a := &domain.Assignment{ID: events.NewID(), IncidentID: incident.ID, ResponderID: chosen.r.ID, Status: "PENDING", Score: chosen.score, ExpiresAt: time.Now().UTC().Add(5 * time.Minute)}
	w.Store.Assignments[a.ID] = a
	w.Store.AssignmentByIncident[incident.ID] = a.ID
	chosen.r.Available = false
	chosen.r.Workload++
	assigned := events.New(events.ResponderAssigned, incident.ID, e.CorrelationID, e.EventID, "dispatch-service", a)
	reserve := events.New(events.ResourceReservationRequested, incident.ID, e.CorrelationID, assigned.EventID, "dispatch-service", map[string]any{"incident_id": incident.ID, "assignment_id": a.ID, "resource_kind": resourceKind(incident.Type)})
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: assigned}, OutboxRecord{Event: reserve})
	return nil
}
func resourceKind(t string) string {
	if t == "fire" {
		return "fire_engine"
	}
	if t == "medical" {
		return "ambulance"
	}
	return "rescue_kit"
}

func (w *Workflow) onReservationRequested(e events.Envelope) error {
	var p struct {
		AssignmentID string `json:"assignment_id"`
		Kind         string `json:"resource_kind"`
	}
	_ = json.Unmarshal(e.Payload, &p)
	w.Store.mu.Lock()
	defer w.Store.mu.Unlock()
	var chosen *domain.Resource
	ids := make([]string, 0, len(w.Store.Resources))
	for id := range w.Store.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := w.Store.Resources[id]
		if r.Kind == p.Kind && r.Available {
			chosen = r
			break
		}
	}
	if chosen == nil {
		failed := events.New(events.ResourceReservationFailed, e.AggregateID, e.CorrelationID, e.EventID, "inventory-service", map[string]any{"incident_id": e.AggregateID, "assignment_id": p.AssignmentID, "reason": "no resource available"})
		w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: failed})
		return nil
	}
	expires := time.Now().UTC().Add(15 * time.Minute)
	chosen.Available = false
	chosen.ReservedFor = e.AggregateID
	chosen.ReservationExpiresAt = &expires
	chosen.Version++
	reserved := events.New(events.ResourceReserved, e.AggregateID, e.CorrelationID, e.EventID, "inventory-service", map[string]any{"incident_id": e.AggregateID, "assignment_id": p.AssignmentID, "resource_id": chosen.ID})
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: reserved})
	return nil
}

func (w *Workflow) onResourceReserved(e events.Envelope) error {
	req := events.New(events.NotificationRequested, e.AggregateID, e.CorrelationID, e.EventID, "inventory-service", map[string]any{"incident_id": e.AggregateID, "channel": "push"})
	w.Store.mu.Lock()
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: req})
	w.Store.mu.Unlock()
	return nil
}

func (w *Workflow) onNotificationRequested(e events.Envelope) error {
	w.failureMu.Lock()
	attempt := w.notificationFailures[e.EventID] + 1
	w.notificationFailures[e.EventID] = attempt
	w.failureMu.Unlock()
	status := "SENT"
	reason := ""
	if attempt <= w.FailNotificationsFor {
		status = "TEMPORARY_FAILURE"
		reason = "injected transient failure"
	}
	w.Store.mu.Lock()
	w.Store.Attempts = append(w.Store.Attempts, domain.NotificationAttempt{EventID: e.EventID, IncidentID: e.AggregateID, Channel: "push", Attempt: attempt, Status: status, FailureReason: reason, AttemptedAt: time.Now().UTC()})
	w.Store.mu.Unlock()
	if reason != "" {
		w.Metrics.Retries.Add(1)
		return errors.New(reason)
	}
	sent := events.New(events.NotificationSent, e.AggregateID, e.CorrelationID, e.EventID, "notification-service", map[string]any{"incident_id": e.AggregateID, "channel": "push"})
	w.Store.mu.Lock()
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: sent})
	w.Store.mu.Unlock()
	return nil
}

func (w *Workflow) onNotificationSent(e events.Envelope) error {
	if err := w.outboxTransition(e.AggregateID, domain.Assigned, "responder, resource, and notification confirmed", e); err != nil {
		return err
	}
	w.Store.mu.Lock()
	if id := w.Store.AssignmentByIncident[e.AggregateID]; id != "" {
		w.Store.Assignments[id].Status = "ACCEPTED"
	}
	w.Store.mu.Unlock()
	return nil
}

func (w *Workflow) onReservationFailed(e events.Envelope) error {
	w.Store.mu.Lock()
	if id := w.Store.AssignmentByIncident[e.AggregateID]; id != "" {
		a := w.Store.Assignments[id]
		a.Status = "CANCELLED"
		if r := w.Store.Responders[a.ResponderID]; r != nil {
			r.Available = true
			if r.Workload > 0 {
				r.Workload--
			}
		}
	}
	w.Store.mu.Unlock()
	return w.outboxTransition(e.AggregateID, domain.Unassigned, "resource reservation failed; assignment compensated", e)
}

func (w *Workflow) onResponderRejected(e events.Envelope) error {
	w.releaseIncidentResource(e.AggregateID, e)
	w.Store.mu.Lock()
	if id := w.Store.AssignmentByIncident[e.AggregateID]; id != "" {
		a := w.Store.Assignments[id]
		a.Status = "REJECTED"
		if r := w.Store.Responders[a.ResponderID]; r != nil {
			r.Available = false
		}
	}
	delete(w.Store.AssignmentByIncident, e.AggregateID)
	w.Store.mu.Unlock()
	if err := w.outboxTransition(e.AggregateID, domain.Unassigned, "responder rejected assignment", e); err != nil {
		return err
	}
	if err := w.outboxTransition(e.AggregateID, domain.AssignmentPending, "reassignment requested", e); err != nil {
		return err
	}
	req := events.New(events.AssignmentRequested, e.AggregateID, e.CorrelationID, e.EventID, "dispatch-service", map[string]any{"incident_id": e.AggregateID})
	w.Store.mu.Lock()
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: req})
	w.Store.mu.Unlock()
	return nil
}

func (w *Workflow) releaseIncidentResource(incidentID string, parent events.Envelope) {
	w.Store.mu.Lock()
	defer w.Store.mu.Unlock()
	for _, r := range w.Store.Resources {
		if r.ReservedFor == incidentID {
			r.Available = true
			r.ReservedFor = ""
			r.ReservationExpiresAt = nil
			r.Version++
			w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: events.New(events.ResourceReleased, incidentID, parent.CorrelationID, parent.EventID, "inventory-service", map[string]any{"resource_id": r.ID})})
		}
	}
}

func (w *Workflow) RejectAssignment(incidentID, correlation string) error {
	w.Store.mu.RLock()
	id := w.Store.AssignmentByIncident[incidentID]
	w.Store.mu.RUnlock()
	if id == "" {
		return errors.New("active assignment not found")
	}
	e := events.New(events.ResponderRejected, incidentID, correlation, "", "dispatch-service", map[string]any{"assignment_id": id})
	w.Store.mu.Lock()
	w.Store.Outbox = append(w.Store.Outbox, OutboxRecord{Event: e})
	w.Store.mu.Unlock()
	w.Flush()
	return nil
}

func (w *Workflow) Responders() []domain.Responder {
	w.Store.mu.RLock()
	defer w.Store.mu.RUnlock()
	out := make([]domain.Responder, 0, len(w.Store.Responders))
	for _, r := range w.Store.Responders {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (w *Workflow) Resources() []domain.Resource {
	w.Store.mu.RLock()
	defer w.Store.mu.RUnlock()
	out := make([]domain.Resource, 0, len(w.Store.Resources))
	for _, r := range w.Store.Resources {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
