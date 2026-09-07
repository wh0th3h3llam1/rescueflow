package events

import (
	"encoding/json"
	"errors"
	"time"
)

const Version = 1

const (
	IncidentCreated              = "incident.created"
	IncidentValidated            = "incident.validated"
	IncidentStatusUpdated        = "incident.status.updated"
	AssignmentRequested          = "assignment.requested"
	ResponderAssigned            = "responder.assigned"
	ResponderRejected            = "responder.rejected"
	AssignmentCancelled          = "assignment.cancelled"
	ResourceReservationRequested = "resource.reservation.requested"
	ResourceReserved             = "resource.reserved"
	ResourceReservationFailed    = "resource.reservation.failed"
	ResourceReleased             = "resource.released"
	NotificationRequested        = "notification.requested"
	NotificationSent             = "notification.sent"
	NotificationFailed           = "notification.failed"
)

type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id,omitempty"`
	Producer      string          `json:"producer"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceParent   string          `json:"traceparent,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

func New(eventType, aggregateID, correlationID, causationID, producer string, payload any) Envelope {
	b, _ := json.Marshal(payload)
	return Envelope{EventID: NewID(), EventType: eventType, EventVersion: Version, AggregateID: aggregateID,
		CorrelationID: correlationID, CausationID: causationID, Producer: producer, OccurredAt: time.Now().UTC(), Payload: b}
}

func (e Envelope) Validate() error {
	if e.EventID == "" || e.EventType == "" || e.AggregateID == "" || e.CorrelationID == "" || e.Producer == "" {
		return errors.New("event envelope is missing a required field")
	}
	if e.EventVersion != Version || e.OccurredAt.IsZero() || !json.Valid(e.Payload) {
		return errors.New("event envelope has an invalid version, timestamp, or payload")
	}
	return nil
}
