package domain

import (
	"errors"
	"time"
)

type IncidentStatus string

const (
	Created           IncidentStatus = "CREATED"
	Validated         IncidentStatus = "VALIDATED"
	AssignmentPending IncidentStatus = "ASSIGNMENT_PENDING"
	Assigned          IncidentStatus = "ASSIGNED"
	InProgress        IncidentStatus = "IN_PROGRESS"
	Resolved          IncidentStatus = "RESOLVED"
	Unassigned        IncidentStatus = "UNASSIGNED"
	Cancelled         IncidentStatus = "CANCELLED"
)

var transitions = map[IncidentStatus]map[IncidentStatus]bool{
	Created:           {Validated: true, Cancelled: true},
	Validated:         {AssignmentPending: true, Cancelled: true},
	AssignmentPending: {Assigned: true, Unassigned: true, Cancelled: true},
	Unassigned:        {AssignmentPending: true, Cancelled: true},
	Assigned:          {InProgress: true, Unassigned: true, Cancelled: true},
	InProgress:        {Resolved: true, Cancelled: true},
}

type Incident struct {
	ID          string         `json:"id"`
	Type        string         `json:"incident_type"`
	Severity    int            `json:"severity"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	Description string         `json:"description"`
	Status      IncidentStatus `json:"status"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Version     int64          `json:"version"`
}

type StatusHistory struct {
	IncidentID string         `json:"incident_id"`
	From       IncidentStatus `json:"from_status,omitempty"`
	To         IncidentStatus `json:"to_status"`
	Reason     string         `json:"reason"`
	OccurredAt time.Time      `json:"occurred_at"`
}

func (i *Incident) Transition(to IncidentStatus) error {
	if i.Status == to {
		return nil
	}
	if !transitions[i.Status][to] {
		return errors.New("invalid incident status transition")
	}
	i.Status, i.UpdatedAt, i.Version = to, time.Now().UTC(), i.Version+1
	return nil
}
