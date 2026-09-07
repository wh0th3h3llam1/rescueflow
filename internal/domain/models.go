package domain

import "time"

type Responder struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities"`
	Available    bool     `json:"available"`
	Latitude     float64  `json:"latitude"`
	Longitude    float64  `json:"longitude"`
	Workload     int      `json:"workload"`
}

type Assignment struct {
	ID          string    `json:"id"`
	IncidentID  string    `json:"incident_id"`
	ResponderID string    `json:"responder_id"`
	Status      string    `json:"status"`
	Score       float64   `json:"score"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Resource struct {
	ID                   string     `json:"id"`
	Kind                 string     `json:"kind"`
	Available            bool       `json:"available"`
	ReservedFor          string     `json:"reserved_for,omitempty"`
	Version              int64      `json:"version"`
	ReservationExpiresAt *time.Time `json:"reservation_expires_at,omitempty"`
}

type NotificationAttempt struct {
	EventID       string    `json:"event_id"`
	IncidentID    string    `json:"incident_id"`
	Channel       string    `json:"channel"`
	Attempt       int       `json:"attempt"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
	AttemptedAt   time.Time `json:"attempted_at"`
}
