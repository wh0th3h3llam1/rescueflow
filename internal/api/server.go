package api

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rescueflow/rescueflow/internal/domain"
	"github.com/rescueflow/rescueflow/internal/events"
	"github.com/rescueflow/rescueflow/internal/platform"
)

type Server struct {
	Workflow *platform.Workflow
	mux      *http.ServeMux
}
type apiError struct {
	Error struct {
		Code          string `json:"code"`
		Message       string `json:"message"`
		CorrelationID string `json:"correlation_id"`
	} `json:"error"`
}

func New(w *platform.Workflow) *Server {
	s := &Server{Workflow: w, mux: http.NewServeMux()}
	s.routes()
	return s
}
func (s *Server) Handler() http.Handler { return s.middleware(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("/api/v1/incidents", s.incidents)
	s.mux.HandleFunc("/api/v1/incidents/", s.incident)
	s.mux.HandleFunc("/api/v1/ws/incidents/", s.wsIncident)
	s.mux.HandleFunc("/api/v1/responders", s.responders)
	s.mux.HandleFunc("/api/v1/responders/", s.responder)
	s.mux.HandleFunc("/api/v1/resources", s.resources)
	s.mux.HandleFunc("/api/v1/resources/", s.resource)
	s.mux.HandleFunc("/api/v1/health/services", s.health)
	s.mux.HandleFunc("/api/v1/operations/dead-letters", s.deadLetters)
	s.mux.HandleFunc("/metrics", s.metrics)
	s.mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, map[string]string{"status": "ok"}) })
	s.mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, map[string]string{"status": "ready"})
	})
}

func (s *Server) wsIncident(w http.ResponseWriter, r *http.Request) {
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/ws/incidents/"), "/")
	if id == "" {
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("incident not found"))
		return
	}
	s.websocket(w, r, id)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		correlation := r.Header.Get("X-Correlation-ID")
		if correlation == "" {
			correlation = events.NewID()
		}
		w.Header().Set("X-Correlation-ID", correlation)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Idempotency-Key,X-Correlation-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		r.Header.Set("X-Correlation-ID", correlation)
		s.Workflow.Metrics.HTTPRequests.Add(1)
		defer func() { s.Workflow.Metrics.HTTPDurationNanos.Add(time.Since(started).Nanoseconds()) }()
		next.ServeHTTP(w, r)
	})
}
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code string, err error) {
	s.Workflow.Metrics.HTTPErrors.Add(1)
	var body apiError
	body.Error.Code = code
	body.Error.Message = err.Error()
	body.Error.CorrelationID = r.Header.Get("X-Correlation-ID")
	jsonResponse(w, status, body)
}
func decode(r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

func (s *Server) incidents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var in platform.CreateIncidentInput
		if err := decode(r, &in); err != nil {
			s.fail(w, r, 400, "INVALID_REQUEST", err)
			return
		}
		incident, isNew, err := s.Workflow.CreateIncident(in, r.Header.Get("Idempotency-Key"), r.Header.Get("X-Correlation-ID"))
		if err != nil {
			s.fail(w, r, 422, "VALIDATION_FAILED", err)
			return
		}
		status := 201
		if !isNew {
			status = 200
		}
		jsonResponse(w, status, incident)
	case http.MethodGet:
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		offset, _ := strconv.Atoi(q.Get("offset"))
		severity, _ := strconv.Atoi(q.Get("severity"))
		items := s.Workflow.Store.ListIncidents(domain.IncidentStatus(q.Get("status")), severity, offset, limit)
		jsonResponse(w, 200, map[string]any{"items": items, "limit": limit, "offset": offset})
	default:
		s.fail(w, r, 405, "METHOD_NOT_ALLOWED", fmt.Errorf("method not allowed"))
	}
}

func (s *Server) incident(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/incidents/"), "/"), "/")
	if len(parts) < 1 || parts[0] == "" {
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("incident not found"))
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		v, ok := s.Workflow.Store.GetIncident(id)
		if !ok {
			s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("incident not found"))
			return
		}
		jsonResponse(w, 200, v)
		return
	}
	if len(parts) == 2 && parts[1] == "timeline" && r.Method == http.MethodGet {
		jsonResponse(w, 200, s.Workflow.Store.Timeline(id))
		return
	}
	if len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet {
		jsonResponse(w, 200, s.Workflow.Store.IncidentEvents(id))
		return
	}
	if len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodPatch {
		var body struct {
			Status domain.IncidentStatus `json:"status"`
			Reason string                `json:"reason"`
		}
		if err := decode(r, &body); err != nil {
			s.fail(w, r, 400, "INVALID_REQUEST", err)
			return
		}
		parent := events.New("api.status.requested", id, r.Header.Get("X-Correlation-ID"), "", "gateway-service", map[string]any{"status": body.Status})
		e := events.New(events.IncidentStatusUpdated, id, parent.CorrelationID, parent.EventID, "incident-service", map[string]any{"status": body.Status, "reason": body.Reason})
		v, err := s.Workflow.Store.Transition(id, body.Status, body.Reason, e)
		if err != nil {
			s.fail(w, r, 409, "INVALID_TRANSITION", err)
			return
		}
		s.Workflow.Flush()
		jsonResponse(w, 200, v)
		return
	}
	if len(parts) == 2 && parts[1] == "reject-assignment" && r.Method == http.MethodPost {
		if err := s.Workflow.RejectAssignment(id, r.Header.Get("X-Correlation-ID")); err != nil {
			s.fail(w, r, 409, "REJECTION_FAILED", err)
			return
		}
		v, _ := s.Workflow.Store.GetIncident(id)
		jsonResponse(w, 200, v)
		return
	}
	if len(parts) == 3 && parts[1] == "ws" {
		s.websocket(w, r, id)
		return
	}
	s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("route not found"))
}

func (s *Server) responders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.fail(w, r, 405, "METHOD_NOT_ALLOWED", fmt.Errorf("method not allowed"))
		return
	}
	jsonResponse(w, 200, map[string]any{"items": s.Workflow.Responders()})
}
func (s *Server) responder(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/responders/"), "/"), "/")
	if len(parts) != 2 || r.Method != http.MethodPatch {
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("route not found"))
		return
	}
	s.Workflow.Store.LockForAPI()
	defer s.Workflow.Store.UnlockForAPI()
	res := s.Workflow.Store.Responders[parts[0]]
	if res == nil {
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("responder not found"))
		return
	}
	switch parts[1] {
	case "availability":
		var b struct {
			Available bool `json:"available"`
		}
		if err := decode(r, &b); err != nil {
			s.fail(w, r, 400, "INVALID_REQUEST", err)
			return
		}
		res.Available = b.Available
	case "location":
		var b struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		}
		if err := decode(r, &b); err != nil {
			s.fail(w, r, 400, "INVALID_REQUEST", err)
			return
		}
		res.Latitude = b.Latitude
		res.Longitude = b.Longitude
	default:
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("route not found"))
		return
	}
	jsonResponse(w, 200, res)
}

func (s *Server) resources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.fail(w, r, 405, "METHOD_NOT_ALLOWED", fmt.Errorf("method not allowed"))
		return
	}
	jsonResponse(w, 200, map[string]any{"items": s.Workflow.Resources()})
}
func (s *Server) resource(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/resources/"), "/"), "/")
	if len(parts) != 2 || r.Method != http.MethodPost {
		s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("route not found"))
		return
	}
	var body struct {
		IncidentID      string `json:"incident_id"`
		ExpectedVersion int64  `json:"expected_version"`
	}
	if err := decode(r, &body); err != nil {
		s.fail(w, r, 400, "INVALID_REQUEST", err)
		return
	}
	if parts[1] == "reserve" {
		res, err := s.Workflow.Store.Reserve(parts[0], body.IncidentID, body.ExpectedVersion, 15*time.Minute)
		if err != nil {
			s.Workflow.Metrics.ReservationConflicts.Add(1)
			s.fail(w, r, 409, "RESERVATION_CONFLICT", err)
			return
		}
		jsonResponse(w, 200, res)
		return
	}
	if parts[1] == "release" {
		if err := s.Workflow.Store.Release(parts[0], body.IncidentID); err != nil {
			s.fail(w, r, 409, "RELEASE_FAILED", err)
			return
		}
		jsonResponse(w, 200, map[string]string{"status": "released"})
		return
	}
	s.fail(w, r, 404, "NOT_FOUND", fmt.Errorf("route not found"))
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, 200, map[string]any{"status": "healthy", "services": []map[string]string{{"name": "incident-service", "status": "up"}, {"name": "dispatch-service", "status": "up"}, {"name": "inventory-service", "status": "up"}, {"name": "notification-service", "status": "up"}}, "outbox_backlog": s.Workflow.Store.OutboxBacklog(), "dead_letters": len(s.Workflow.Bus.DeadLetters())})
}
func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		jsonResponse(w, 200, map[string]any{"items": s.Workflow.Bus.DeadLetters()})
		return
	}
	if r.Method == http.MethodPost {
		idx, _ := strconv.Atoi(r.URL.Query().Get("index"))
		if err := s.Workflow.Bus.Replay(idx); err != nil {
			s.fail(w, r, 409, "REPLAY_FAILED", err)
			return
		}
		s.Workflow.Flush()
		jsonResponse(w, 200, map[string]string{"status": "replayed"})
		return
	}
	s.fail(w, r, 405, "METHOD_NOT_ALLOWED", fmt.Errorf("method not allowed"))
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	m := s.Workflow.Metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "rescueflow_http_requests_total %d\nrescueflow_http_errors_total %d\nrescueflow_http_request_duration_seconds_sum %.6f\nrescueflow_http_request_duration_seconds_count %d\nrescueflow_events_processed_total %d\nrescueflow_retry_total %d\nrescueflow_dead_letter_total %d\nrescueflow_outbox_backlog %d\nrescueflow_reservation_conflicts_total %d\nrescueflow_websocket_connections %d\n", m.HTTPRequests.Load(), m.HTTPErrors.Load(), float64(m.HTTPDurationNanos.Load())/1e9, m.HTTPRequests.Load(), m.Events.Load(), m.Retries.Load(), len(s.Workflow.Bus.DeadLetters()), s.Workflow.Store.OutboxBacklog(), m.ReservationConflicts.Load(), s.Workflow.Hub.Connections())
}

func (s *Server) websocket(w http.ResponseWriter, r *http.Request, incidentID string) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		s.fail(w, r, 426, "UPGRADE_REQUIRED", fmt.Errorf("websocket upgrade required"))
		return
	}
	h, ok := w.(http.Hijacker)
	if !ok {
		s.fail(w, r, 500, "WEBSOCKET_UNAVAILABLE", fmt.Errorf("hijacking unavailable"))
		return
	}
	conn, rw, err := h.Hijack()
	if err != nil {
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	_ = rw.Flush()
	_, ch, unsubscribe := s.Workflow.Hub.Subscribe(incidentID)
	defer unsubscribe()
	defer conn.Close()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case data, ok := <-ch:
			if !ok || writeFrame(conn, data) != nil {
				return
			}
		case <-ticker.C:
			if writeFrame(conn, []byte(`{"type":"heartbeat"}`)) != nil {
				return
			}
		}
	}
}
func writeFrame(conn net.Conn, payload []byte) error {
	header := []byte{0x81}
	n := len(payload)
	if n < 126 {
		header = append(header, byte(n))
	} else if n <= 65535 {
		header = append(header, 126, byte(n>>8), byte(n))
	} else {
		return fmt.Errorf("frame too large")
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}
