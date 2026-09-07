package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rescueflow/rescueflow/internal/platform"
)

func TestCreateAndGetIncident(t *testing.T) {
	s := New(platform.NewWorkflow())
	body := []byte(`{"incident_type":"medical","severity":4,"latitude":47.61,"longitude":-122.33,"description":"synthetic"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/incidents", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", "api-test")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("create status %d: %s", rr.Code, rr.Body.String())
	}
	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRequest(http.MethodGet, "/api/v1/incidents/"+created.ID, nil)
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, get)
	if out.Code != 200 {
		t.Fatalf("get status %d", out.Code)
	}
	if created.Status != "ASSIGNED" {
		t.Fatalf("unexpected final status %s", created.Status)
	}
}
