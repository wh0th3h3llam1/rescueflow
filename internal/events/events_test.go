package events

import "testing"

func TestEnvelopeValidation(t *testing.T) {
	e := New(IncidentCreated, NewID(), NewID(), "", "test", map[string]string{"ok": "true"})
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	e.EventVersion = 99
	if err := e.Validate(); err == nil {
		t.Fatal("expected unsupported version to fail")
	}
}
