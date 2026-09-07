package domain

import "testing"

func TestIncidentTransitions(t *testing.T) {
	i := Incident{Status: Created, Version: 1}
	if err := i.Transition(Validated); err != nil {
		t.Fatal(err)
	}
	if err := i.Transition(Resolved); err == nil {
		t.Fatal("expected invalid transition to fail")
	}
	if i.Status != Validated || i.Version != 2 {
		t.Fatalf("unexpected state: %+v", i)
	}
}
