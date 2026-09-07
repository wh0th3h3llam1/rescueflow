package platform

import (
	"encoding/json"
	"sync"

	"github.com/rescueflow/rescueflow/internal/events"
)

type Hub struct {
	mu          sync.Mutex
	next        int
	subscribers map[string]map[int]chan []byte
}

func NewHub() *Hub { return &Hub{subscribers: map[string]map[int]chan []byte{}} }
func (h *Hub) Subscribe(incidentID string) (int, <-chan []byte, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	id := h.next
	if h.subscribers[incidentID] == nil {
		h.subscribers[incidentID] = map[int]chan []byte{}
	}
	ch := make(chan []byte, 16)
	h.subscribers[incidentID][id] = ch
	return id, ch, func() { h.mu.Lock(); defer h.mu.Unlock(); delete(h.subscribers[incidentID], id); close(ch) }
}
func (h *Hub) Broadcast(e events.Envelope) {
	b, _ := json.Marshal(e)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subscribers[e.AggregateID] {
		select {
		case ch <- b:
		default:
		}
	}
}
func (h *Hub) Connections() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, xs := range h.subscribers {
		n += len(xs)
	}
	return n
}
