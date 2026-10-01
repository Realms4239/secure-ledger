package dash

import (
	"embed"
	"fmt"
	"net/http"
	"sync"
	"time"
)

//go:embed web/dashboard.html web/bench-latest.json
var webFS embed.FS

// Hub broadcasts gateway intake/decision events to dashboard SSE clients.
// Subscribers get a short replay (so a freshly opened dashboard sees recent
// activity), then live events. Slow clients drop events, never block intake.
type Hub struct {
	mu     sync.Mutex
	subs   map[chan string]struct{}
	recent []string
}

func NewHub() *Hub { return &Hub{subs: make(map[chan string]struct{})} }

func (h *Hub) Publish(eventJSON string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recent = append(h.recent, eventJSON)
	if len(h.recent) > 8 {
		h.recent = h.recent[len(h.recent)-8:]
	}
	for ch := range h.subs {
		select {
		case ch <- eventJSON:
		default: // ponytail: drop-slowest; a stuck dashboard never slows intake
		}
	}
}

func (h *Hub) Subscribe() (chan string, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan string, 16)
	for _, e := range h.recent {
		ch <- e
	}
	h.subs[ch] = struct{}{}
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs, ch)
	}
}

func (h *Hub) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /dashboard", h.handleDashboard)
	mux.HandleFunc("GET /events/stream", h.handleStream)
	mux.HandleFunc("GET /bench-latest.json", h.handleBench)
}

func (h *Hub) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	raw, err := webFS.ReadFile("web/dashboard.html")
	if err != nil {
		http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(raw)
}

func (h *Hub) handleBench(w http.ResponseWriter, _ *http.Request) {
	raw, err := webFS.ReadFile("web/bench-latest.json")
	if err != nil {
		http.Error(w, "bench data unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func (h *Hub) handleStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch, unsub := h.Subscribe()
	defer unsub()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case e := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", e)
			fl.Flush()
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
