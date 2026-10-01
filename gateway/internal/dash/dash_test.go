package dash

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardServes(t *testing.T) {
	h := NewHub()
	mux := http.NewServeMux()
	h.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/dashboard", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>SettleLedger") {
		t.Fatalf("dashboard = %d, want 200 with title", rec.Code)
	}
	for _, marker := range []string{`rel="icon"`, `id="summary"`, `id="l-mismatch"`, `id="benchbars"`, `id="pauseBtn`} {
		if !strings.Contains(rec.Body.String(), marker) {
			t.Fatalf("dashboard missing %s", marker)
		}
	}
}

func TestBenchLatest(t *testing.T) {
	h := NewHub()
	mux := http.NewServeMux()
	h.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/bench-latest.json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("bench = %d, want 200", rec.Code)
	}
	var v map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v["measured"] != true || v["p99_ms"] == nil || v["report"] == nil {
		t.Fatalf("bench lacks measured numbers: %s", rec.Body.String())
	}
}

func TestStreamReplaysThenLives(t *testing.T) {
	h := NewHub()
	mux := http.NewServeMux()
	h.Register(mux)
	h.Publish(`{"kind":"accepted","saga_id":"s1"}`)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	br := bufio.NewReader(resp.Body)
	hello, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hello, ": connected") {
		t.Fatalf("hello = %q", hello)
	}
	line, err := br.ReadString('\n') // blank separator after the hello
	if err != nil {
		t.Fatal(err)
	}
	line, err = br.ReadString('\n') // first replayed event
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, `data: {"kind":"accepted"`) {
		t.Fatalf("first line = %q", line)
	}
}
