package dash

import (
	"bufio"
	"context"
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
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>SettleLedger</title>") {
		t.Fatalf("dashboard = %d, want 200 with title", rec.Code)
	}
}

func TestBenchPlaceholder(t *testing.T) {
	h := NewHub()
	mux := http.NewServeMux()
	h.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/bench-latest.json", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"measured":false`) {
		t.Fatalf("bench = %d %q, want measured:false", rec.Code, rec.Body.String())
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
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, `data: {"kind":"accepted"`) {
		t.Fatalf("first line = %q", line)
	}
}
