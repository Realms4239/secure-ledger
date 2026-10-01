package ipc

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestFileSink_AppendsJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	s, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := uuid.New()
	payload := []byte(`{"from":"alice","to":"bob","amount":100,"idempotency_key":"k"}`)
	if err := s.Write(0x01, [16]byte(id), payload); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		lines++
		var v map[string]any
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			t.Fatalf("line is not JSON: %v", err)
		}
		if v["id"] != id.String() || v["t"] != float64(1) {
			t.Fatalf("bad line: %s", sc.Text())
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(v["p"].(string)), &p); err != nil {
			t.Fatalf("payload not embedded JSON: %v", err)
		}
	}
	if lines != 1 {
		t.Fatalf("want 1 line got %d", lines)
	}
}
