package ipc

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// Concurrent appends must interleave as whole lines, never tear.
func TestFileSinkConcurrentHammer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	s, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := uuid.New()
				if err := s.Write(0x01, [16]byte(id), []byte(`{"x":1}`)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	f, _ := os.Open(path)
	defer f.Close()
	lines := 0
	for sc := bufio.NewScanner(f); sc.Scan(); lines++ {
	}
	if lines != 1000 {
		t.Fatalf("lines = %d, want 1000", lines)
	}
}
