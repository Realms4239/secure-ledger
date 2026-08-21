package idempotency

import "testing"

func TestIdempotencyStore_Dedup(t *testing.T) {
	s := New()
	dup, existing := s.CheckOrStore("key-1", "saga-1")
	if dup {
		t.Fatal("first insert should not be dup")
	}
	dup2, existing2 := s.CheckOrStore("key-1", "saga-2")
	if !dup2 || existing2 != "saga-1" {
		t.Fatalf("want dup with saga-1 got %v %q", dup2, existing2)
	}
	_ = existing
}
