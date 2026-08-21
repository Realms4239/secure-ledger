package idempotency

import "sync"

type Store struct {
	mu sync.Mutex
	m  map[string]string
}

func New() *Store { return &Store{m: make(map[string]string)} }

func (s *Store) CheckOrStore(key, sagaID string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.m[key]; ok {
		return true, existing
	}
	s.m[key] = sagaID
	return false, ""
}
