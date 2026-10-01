package ipc

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// FileSink appends one JSON object per event: {"t":1,"id":"<uuid>","ts":<ns>,"p":"{...}"}.
// Same schema the engine tailer parses; payload bytes are embedded verbatim as a string.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
}

func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

func (s *FileSink) Write(typ byte, sagaID [16]byte, payload []byte) error {
	v := map[string]any{
		"t":  typ,
		"id": uuid.UUID(sagaID).String(),
		"ts": time.Now().UnixNano(),
		"p":  string(payload),
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.f.Write(append(raw, '\n'))
	return err
}

func (s *FileSink) Close() error { return s.f.Close() }
