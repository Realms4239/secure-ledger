package audit

import (
	"encoding/json"
	"io"
	"time"
)

type Logger struct {
	w io.Writer
}

func New(w io.Writer) *Logger { return &Logger{w: w} }

func (l *Logger) Log(decision, subject, route, sagaID, idempotencyKey string, latencyMs int64) {
	line, err := json.Marshal(map[string]any{
		"ts":              time.Now().Format(time.RFC3339Nano),
		"decision":        decision,
		"subject":         subject,
		"route":           route,
		"saga_id":         sagaID,
		"idempotency_key": idempotencyKey,
		"latency_ms":      latencyMs,
	})
	if err != nil {
		return
	}
	_, _ = l.w.Write(append(line, '\n'))
}
