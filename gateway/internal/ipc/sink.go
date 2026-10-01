package ipc

// Sink is the event transport. RingWriter (shm bench path) and FileSink
// (Windows demo path) both implement it. Handler code uses Sink only.
type Sink interface {
	Write(typ byte, sagaID [16]byte, payload []byte) error
}

// ErrSinkFull reports honest backpressure; identical to ErrRingFull so
// existing 503 mapping keeps working for both transports.
var ErrSinkFull = ErrRingFull
