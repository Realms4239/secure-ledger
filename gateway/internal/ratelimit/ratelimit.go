package ratelimit

import (
	"net"
	"sync"
	"time"
)

// Limiter is a per-IP token bucket. rps sets the sustained rate, burst the
// short burst. Zero value is unusable — always New. Map growth is bounded by
// periodic reset (see Allow).
type Limiter struct {
	mu      sync.Mutex
	rps     float64
	burst   float64
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func New(rps float64, burst int) *Limiter {
	return &Limiter{rps: rps, burst: float64(burst), buckets: make(map[string]*bucket)}
}

// Allow reports whether a request from ip may proceed, consuming a token.
func (l *Limiter) Allow(ip string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buckets) > 65536 {
		// ponytail: wholesale reset instead of LRU — a flood of spoofed IPs
		// degrades to briefly permissive, never to unbounded memory.
		l.buckets = make(map[string]*bucket)
	}
	b := l.buckets[ip]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[ip] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rps
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// ClientIP strips the port; unparsable remotes pass through as one bucket.
func ClientIP(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}
