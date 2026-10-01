package ratelimit

import (
	"testing"
	"time"
)

func TestAllowsBurstThenDenies(t *testing.T) {
	l := New(1, 3) // 1 token/sec, burst 3
	ip := "10.0.0.1"
	for i := 0; i < 3; i++ {
		if !l.Allow(ip) {
			t.Fatalf("request %d should pass (burst)", i)
		}
	}
	if l.Allow(ip) {
		t.Fatal("4th rapid request should be limited")
	}
	// Independent bucket per IP: a fresh address gets a full burst.
	for i := 0; i < 3; i++ {
		if !l.Allow("10.0.0.2") {
			t.Fatalf("other IP request %d should pass (own bucket)", i)
		}
	}
}

func TestRefillsOverTime(t *testing.T) {
	l := New(10, 1)
	if !l.Allow("10.0.0.3") {
		t.Fatal("first request should pass")
	}
	if l.Allow("10.0.0.3") {
		t.Fatal("immediate second should be limited")
	}
	time.Sleep(300 * time.Millisecond) // 10rps → ~3 tokens
	if !l.Allow("10.0.0.3") {
		t.Fatal("should refill within 300ms at 10rps")
	}
}

func TestClientIPStripsPort(t *testing.T) {
	if got := ClientIP("192.0.2.1:1234"); got != "192.0.2.1" {
		t.Fatalf("ip = %q", got)
	}
	if got := ClientIP("garbage"); got != "garbage" {
		t.Fatalf("unparsable passes through: %q", got)
	}
}
