package fraud

import "testing"

func TestVelocity(t *testing.T) {
	now := int64(1_700_000_000)
	five := []int64{now - 50, now - 40, now - 30, now - 20, now - 10}
	if VelocityExceeded(five, now) {
		t.Fatal("5 events in 60s should pass")
	}
	six := append(append([]int64{}, five...), now-5)
	if !VelocityExceeded(six, now) {
		t.Fatal("6 events in 60s should hold")
	}
	old := []int64{now - 400, now - 300, now - 200, now - 100, now - 61, now - 62}
	if VelocityExceeded(old, now) {
		t.Fatal("events older than 60s should not count")
	}
}

func TestAmount(t *testing.T) {
	if !AmountExceeds(60_000, 10) {
		t.Fatal("new account over 50000 should hold")
	}
	if AmountExceeds(40_000, 10) {
		t.Fatal("new account under cap should pass")
	}
	if !AmountExceeds(600_000, 100) {
		t.Fatal("young account over 500000 should hold")
	}
	if AmountExceeds(600_000, 400) {
		t.Fatal("established account should pass any amount")
	}
}

func TestGeo(t *testing.T) {
	if !GeoImpossible("mvola", 100, "orange", 110) {
		t.Fatal("cross-operator hop in 10s should hold")
	}
	if GeoImpossible("mvola", 100, "orange", 200) {
		t.Fatal("cross-operator hop after 60s should pass")
	}
	if GeoImpossible("mvola", 100, "mvola", 101) {
		t.Fatal("same operator should always pass")
	}
}

func TestStoreVelocityEndToEnd(t *testing.T) {
	s := New()
	now := int64(1_700_000_000)
	for i := 0; i < 5; i++ {
		held, _ := s.Check("alice", "mvola", 100, now+int64(i))
		if held {
			t.Fatalf("event %d should pass", i)
		}
	}
	held, reason := s.Check("alice", "mvola", 100, now+5)
	if !held || reason != "velocity" {
		t.Fatalf("6th rapid event should hold for velocity, got %v %q", held, reason)
	}
}
