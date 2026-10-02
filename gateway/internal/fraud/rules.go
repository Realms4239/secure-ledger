package fraud

import "sync"

// Project A workload as a gateway plugin: stateless checks over in-memory
// windows. A hold never touches the sink — the transfer stays out of the
// ledger and lands in the audit log + fraud_hold_total for review.

const velocityWindowSec = 60
const velocityMax = 5
const geoImpossibleSec = 30

// New-account amount caps in minor units by account age.
const (
	newAccountDays   = 30
	newAccountCap    = 50_000
	youngAccountDays = 365
	youngAccountCap  = 500_000
)

// VelocityExceeded reports >5 events in the trailing 60s.
func VelocityExceeded(hist []int64, now int64) bool {
	n := 0
	for _, ts := range hist {
		if now-ts < velocityWindowSec {
			n++
		}
	}
	return n > velocityMax
}

// AmountExceeds applies the static new-account table (minor units, v0).
func AmountExceeds(amount float64, ageDays int) bool {
	switch {
	case ageDays < newAccountDays:
		return amount > newAccountCap
	case ageDays < youngAccountDays:
		return amount > youngAccountCap
	default:
		return false
	}
}

// GeoImpossible flags a cross-operator hop faster than any human can move.
func GeoImpossible(prevOp string, prevTs int64, curOp string, curTs int64) bool {
	return prevOp != "" && prevOp != curOp && curTs-prevTs < geoImpossibleSec
}

type senderWindow struct {
	times  []int64
	lastOp string
	lastTs int64
	age    int
}

// Store keeps per-sender windows. Ages default to established (400d); tests
// and the seed harness set real ages via SetAge.
type Store struct {
	mu      sync.Mutex
	senders map[string]*senderWindow
}

func New() *Store { return &Store{senders: make(map[string]*senderWindow)} }

func (s *Store) SetAge(sender string, days int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.senders[sender]
	if w == nil {
		w = &senderWindow{age: days}
		s.senders[sender] = w
		return
	}
	w.age = days
}

// Check records this attempt and reports whether it must be held for review.
func (s *Store) Check(sender, op string, amount float64, now int64) (held bool, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.senders[sender]
	if w == nil {
		w = &senderWindow{age: 400}
		s.senders[sender] = w
	}
	if GeoImpossible(w.lastOp, w.lastTs, op, now) {
		return true, "geo"
	}
	if AmountExceeds(amount, w.age) {
		return true, "amount"
	}
	w.times = append(w.times, now)
	// prune outside the window so histories stay short
	kept := w.times[:0]
	for _, ts := range w.times {
		if now-ts < velocityWindowSec {
			kept = append(kept, ts)
		}
	}
	w.times = kept
	w.lastOp, w.lastTs = op, now
	if VelocityExceeded(w.times, now) {
		return true, "velocity"
	}
	return false, ""
}
