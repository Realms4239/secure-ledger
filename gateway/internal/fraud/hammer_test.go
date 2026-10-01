package fraud

import (
	"sync"
	"sync/atomic"
	"testing"
)

// No race detector on this box (no cgo); this hammer at least proves the
// store survives concurrent use without panics or lost updates.
func TestStoreConcurrentHammer(t *testing.T) {
	s := New()
	var held, passed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sender := "hammer"
			if w%2 == 0 {
				sender = "hammer-even"
			}
			for i := 0; i < 20; i++ {
				ok, _ := s.Check(sender, "mvola", 100, int64(1_700_000_000+w*20+i))
				if ok {
					held.Add(1)
				} else {
					passed.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	if held.Load()+passed.Load() != 1000 {
		t.Fatalf("lost updates: held=%d passed=%d", held.Load(), passed.Load())
	}
}
