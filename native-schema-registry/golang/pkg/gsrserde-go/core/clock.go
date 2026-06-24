package gsrserde

import (
	"sync"
	"time"
)

// Clock is the narrow time-source seam the schema caches consume. The
// production implementation reads the wall clock; tests inject a fake
// that lets them advance time deterministically without time.Sleep.
//
// Java parity: Caffeine accepts a Ticker for cache entry timing
// (Caffeine.newBuilder().ticker(...)); this is the Go analog at the
// same conceptual layer.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// realClock is the production Clock implementation. The zero value is
// usable.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RealClock is the canonical production Clock value. Callers that need
// a Clock but want default wall-clock semantics use this.
var RealClock Clock = realClock{}

// FakeClock is a test-only Clock whose Now returns a stored time.Time
// that callers advance with Advance. It is intentionally exported in
// the production package (rather than _test.go-scoped) so integration
// tests outside pkg/gsrserde-go/core/ can also import it.
//
// Concurrency: FakeClock is safe for concurrent Now and Advance calls.
// Per spec C12, the multithreaded TTL test must not race on the clock
// itself, so both reads and writes go through the same mutex.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock pinned to start. Subsequent Advance
// calls move the clock forward; Now reflects the latest position.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the FakeClock's current time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the FakeClock forward by d. Negative durations are
// accepted and move the clock backwards — callers are responsible for
// the semantics.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
