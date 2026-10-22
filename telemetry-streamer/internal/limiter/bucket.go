// Package limiter implements a small non-blocking token bucket.
package limiter

import (
	"math"
	"sync"
	"time"
)

// Bucket is a token bucket measured in rows. It never blocks: Take returns how
// many of the requested tokens were available right now.
type Bucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
}

// New returns a bucket that starts empty (no initial burst).
func New(rate, burst float64, now time.Time) *Bucket {
	return &Bucket{rate: rate, burst: burst, tokens: 0, last: now}
}

// Take removes up to n whole tokens and returns how many were granted.
func (b *Bucket) Take(n int, now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens = math.Min(b.burst, b.tokens+el*b.rate)
	}
	b.last = now
	granted := int(math.Min(float64(n), math.Floor(b.tokens)))
	if granted < 0 {
		granted = 0
	}
	b.tokens -= float64(granted)
	return granted
}

// SetRate changes the refill rate (hook for the runtime rate endpoint stretch item).
func (b *Bucket) SetRate(rate, burst float64) {
	b.mu.Lock()
	b.rate, b.burst = rate, burst
	if b.tokens > burst {
		b.tokens = burst
	}
	b.mu.Unlock()
}
