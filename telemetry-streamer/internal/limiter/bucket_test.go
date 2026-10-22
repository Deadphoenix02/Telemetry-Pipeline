package limiter

import (
	"testing"
	"time"
)

func TestBucketRefillCapsAndRateChanges(t *testing.T) {
	now := time.Unix(100, 0)
	b := New(4, 3, now)
	if got := b.Take(2, now); got != 0 {
		t.Fatalf("new bucket granted %d tokens", got)
	}
	if got := b.Take(1, now.Add(125*time.Millisecond)); got != 0 {
		t.Fatalf("fractional token granted early: %d", got)
	}
	if got := b.Take(2, now.Add(500*time.Millisecond)); got != 2 {
		t.Fatalf("granted %d tokens, want 2", got)
	}
	if got := b.Take(10, now.Add(5*time.Second)); got != 3 {
		t.Fatalf("burst cap granted %d tokens, want 3", got)
	}
	if got := b.Take(0, now.Add(6*time.Second)); got != 0 {
		t.Fatalf("zero request granted %d tokens", got)
	}
	if got := b.Take(-2, now.Add(7*time.Second)); got != 0 {
		t.Fatalf("negative request granted %d tokens", got)
	}

	b.Take(0, now.Add(8*time.Second))
	b.SetRate(0, 0)
	if got := b.Take(10, now.Add(20*time.Second)); got != 0 {
		t.Fatalf("rate change failed to trim stored tokens: %d", got)
	}
}

func TestBucketDoesNotRefillWhenTimeMovesBackward(t *testing.T) {
	now := time.Unix(200, 0)
	b := New(10, 5, now)
	if got := b.Take(5, now.Add(time.Second)); got != 5 {
		t.Fatalf("initial refill granted %d tokens, want 5", got)
	}
	if got := b.Take(1, now); got != 0 {
		t.Fatalf("backward time granted %d tokens", got)
	}
}
