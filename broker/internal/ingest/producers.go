package ingest

import (
	"context"
	"sort"
	"sync"
	"time"

	"broker/internal/plog"
)

// producerKey identifies one streamer process: the streamer's pod ordinal and
// the random epoch it picked at start. Sequences restart at 0 per epoch.
type producerKey struct {
	streamer uint16
	epoch    uint32
}

// producer is the dedupe state of one producer. next[p] is one past the
// highest sequence already in partition p's log; a record is a retry exactly
// when its sequence is below that. Per-partition (not per-producer) state is
// what keeps a crash safe: a resent record always hashes to the same
// partition, so a partition whose tail was lost does not reject it.
type producer struct {
	next []uint64

	// owner is held by the stream currently serving this producer. A new
	// stream cancels the old one and waits here until it has exited, so two
	// streams never append for the same producer at once.
	owner sync.Mutex

	// guarded by producers.mu
	gen      uint64
	cancel   context.CancelFunc
	lastUsed time.Time
}

type producers struct {
	mu      sync.Mutex
	m       map[producerKey]*producer
	parts   int
	max     int
	idleTTL time.Duration
}

func newProducers(parts, max int, idleTTL time.Duration) *producers {
	return &producers{m: map[producerKey]*producer{}, parts: parts, max: max, idleTTL: idleTTL}
}

// register makes cancel the active stream of key, cancelling any previous
// one. It returns the producer, the registration's generation and whether a
// previous stream was fenced off.
func (ps *producers) register(key producerKey, cancel context.CancelFunc, now time.Time) (*producer, uint64, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p := ps.m[key]
	if p == nil {
		ps.evictLocked(now)
		p = &producer{next: make([]uint64, ps.parts)}
		ps.m[key] = p
	}
	fenced := p.cancel != nil
	if fenced {
		p.cancel()
	}
	p.gen++
	p.cancel = cancel
	p.lastUsed = now
	return p, p.gen, fenced
}

// release clears the active stream if gen is still the latest registration.
func (ps *producers) release(p *producer, gen uint64, now time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if p.gen == gen {
		p.cancel = nil
	}
	p.lastUsed = now
}

// evictLocked drops producers with no live stream that have been idle longer
// than idleTTL, then, if still over max, the least recently used idle ones.
// An evicted producer's late retries are appended again; the database key
// removes those duplicates.
func (ps *producers) evictLocked(now time.Time) {
	var idle []producerKey
	for k, p := range ps.m {
		if p.cancel != nil {
			continue
		}
		if now.Sub(p.lastUsed) > ps.idleTTL {
			delete(ps.m, k)
			continue
		}
		idle = append(idle, k)
	}
	if len(ps.m) < ps.max {
		return
	}
	sort.Slice(idle, func(a, b int) bool { return ps.m[idle[a]].lastUsed.Before(ps.m[idle[b]].lastUsed) })
	for _, k := range idle {
		if len(ps.m) < ps.max {
			return
		}
		delete(ps.m, k)
	}
}

func (ps *producers) len() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.m)
}

// rebuild seeds the dedupe table from the headers of the last tail entries of
// every partition, so retries that arrive after a broker restart are still
// recognised.
func (ps *producers) rebuild(store *plog.Store, tail uint64) {
	for i := 0; i < store.Partitions(); i++ {
		part := store.Partition(i)
		first, last := part.First(), part.Last()
		if last < first {
			continue
		}
		from := first
		if last-first >= tail {
			from = last - tail + 1
		}
		for idx := from; idx <= last; idx++ {
			h, err := part.HeaderAt(idx)
			if err != nil {
				continue
			}
			key := producerKey{streamer: h.StreamerID, epoch: h.Epoch}
			p := ps.m[key]
			if p == nil {
				p = &producer{next: make([]uint64, ps.parts), lastUsed: time.Now()}
				ps.m[key] = p
			}
			if h.LastSeq+1 > p.next[i] {
				p.next[i] = h.LastSeq + 1
			}
		}
	}
}
