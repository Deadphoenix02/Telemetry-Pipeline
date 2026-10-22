package collector

import (
	"context"
	"log/slog"
	"time"

	"collector/internal/metrics"
	"collector/internal/sink"
)

// Registry keeps the gpus table (GET /gpus) current: a GPU is written the
// first time this collector sees it and again every refresh interval while it
// keeps reporting. Writes happen in the background and never hold up inserts
// or commits; a failed write is retried on the next tick.
type Registry struct {
	sink    sink.Sink
	refresh time.Duration
	tick    time.Duration
	m       *metrics.Metrics
	log     *slog.Logger

	in      chan map[[16]byte]struct{}
	written map[[16]byte]time.Time // owned by Run
	pending map[[16]byte]struct{}  // owned by Run
}

func NewRegistry(s sink.Sink, refresh, tick time.Duration, m *metrics.Metrics, log *slog.Logger) *Registry {
	return &Registry{sink: s, refresh: refresh, tick: tick, m: m, log: log,
		in: make(chan map[[16]byte]struct{}, 8), written: map[[16]byte]time.Time{}, pending: map[[16]byte]struct{}{}}
}

// Seen reports the GPUs of an inserted block. It never blocks: if the writer
// is busy the set is dropped, and a later block reports the same GPUs again.
func (r *Registry) Seen(rows []sink.Row) {
	set := map[[16]byte]struct{}{}
	for i := range rows {
		set[rows[i].GPU] = struct{}{}
	}
	select {
	case r.in <- set:
	default:
	}
}

// Run writes due GPUs every tick until ctx ends, then makes one last attempt.
func (r *Registry) Run(ctx context.Context) {
	t := time.NewTicker(r.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			r.drainIn()
			r.write(fctx)
			cancel()
			return
		case set := <-r.in:
			r.merge(set)
		case <-t.C:
			r.write(ctx)
		}
	}
}

func (r *Registry) drainIn() {
	for {
		select {
		case set := <-r.in:
			r.merge(set)
		default:
			return
		}
	}
}

func (r *Registry) merge(set map[[16]byte]struct{}) {
	now := time.Now()
	for g := range set {
		if last, ok := r.written[g]; !ok || now.Sub(last) >= r.refresh {
			r.pending[g] = struct{}{}
		}
	}
}

func (r *Registry) write(ctx context.Context) {
	if len(r.pending) == 0 {
		return
	}
	gpus := make([][16]byte, 0, len(r.pending))
	for g := range r.pending {
		gpus = append(gpus, g)
	}
	now := time.Now()
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.sink.UpsertGPUs(wctx, gpus, now); err != nil {
		r.m.RegistryErrors.Add(1)
		r.log.Warn("GPU registry write failed; will retry", "gpus", len(gpus), "err", err)
		return
	}
	for _, g := range gpus {
		r.written[g] = now
		delete(r.pending, g)
	}
	r.m.RegistryUpserts.Add(uint64(len(gpus)))
}
