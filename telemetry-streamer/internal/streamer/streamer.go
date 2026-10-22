// Package streamer paces CSV rows out as individual records, on one stream at
// a time to one of several brokers, failing over between them (see worker).
package streamer

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"telemetry-streamer/internal/limiter"
	"telemetry-streamer/internal/metrics"
	"telemetry-streamer/internal/source"
	"telemetry-streamer/internal/wire"
)

type Options struct {
	ProducerID string
	Rate       int           // rows per second (1..100000)
	Tick       time.Duration // pacing micro-tick; each tick emits what has accrued
	Grace      time.Duration // shutdown flush/ack deadline

	// Failover. Zero values take the defaults below.
	Home             int           // index of this streamer's home broker
	Window           time.Duration // unacked data held for resend, as time at Rate (3s)
	WindowMaxRecords int           // cap on the window in records (60000)
	ConnectTimeout   time.Duration // one stream-open attempt (1s)
	AckTimeout       time.Duration // oldest record unacked this long: fail over (2s)
	HomeProbe        time.Duration // while away from home, try to return this often (10s)
}

func (o *Options) defaults() {
	if o.Window <= 0 {
		o.Window = 3 * time.Second
	}
	if o.WindowMaxRecords <= 0 {
		o.WindowMaxRecords = 60000
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = time.Second
	}
	if o.AckTimeout <= 0 {
		o.AckTimeout = 2 * time.Second
	}
	if o.HomeProbe <= 0 {
		o.HomeProbe = 10 * time.Second
	}
}

// QueueFor sizes the queue between the pacer and the worker: 200 ms of rows,
// at least 256. The worker can be descheduled for tens of milliseconds, for
// example by CPU throttling during the resend burst after a failover; the
// queue has to outlast that, or the pacer blocks while a broker is up.
func QueueFor(rate int) int { return max(256, rate/5) }

// WindowFor is the max unacked records: window worth of data at rate, capped at
// maxRecords, at least 100. It has to outlast a failover (ack timeout plus one
// connect) for the pacer never to block while a broker is up.
func WindowFor(rate int, window time.Duration, maxRecords int) int {
	n := int(float64(rate) * window.Seconds())
	if maxRecords > 0 && n > maxRecords {
		n = maxRecords
	}
	return max(100, n)
}

type Streamer struct {
	opt     Options
	src     source.Source
	targets []Target
	m       *metrics.Metrics
	log     *slog.Logger
	bucket  *limiter.Bucket
	seq     uint64 // next record sequence; only touched by the pacer loop
}

func New(opt Options, src source.Source, targets []Target, m *metrics.Metrics, log *slog.Logger) *Streamer {
	opt.defaults()
	// Burst = at most 100 ms (or two ticks) of catch-up after a late tick, so
	// the stream stays smooth instead of bursting.
	burst := float64(opt.Rate) * math.Max(0.1, 2*opt.Tick.Seconds())
	return &Streamer{
		opt: opt, src: src, targets: targets, m: m, log: log,
		bucket: limiter.New(float64(opt.Rate), burst, time.Now()),
	}
}

// Run blocks until ctx is cancelled (SIGTERM), then stops emitting, flushes and
// waits for acks up to the grace period. It returns an error if records were
// left unacked or the source failed.
func (s *Streamer) Run(ctx context.Context) error {
	window := WindowFor(s.opt.Rate, s.opt.Window, s.opt.WindowMaxRecords)
	queueCap := QueueFor(s.opt.Rate)
	names := make([]string, len(s.targets))
	for i, t := range s.targets {
		names[i] = t.Name
	}
	s.log.Info("streamer starting", "producer_id", s.opt.ProducerID,
		"rate_rows_per_sec", s.opt.Rate, "tick", s.opt.Tick, "window_records", window,
		"brokers", names, "home", names[s.opt.Home])
	s.m.SetHomeBroker(s.opt.Home)

	// The worker outlives ctx: it keeps flushing during the grace period.
	wctx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	w := newWorker(s.opt.ProducerID, s.targets, WorkerOptions{
		Window: window, QueueCap: queueCap, Home: s.opt.Home,
		ConnectTimeout: s.opt.ConnectTimeout, AckTimeout: s.opt.AckTimeout, HomeProbe: s.opt.HomeProbe,
	}, s.m, s.log)
	done := make(chan int, 1)
	go func() { done <- w.run(wctx) }()

	ticker := time.NewTicker(s.opt.Tick)
	defer ticker.Stop()

	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			break
		}
		if err := s.tick(wctx, w); err != nil {
			loopErr = err
			break
		}
	}

	s.log.Info("stopping; flushing and waiting for acks", "grace", s.opt.Grace)
	close(w.in)
	timer := time.AfterFunc(s.opt.Grace, func() {
		s.log.Warn("grace period expired; abandoning unacked records")
		cancelWorker()
	})
	unacked := <-done
	timer.Stop()

	s.log.Info("streamer stopped", "rows_emitted", s.m.RowsEmitted.Load(),
		"records_acked", s.m.RecordsAcked.Load(), "unacked_records", unacked)
	if loopErr != nil {
		return loopErr
	}
	if unacked > 0 {
		return fmt.Errorf("%d records unacked at exit", unacked)
	}
	return nil
}

// tick emits every row the bucket has accrued since the last tick.
func (s *Streamer) tick(wctx context.Context, w *worker) error {
	n := s.bucket.Take(math.MaxInt32, time.Now())
	for i := 0; i < n; i++ {
		row, err := s.src.Next()
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		rec := wire.Record{
			Seq:            s.seq,
			TimestampNanos: time.Now().UnixNano(), // stamped once, here
			Metric:         row.S.Metric,
			GPUID:          row.S.GPUID,
			Device:         row.S.Device,
			UUID:           row.S.UUID,
			ModelName:      row.S.ModelName,
			Hostname:       row.S.Hostname,
			Container:      row.S.Container,
			Pod:            row.S.Pod,
			Namespace:      row.S.Namespace,
			Value:          row.Value,
		}
		s.seq++
		s.m.RowsEmitted.Add(1)
		if err := s.dispatch(wctx, w, rec); err != nil {
			return err
		}
	}
	return nil
}

// dispatch hands one record to the worker; a full queue blocks (backpressure)
// and the blocked time is recorded.
func (s *Streamer) dispatch(wctx context.Context, w *worker, r wire.Record) error {
	select {
	case w.in <- r:
		return nil
	default:
	}
	s.m.BeginBlocked(time.Now())
	defer func() { s.m.EndBlocked(time.Now()) }()
	select {
	case w.in <- r:
		return nil
	case <-wctx.Done():
		return wctx.Err()
	}
}
