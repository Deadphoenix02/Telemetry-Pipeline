// Package collector moves telemetry from the broker into ClickHouse. For each
// consumer-group session it decodes frames into rows, inserts them as one
// block per flush (at BatchRows rows, BatchWait after the first row, or
// BufferBytes of rows), and only after the insert succeeds commits the next
// offset of every partition in the block. Anything not committed is fetched
// again by the partition's next owner; the table's key absorbs the replays.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
	"unsafe"

	"broker/pkg/consumer"
	"broker/pkg/frame"

	"collector/internal/metrics"
	"collector/internal/sink"
)

type Options struct {
	BatchRows     int
	BatchWait     time.Duration
	BufferBytes   int
	InsertTimeout time.Duration
	PoisonPolicy  string // skip | stop
}

// session is what a handler needs from consumer.Session; tests may fake it.
type session interface {
	Batches() <-chan consumer.Batch
	Commit(ctx context.Context, offsets map[uint32]uint64) error
}

type adapter struct{ s *consumer.Session }

func (a adapter) Batches() <-chan consumer.Batch { return a.s.Batches }
func (a adapter) Commit(ctx context.Context, o map[uint32]uint64) error {
	return a.s.Commit(ctx, o)
}

const rowSize = int(unsafe.Sizeof(sink.Row{}))

// Collector is shared by the sessions of every broker it consumes from: they
// run concurrently, so per-session state lives in each run's block.
type Collector struct {
	opts  Options
	sink  sink.Sink
	reg   *Registry
	m     *metrics.Metrics
	log   *slog.Logger
	fatal chan error
}

func New(opts Options, s sink.Sink, reg *Registry, m *metrics.Metrics, log *slog.Logger) *Collector {
	return &Collector{opts: opts, sink: s, reg: reg, m: m, log: log, fatal: make(chan error, 1)}
}

// Fatal delivers an error that should stop the process (poison_policy=stop).
func (c *Collector) Fatal() <-chan error { return c.fatal }

// Handle is the consumer.Handler: it runs until the session ends (rebalance or
// shutdown), then inserts and commits what it holds before returning.
func (c *Collector) Handle(ctx context.Context, s *consumer.Session) error {
	c.log.Info("session started", "generation", s.Generation, "partitions", s.Partitions)
	return c.run(ctx, adapter{s})
}

// block is the rows of the next insert and, per partition, the offset to
// commit once they are stored.
type block struct {
	rows   []sink.Row
	next   map[uint32]uint64
	bytes  int
	intern map[string]string // metric names, per session: sessions run concurrently

	// What this block last added to the shared buffer gauges, which sum all sessions.
	shownRows, shownBytes int64
}

// show moves the shared buffer gauges by this block's change since the last call.
func (b *block) show(m *metrics.Metrics) {
	rows, bytes := int64(len(b.rows)), int64(b.bytes)
	m.BufferRows.Add(rows - b.shownRows)
	m.BufferBytes.Add(bytes - b.shownBytes)
	b.shownRows, b.shownBytes = rows, bytes
}

func (b *block) pending() bool { return len(b.rows) > 0 || len(b.next) > 0 }

func (b *block) reset() {
	b.rows = b.rows[:0]
	clear(b.next)
	b.bytes = 0
}

func (c *Collector) run(ctx context.Context, s session) error {
	blk := &block{next: map[uint32]uint64{}, intern: map[string]string{}}
	defer func() { // this session's rows leave the shared gauges
		c.m.BufferRows.Add(-blk.shownRows)
		c.m.BufferBytes.Add(-blk.shownBytes)
	}()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return c.final(s, blk)
		case b, ok := <-s.Batches():
			if !ok {
				return c.final(s, blk)
			}
			wasPending := blk.pending()
			if err := c.add(blk, b); err != nil {
				return err
			}
			if !wasPending && blk.pending() {
				timer.Reset(c.opts.BatchWait)
			}
			if len(blk.rows) >= c.opts.BatchRows || blk.bytes >= c.opts.BufferBytes {
				timer.Stop()
				if err := c.flush(ctx, s, blk); err != nil {
					return c.final(s, blk)
				}
			}
		case <-timer.C:
			if err := c.flush(ctx, s, blk); err != nil {
				return c.final(s, blk)
			}
		}
		blk.show(c.m)
	}
}

func (b *block) internName(name []byte) string {
	if s, ok := b.intern[string(name)]; ok { // no allocation for the lookup
		return s
	}
	if len(b.intern) > 10_000 { // metric names are few; guard against junk
		clear(b.intern)
	}
	s := string(name)
	b.intern[s] = s
	return s
}

// add decodes one batch into the block. An undecodable batch is skipped (and
// committed past) or, with poison_policy=stop, ends the process.
func (c *Collector) add(blk *block, b consumer.Batch) error {
	if b.Skipped {
		c.m.SkippedRanges.Add(1)
		c.log.Error("broker could not read a log range; skipping it", "partition", b.Partition, "from", b.Offset, "to", b.Next)
		blk.next[b.Partition] = b.Next
		return nil
	}
	start := len(blk.rows)
	h, err := frame.ParseHeader(b.Frame)
	if err == nil {
		_, err = frame.Decode(b.Frame, blk.internName, func(r *frame.Record) error {
			blk.rows = append(blk.rows, sink.Row{GPU: r.UUID, TsNanos: r.TsNanos, Metric: r.Metric, StreamerID: h.StreamerID, Value: r.Value})
			return nil
		})
	}
	if err != nil {
		blk.rows = blk.rows[:start]
		c.m.PoisonBatches.Add(1)
		c.log.Error("undecodable batch", "partition", b.Partition, "offset", b.Offset, "err", err, "policy", c.opts.PoisonPolicy)
		if c.opts.PoisonPolicy == "stop" {
			err = fmt.Errorf("undecodable batch at partition %d offset %d: %w (poison_policy=stop)", b.Partition, b.Offset, err)
			select {
			case c.fatal <- err:
			default:
			}
			return err
		}
		blk.next[b.Partition] = b.Next
		return nil
	}
	if uint32(h.Partition) != b.Partition {
		c.m.MisroutedRecords.Add(uint64(h.RecordCount))
	}
	blk.next[b.Partition] = b.Next
	blk.bytes += (len(blk.rows) - start) * rowSize
	return nil
}

// flush inserts the block (retrying until it succeeds or ctx ends) and then
// commits its offsets. It returns an error only when ctx ended first; the
// block is then left for final.
func (c *Collector) flush(ctx context.Context, s session, blk *block) error {
	if !blk.pending() {
		return nil
	}
	if len(blk.rows) > 0 {
		if err := c.insert(ctx, blk.rows); err != nil {
			return err
		}
	}
	c.commit(ctx, s, blk)
	return nil
}

// final runs when the session ends: one bounded attempt to store and commit
// what is buffered. The generation is still valid while the rebalance is being
// prepared, so the commit normally succeeds; if it does not, the next owner
// re-reads the rows and the table key removes the duplicates.
func (c *Collector) final(s session, blk *block) error {
	if !blk.pending() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.opts.InsertTimeout+2*time.Second)
	defer cancel()
	if err := c.flush(ctx, s, blk); err != nil {
		c.log.Warn("could not store buffered rows before the session ended; the next owner will re-read them",
			"rows", len(blk.rows), "err", err)
	}
	blk.reset()
	return nil
}

func (c *Collector) insert(ctx context.Context, rows []sink.Row) error {
	for attempt := 1; ; attempt++ {
		ictx, cancel := context.WithTimeout(ctx, c.opts.InsertTimeout)
		start := time.Now()
		err := c.sink.InsertTelemetry(ictx, rows)
		cancel()
		if err == nil {
			c.m.InsertLatency.Observe(time.Since(start))
			c.m.Inserts.Add(1)
			c.m.RowsInserted.Add(uint64(len(rows)))
			return nil
		}
		c.m.InsertErrors.Add(1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		wait := backoff(attempt)
		c.log.Warn("insert failed; retrying", "rows", len(rows), "attempt", attempt, "in", wait.Round(time.Millisecond), "err", err)
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

func (c *Collector) commit(ctx context.Context, s session, blk *block) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.Commit(cctx, blk.next)
	switch {
	case err == nil:
		c.m.Commits.Add(1)
	case errors.Is(err, consumer.ErrFenced):
		c.m.CommitsFenced.Add(1)
		c.log.Info("commit fenced: the partitions moved to another member", "offsets", blk.next)
	default:
		// The rows are stored; a later commit of the same partitions covers
		// these offsets, and if none comes the next owner re-reads them.
		c.m.CommitErrors.Add(1)
		c.log.Warn("commit failed", "offsets", blk.next, "err", err)
	}
	if c.reg != nil && len(blk.rows) > 0 {
		c.reg.Seen(blk.rows)
	}
	blk.reset()
}

// backoff is exponential from 100 ms to 5 s with ±20 % jitter.
func backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(min(attempt-1, 6))
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}
