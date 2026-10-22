// Package consumer is the client library for the broker's consumer group. It
// joins the group, heartbeats, fetches every assigned partition and hands the
// caller one Session per generation. The caller acks by committing offsets,
// normally only after the data is safely stored, which gives at-least-once
// delivery: anything not committed is fetched again by the next owner.
//
// Rebalancing is eager. When the coordinator starts a rebalance the session's
// context is cancelled; the caller finishes its in-flight work, commits (the
// generation is still valid while the rebalance is being prepared) and returns,
// and the consumer rejoins. Heartbeats continue until the session returns.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"broker/pkg/queuepb"
)

// ErrFenced is returned by Commit when this member no longer owns the
// partitions in the current generation; the session is over.
var ErrFenced = errors.New("consumer: commit fenced by a newer generation")

type Config struct {
	Group         string
	ClientID      string        // shown in broker logs, e.g. the pod name
	FetchMaxBytes uint32        // 0 = broker default
	FetchMaxWait  time.Duration // 0 = broker default
	BufferBatches int           // batches buffered between the fetch loops and the caller
	Log           *slog.Logger
}

// Batch is one frame from a partition. Commit Next once it is processed.
// Skipped batches carry no frame: the broker could not read [Offset, Next)
// from disk.
type Batch struct {
	Partition uint32
	Offset    uint64
	Next      uint64
	Frame     []byte
	Skipped   bool
}

// Session is one generation of group membership.
type Session struct {
	MemberID   string
	Generation uint64
	Partitions []uint32
	// Batches delivers frames from every assigned partition, in offset order
	// per partition. It is closed when the session's fetch loops stop.
	Batches <-chan Batch

	c *Consumer
}

// Commit acks offsets (the next offset to read, per partition). It returns
// ErrFenced if the generation is over.
func (s *Session) Commit(ctx context.Context, offsets map[uint32]uint64) error {
	if len(offsets) == 0 {
		return nil
	}
	req := &queuepb.CommitOffsetRequest{Group: s.c.cfg.Group, MemberId: s.MemberID, Generation: s.Generation}
	for p, off := range offsets {
		req.Offsets = append(req.Offsets, &queuepb.PartitionOffset{Partition: p, Offset: off})
	}
	resp, err := s.c.coord.CommitOffset(ctx, req)
	if err != nil {
		return err
	}
	switch resp.Error {
	case queuepb.ErrorCode_OK:
		return nil
	case queuepb.ErrorCode_ILLEGAL_GENERATION, queuepb.ErrorCode_UNKNOWN_MEMBER, queuepb.ErrorCode_NOT_ASSIGNED:
		return ErrFenced
	default:
		return fmt.Errorf("consumer: commit rejected: %v", resp.Error)
	}
}

// Handler processes one session. It must return once ctx is cancelled, after
// finishing (and committing) whatever it has in flight.
type Handler func(ctx context.Context, s *Session) error

// Stats are counters a caller may export as metrics.
type Stats struct {
	Rebalances atomic.Uint64
	Assigned   atomic.Int64
	InGroup    atomic.Bool   // a member of the current generation (between join and the session's end)
	Skipped    atomic.Uint64 // corrupt ranges skipped by the broker
	// HighWatermark per partition, as last reported by Fetch.
	hw sync.Map // uint32 -> uint64
}

// HighWatermarks returns the last reported high watermark per partition.
func (st *Stats) HighWatermarks() map[uint32]uint64 {
	out := map[uint32]uint64{}
	st.hw.Range(func(k, v any) bool { out[k.(uint32)] = v.(uint64); return true })
	return out
}

type Consumer struct {
	cfg    Config
	coord  queuepb.CoordinatorClient
	broker queuepb.BrokerClient
	Stats  Stats
}

func New(cc grpc.ClientConnInterface, cfg Config) *Consumer {
	if cfg.BufferBatches <= 0 {
		cfg.BufferBatches = 16
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Consumer{cfg: cfg, coord: queuepb.NewCoordinatorClient(cc), broker: queuepb.NewBrokerClient(cc)}
}

func (c *Consumer) backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(min(attempt-1, 6))
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// Run joins the group and calls h once per generation until ctx ends, then
// leaves the group so the remaining members rebalance immediately.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	memberID := ""
	defer func() {
		if memberID == "" {
			return
		}
		lctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = c.coord.LeaveGroup(lctx, &queuepb.LeaveGroupRequest{Group: c.cfg.Group, MemberId: memberID})
		c.cfg.Log.Info("left the group", "member", memberID)
	}()
	attempt := 0
	for ctx.Err() == nil {
		resp, err := c.coord.JoinGroup(ctx, &queuepb.JoinGroupRequest{Group: c.cfg.Group, MemberId: memberID, ClientId: c.cfg.ClientID})
		if err == nil && resp.Error == queuepb.ErrorCode_UNKNOWN_MEMBER {
			memberID = "" // the broker restarted or dropped us: join as a new member
			continue
		}
		if err == nil && resp.Error != queuepb.ErrorCode_OK {
			err = fmt.Errorf("join rejected: %v", resp.Error)
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			attempt++
			c.cfg.Log.Warn("join failed", "err", err, "attempt", attempt)
			sleepCtx(ctx, c.backoff(attempt))
			continue
		}
		attempt = 0
		memberID = resp.MemberId
		c.Stats.Rebalances.Add(1)
		c.Stats.Assigned.Store(int64(len(resp.Partitions)))
		c.Stats.InGroup.Store(true)
		c.cfg.Log.Info("joined", "member", memberID, "generation", resp.Generation, "partitions", resp.Partitions)
		lost := c.session(ctx, resp, h)
		c.Stats.InGroup.Store(false)
		if lost {
			memberID = ""
		}
	}
	c.Stats.Assigned.Store(0)
	return ctx.Err()
}

// session runs one generation. It reports whether the membership was lost
// (so the next join must be as a new member).
func (c *Consumer) session(ctx context.Context, resp *queuepb.JoinGroupResponse, h Handler) bool {
	sctx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	hbCtx, stopHB := context.WithCancel(context.Background())
	var lost atomic.Bool
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		c.heartbeat(hbCtx, resp, cancelSession, &lost)
	}()

	batches := make(chan Batch, c.cfg.BufferBatches)
	var fetchers sync.WaitGroup
	if len(resp.Partitions) > 0 {
		offs, err := c.coord.FetchOffsets(sctx, &queuepb.FetchOffsetsRequest{Group: c.cfg.Group, Partitions: resp.Partitions})
		if err != nil || offs.Error != queuepb.ErrorCode_OK {
			c.cfg.Log.Warn("fetching committed offsets failed; rejoining", "err", err)
			cancelSession()
		} else {
			for _, po := range offs.Offsets {
				fetchers.Add(1)
				go func(p uint32, off uint64) {
					defer fetchers.Done()
					c.fetch(sctx, p, off, batches)
				}(po.Partition, po.Offset)
			}
		}
	}
	go func() { fetchers.Wait(); close(batches) }()

	s := &Session{MemberID: resp.MemberId, Generation: resp.Generation, Partitions: resp.Partitions, Batches: batches, c: c}
	if err := h(sctx, s); err != nil && !errors.Is(err, context.Canceled) {
		c.cfg.Log.Warn("session handler returned an error", "err", err)
	}
	cancelSession()
	for range batches { // let the fetch loops exit
	}
	stopHB()
	<-hbDone
	return lost.Load()
}

// heartbeat keeps the membership alive until stopped, and cancels the session
// when the coordinator signals a rebalance or no longer knows this member.
func (c *Consumer) heartbeat(ctx context.Context, resp *queuepb.JoinGroupResponse, cancelSession context.CancelFunc, lost *atomic.Bool) {
	interval := time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
	session := time.Duration(resp.SessionTimeoutMs) * time.Millisecond
	t := time.NewTicker(interval)
	defer t.Stop()
	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		hctx, cancel := context.WithTimeout(ctx, interval)
		r, err := c.coord.Heartbeat(hctx, &queuepb.HeartbeatRequest{Group: c.cfg.Group, MemberId: resp.MemberId, Generation: resp.Generation})
		cancel()
		switch {
		case err != nil:
			if time.Since(lastOK) > session {
				c.cfg.Log.Warn("heartbeats failing for a whole session timeout; rejoining", "err", err)
				lost.Store(true)
				cancelSession()
			}
		case r.Error == queuepb.ErrorCode_OK:
			lastOK = time.Now()
		case r.Error == queuepb.ErrorCode_UNKNOWN_MEMBER:
			lost.Store(true)
			cancelSession()
		default: // REBALANCE_IN_PROGRESS, ILLEGAL_GENERATION
			lastOK = time.Now()
			cancelSession()
		}
	}
}

// fetch streams one partition from off into out until ctx ends, reconnecting
// with backoff.
func (c *Consumer) fetch(ctx context.Context, p uint32, off uint64, out chan<- Batch) {
	attempt := 0
	for ctx.Err() == nil {
		st, err := c.broker.Fetch(ctx, &queuepb.FetchRequest{
			Partition: p, Offset: off, MaxBytes: c.cfg.FetchMaxBytes, MaxWaitMs: uint32(c.cfg.FetchMaxWait.Milliseconds()),
		})
		for err == nil {
			var r *queuepb.FetchResponse
			if r, err = st.Recv(); err != nil {
				break
			}
			attempt = 0
			c.Stats.hw.Store(p, r.HighWatermark)
			if r.SkipTo > 0 {
				c.Stats.Skipped.Add(1)
				if !send(ctx, out, Batch{Partition: p, Offset: off, Next: r.SkipTo, Skipped: true}) {
					return
				}
				off = r.SkipTo
			}
			for _, e := range r.Entries {
				if e.Offset < off {
					continue
				}
				if !send(ctx, out, Batch{Partition: p, Offset: e.Offset, Next: e.Offset + 1, Frame: e.Frame}) {
					return
				}
				off = e.Offset + 1
			}
		}
		if ctx.Err() != nil {
			return
		}
		if status.Code(err) == codes.OutOfRange {
			// The log no longer holds off (or it is ahead of the log): restart
			// from where the group stands.
			if r, ferr := c.coord.FetchOffsets(ctx, &queuepb.FetchOffsetsRequest{Group: c.cfg.Group, Partitions: []uint32{p}}); ferr == nil && len(r.Offsets) == 1 {
				c.cfg.Log.Warn("fetch offset out of range; resetting", "partition", p, "from", off, "to", r.Offsets[0].Offset)
				off = r.Offsets[0].Offset
			}
		}
		attempt++
		c.cfg.Log.Debug("fetch stream ended; reconnecting", "partition", p, "err", err, "attempt", attempt)
		sleepCtx(ctx, c.backoff(attempt))
	}
}

func send(ctx context.Context, out chan<- Batch, b Batch) bool {
	select {
	case out <- b:
		return true
	case <-ctx.Done():
		return false
	}
}
