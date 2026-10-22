// Package ingest serves telemetry.v1.Ingest/Stream, the streamers' existing
// per-record protocol. The broker partitions each record by GPU UUID, drops
// producer retries, packs records into one frame per partition, appends the
// frames to the WAL, and sends a cumulative Ack only once every record up to
// that sequence is fsynced.
package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"broker/internal/ingestpb"
	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
	"broker/pkg/partitioner"
)

const (
	headerStreamerID = "x-streamer-id"
	headerEpoch      = "x-epoch"
	headerProducerID = "x-producer-id" // free text, logged only
)

type Options struct {
	Linger          time.Duration // how long records wait for more before their frames are written
	MaxFrameBytes   int           // a partition's frame is written as soon as it reaches this size
	MaxProducers    int           // dedupe table entries kept
	ProducerIdleTTL time.Duration // idle entries older than this are dropped first
	RebuildTail     uint64        // entries per partition scanned at start to rebuild the dedupe table
	DrainTimeout    time.Duration // at shutdown, how long a stream may wait for its last acks
}

type Server struct {
	ingestpb.UnimplementedIngestServer
	opts  Options
	store *plog.Store
	ps    *producers
	m     *metrics.Metrics
	log   *slog.Logger

	stopOnce sync.Once
	stopping chan struct{}
	streams  sync.WaitGroup
}

func New(opts Options, store *plog.Store, m *metrics.Metrics, log *slog.Logger) *Server {
	ps := newProducers(store.Partitions(), opts.MaxProducers, opts.ProducerIdleTTL)
	ps.rebuild(store, opts.RebuildTail)
	log.Info("producer table rebuilt from the log", "producers", ps.len())
	return &Server{opts: opts, store: store, ps: ps, m: m, log: log, stopping: make(chan struct{})}
}

// Drain makes every stream write its buffered records, wait (up to
// DrainTimeout) for their acks, and close with UNAVAILABLE so streamers
// reconnect to the restarted broker. New streams are refused. Drain returns
// when all streams have exited or ctx ends.
func (s *Server) Drain(ctx context.Context) {
	s.stopOnce.Do(func() { close(s.stopping) })
	done := make(chan struct{})
	go func() { s.streams.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Producers is the size of the dedupe table, for metrics.
func (s *Server) Producers() int { return s.ps.len() }

func parseProducer(ctx context.Context) (producerKey, string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	sid, err1 := strconv.ParseUint(get(headerStreamerID), 10, 16)
	ep, err2 := strconv.ParseUint(get(headerEpoch), 10, 32)
	if err1 != nil || err2 != nil {
		return producerKey{}, "", status.Errorf(codes.InvalidArgument,
			"metadata %s (0..65535) and %s (0..2^32-1) are required", headerStreamerID, headerEpoch)
	}
	return producerKey{streamer: uint16(sid), epoch: uint32(ep)}, get(headerProducerID), nil
}

func (s *Server) Stream(st grpc.BidiStreamingServer[ingestpb.Record, ingestpb.Ack]) error {
	select {
	case <-s.stopping:
		return status.Error(codes.Unavailable, "broker is shutting down")
	default:
	}
	key, name, err := parseProducer(st.Context())
	if err != nil {
		return err
	}
	s.streams.Add(1)
	defer s.streams.Done()

	ctx, cancel := context.WithCancel(st.Context())
	defer cancel()
	prod, gen, fenced := s.ps.register(key, cancel, time.Now())
	defer func() { s.ps.release(prod, gen, time.Now()) }()
	if fenced {
		s.m.StreamsFenced.Add(1)
	}
	prod.owner.Lock() // wait for the stream this one replaces to finish
	defer prod.owner.Unlock()
	if ctx.Err() != nil {
		return status.Error(codes.Aborted, "superseded by a newer stream from the same producer")
	}
	s.m.StreamsOpened.Add(1)
	log := s.log.With("streamer_id", key.streamer, "epoch", key.epoch, "producer", name)
	log.Info("producer stream opened")

	ss := &session{
		s: s, st: st, prod: prod, key: key, log: log,
		bufs: make([]frame.Builder, s.store.Partitions()),
		need: make([]uint64, s.store.Partitions()),
	}
	err = ss.run(ctx)
	log.Info("producer stream closed", "err", err)
	return err
}

// mark is one ack point: Ack{seq} may be sent once every partition p has
// fsynced up to need[p].
type mark struct {
	seq  uint64
	need []uint64
	at   time.Time
}

type session struct {
	s    *Server
	st   grpc.BidiStreamingServer[ingestpb.Record, ingestpb.Ack]
	prod *producer
	key  producerKey
	log  *slog.Logger

	bufs []frame.Builder // one frame being filled per partition
	need []uint64        // per partition: offsets the pending records depend on

	started bool   // a record has been seen on this stream
	prevSeq uint64 // last sequence seen on this stream

	pending    bool // records received since the last mark
	pendingSeq uint64
	pendingAt  time.Time

	marks []mark
}

func (ss *session) run(ctx context.Context) error {
	recCh := make(chan *ingestpb.Record, 256)
	errCh := make(chan error, 1)
	go func() {
		for {
			r, err := ss.st.Recv()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case recCh <- r:
			case <-ctx.Done():
				return
			}
		}
	}()

	linger := time.NewTimer(time.Hour)
	linger.Stop()
	lingering := false
	for {
		hwc := ss.s.store.HWChanged() // before ack(), so no advance is missed
		if err := ss.ack(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			if ss.st.Context().Err() == nil { // cancelled by register, not by the client
				return status.Error(codes.Aborted, "superseded by a newer stream from the same producer")
			}
			return ctx.Err()
		case <-ss.s.stopping:
			return ss.shutdown()
		case r := <-recCh:
			if err := ss.add(ctx, r); err != nil {
				return err
			}
			if ss.pending && !lingering {
				linger.Reset(ss.s.opts.Linger)
				lingering = true
			}
		case <-linger.C:
			lingering = false
			if err := ss.flush(ctx); err != nil {
				return err
			}
		case <-hwc:
		case err := <-errCh:
			// every record the receiver read came before its error
			for drained := false; !drained; {
				select {
				case r := <-recCh:
					if err := ss.add(ctx, r); err != nil {
						return err
					}
				default:
					drained = true
				}
			}
			if !errors.Is(err, io.EOF) {
				return err // the client is gone; unacked records will be resent
			}
			// the client half-closed: write what is buffered and ack it
			if err := ss.flush(ctx); err != nil {
				return err
			}
			return ss.waitAcks(ctx)
		}
	}
}

// add validates one record, drops it if it is a retry of a record already in
// the log, and otherwise buffers it in its partition's frame.
func (ss *session) add(ctx context.Context, r *ingestpb.Record) error {
	if ss.started && r.Seq != ss.prevSeq+1 {
		ss.s.m.SequenceGaps.Add(1)
		return status.Errorf(codes.InvalidArgument, "sequence gap: got %d after %d", r.Seq, ss.prevSeq)
	}
	ss.started, ss.prevSeq = true, r.Seq
	if !ss.pending {
		ss.pending, ss.pendingAt = true, time.Now()
	}
	ss.pendingSeq = r.Seq

	u, err := parseUUID(r.Uuid)
	if err != nil || len(r.MetricName) > frame.MaxMetricLen {
		if n := ss.s.m.RecordsInvalid.Add(1); n%1000 == 1 {
			ss.log.Warn("dropping invalid record", "seq", r.Seq, "uuid", r.Uuid, "metric_len", len(r.MetricName), "dropped_total", n)
		}
		return nil // acked like any other record: resending it cannot help
	}
	p := partitioner.Partition(u, len(ss.bufs))
	if r.Seq < ss.prod.next[p] {
		// The original is in the log at or below Last; ack only once that is fsynced.
		ss.s.m.RecordsDuplicate.Add(1)
		if l := ss.s.store.Partition(p).Last(); l > ss.need[p] {
			ss.need[p] = l
		}
		return nil
	}
	b := &ss.bufs[p]
	if b.Count() == 0 {
		b.Reset(uint16(p), ss.key.streamer, ss.key.epoch)
	}
	rec := frame.Record{TsNanos: r.TimestampNs, UUID: u, Metric: r.MetricName, Value: r.Value}
	_ = b.Add(r.Seq, &rec) // metric length checked above
	if b.Len() >= ss.s.opts.MaxFrameBytes {
		return ss.appendPartition(ctx, p)
	}
	return nil
}

func (ss *session) appendPartition(ctx context.Context, p int) error {
	b := &ss.bufs[p]
	f, count, lastSeq := b.Bytes(), b.Count(), b.LastSeq()
	b.Reset(uint16(p), ss.key.streamer, ss.key.epoch)
	idx, err := ss.s.store.Append(ctx, p, f)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ss.log.Error("append failed", "partition", p, "err", err)
		return status.Error(codes.Internal, "write-ahead log append failed")
	}
	// Only now is the batch in the log; a stream that dies before this point
	// leaves next unchanged and the producer's resend is appended.
	ss.prod.next[p] = lastSeq + 1
	ss.s.m.RecordsAccepted.Add(uint64(count))
	if idx > ss.need[p] {
		ss.need[p] = idx
	}
	return nil
}

// flush writes every partition's frame and closes the pending records into a
// mark.
func (ss *session) flush(ctx context.Context) error {
	for p := range ss.bufs {
		if ss.bufs[p].Count() > 0 {
			if err := ss.appendPartition(ctx, p); err != nil {
				return err
			}
		}
	}
	if !ss.pending {
		return nil
	}
	ss.marks = append(ss.marks, mark{seq: ss.pendingSeq, need: append([]uint64(nil), ss.need...), at: ss.pendingAt})
	clear(ss.need)
	ss.pending = false
	return nil
}

// ack sends one cumulative Ack for the longest prefix of marks that is now fsynced.
func (ss *session) ack() error {
	n := 0
	for n < len(ss.marks) && ss.durable(&ss.marks[n]) {
		n++
	}
	if n == 0 {
		return nil
	}
	now := time.Now()
	for i := 0; i < n; i++ {
		ss.s.m.AckLatency.Observe(now.Sub(ss.marks[i].at))
	}
	seq := ss.marks[n-1].seq
	ss.marks = ss.marks[n:]
	return ss.st.Send(&ingestpb.Ack{AckedUpToSeq: seq})
}

func (ss *session) durable(m *mark) bool {
	for p, need := range m.need {
		if need > 0 && ss.s.store.Partition(p).HW() < need {
			return false
		}
	}
	return true
}

// waitAcks blocks until every mark is acked or ctx ends.
func (ss *session) waitAcks(ctx context.Context) error {
	for {
		hwc := ss.s.store.HWChanged()
		if err := ss.ack(); err != nil {
			return err
		}
		if len(ss.marks) == 0 {
			return nil
		}
		select {
		case <-hwc:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ss *session) shutdown() error {
	ctx, cancel := context.WithTimeout(ss.st.Context(), ss.s.opts.DrainTimeout)
	defer cancel()
	if err := ss.flush(ctx); err != nil {
		ss.log.Warn("shutdown: could not write buffered records; the producer will resend them", "err", err)
	} else if err := ss.waitAcks(ctx); err != nil {
		ss.log.Warn("shutdown: acks not complete; the producer will resend", "err", err)
	}
	return status.Error(codes.Unavailable, "broker is shutting down")
}
