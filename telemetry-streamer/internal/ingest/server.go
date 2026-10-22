package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"telemetry-streamer/internal/wire"
)

// IngestServer is the server-side interface of the Ingest service.
type IngestServer interface {
	Stream(grpc.ServerStream) error
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "telemetry.v1.Ingest",
	HandlerType: (*IngestServer)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       func(srv any, st grpc.ServerStream) error { return srv.(IngestServer).Stream(st) },
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "proto/ingest.proto",
}

func RegisterIngestServer(s grpc.ServiceRegistrar, srv IngestServer) {
	s.RegisterService(&serviceDesc, srv)
}

// ServerConfig tunes the fake server.
type ServerConfig struct {
	AckInterval time.Duration // how often cumulative acks are sent
	KillEvery   uint64        // fail the stream after every Nth record received (0 = never)
	PrintEvery  uint64        // log every Nth record received (0 = never)
	Log         *slog.Logger
}

// Server is a fake ingest service: it "enqueues" records by counting them,
// dedupes on (producer, seq), and acks cumulatively.
type Server struct {
	cfg  ServerConfig
	mu   sync.Mutex
	prod map[string]*producerState

	Received atomic.Uint64 // every record seen, including resends
	Accepted atomic.Uint64 // new records handed to the "queue"
	Dups     atomic.Uint64 // resends already accepted
	Gaps     atomic.Uint64 // out-of-order records (should stay 0)
	Kills    atomic.Uint64 // injected stream failures
}

type producerState struct {
	mu    sync.Mutex
	last  int64 // highest accepted seq, -1 = none
	dirty atomic.Bool
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{cfg: cfg, prod: map[string]*producerState{}}
}

func (s *Server) producer(id string) *producerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := s.prod[id]
	if ps == nil {
		ps = &producerState{last: -1}
		s.prod[id] = ps
	}
	return ps
}

func (s *Server) accept(ps *producerState, r *wire.Record) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	switch seq := int64(r.Seq); {
	case ps.last >= 0 && seq <= ps.last:
		s.Dups.Add(1)
	case ps.last < 0 || seq == ps.last+1:
		// Like the real broker, a producer's first record here may have any
		// sequence: after a failover a streamer arrives mid-sequence.
		ps.last = seq
		s.Accepted.Add(1) // <- a real server would enqueue the record here
	default:
		s.Gaps.Add(1)
	}
}

func (s *Server) Stream(stream grpc.ServerStream) error {
	ctx := stream.Context()
	producer := "unknown"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(producerIDHeader); len(v) > 0 {
			producer = v[0]
		}
	}
	ps := s.producer(producer)
	s.cfg.Log.Info("stream opened", "producer", producer)

	done := make(chan struct{})
	defer close(done)
	go s.ackLoop(ctx, stream, ps, done)

	for {
		var r wire.Record
		if err := stream.RecvMsg(&r); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		s.accept(ps, &r)
		ps.dirty.Store(true)
		n := s.Received.Add(1)
		if s.cfg.PrintEvery > 0 && n%s.cfg.PrintEvery == 0 {
			s.cfg.Log.Info("record", "producer", producer, "seq", r.Seq, "ts", r.TimestampNanos,
				"metric", r.Metric, "gpu_id", r.GPUID, "device", r.Device, "uuid", r.UUID,
				"model", r.ModelName, "host", r.Hostname, "container", r.Container,
				"pod", r.Pod, "namespace", r.Namespace, "value", r.Value)
		}
		if s.cfg.KillEvery > 0 && n%s.cfg.KillEvery == 0 {
			s.Kills.Add(1)
			return status.Error(codes.Unavailable, "fake server: injected stream failure")
		}
	}
}

// ackLoop sends a cumulative ack every AckInterval if anything arrived since
// the last one (resends count, so a replayed window is always re-acked).
func (s *Server) ackLoop(ctx context.Context, stream grpc.ServerStream, ps *producerState, done <-chan struct{}) {
	t := time.NewTicker(s.cfg.AckInterval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			if !ps.dirty.Swap(false) {
				continue
			}
			ps.mu.Lock()
			last := ps.last
			ps.mu.Unlock()
			if last < 0 {
				continue
			}
			if err := stream.SendMsg(&wire.Ack{Seq: uint64(last)}); err != nil {
				return
			}
		}
	}
}
