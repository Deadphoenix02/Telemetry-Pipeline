// Package fetch serves queue.v1.Broker/Fetch: a push stream of fsynced frames
// for one partition.
package fetch

import (
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/queuepb"
)

type Options struct {
	DefaultMaxBytes int
	MaxMaxBytes     int // cap on what a client may ask for (keep under the gRPC message limit)
	DefaultMaxWait  time.Duration
}

type Server struct {
	queuepb.UnimplementedBrokerServer
	opts     Options
	store    *plog.Store
	m        *metrics.Metrics
	stopping <-chan struct{}
}

// New returns a Fetch server; streams end with UNAVAILABLE when stopping closes.
func New(opts Options, store *plog.Store, m *metrics.Metrics, stopping <-chan struct{}) *Server {
	return &Server{opts: opts, store: store, m: m, stopping: stopping}
}

func (s *Server) Fetch(req *queuepb.FetchRequest, st grpc.ServerStreamingServer[queuepb.FetchResponse]) error {
	if int(req.Partition) >= s.store.Partitions() {
		return status.Errorf(codes.InvalidArgument, "partition %d does not exist (have %d)", req.Partition, s.store.Partitions())
	}
	part := s.store.Partition(int(req.Partition))
	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = s.opts.DefaultMaxBytes
	}
	maxBytes = min(maxBytes, s.opts.MaxMaxBytes)
	maxWait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if maxWait <= 0 {
		maxWait = s.opts.DefaultMaxWait
	}
	ctx := st.Context()
	offset := req.Offset
	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	for {
		ents, skip, err := part.Read(offset, maxBytes)
		if errors.Is(err, plog.ErrOutOfRange) {
			return status.Errorf(codes.OutOfRange, "offset %d is outside [%d, %d]", offset, part.First(), part.HW()+1)
		}
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		resp := &queuepb.FetchResponse{Partition: req.Partition, HighWatermark: part.HW(), FirstOffset: part.First()}
		switch {
		case skip > 0:
			resp.SkipTo = skip
			offset = skip
		case len(ents) > 0:
			resp.Entries = make([]*queuepb.Entry, len(ents))
			n := 0
			for i, e := range ents {
				resp.Entries[i] = &queuepb.Entry{Offset: e.Offset, Frame: e.Frame}
				n += len(e.Frame)
			}
			offset = ents[len(ents)-1].Offset + 1
			s.m.FetchFrames.Add(uint64(len(ents)))
			s.m.FetchBytes.Add(uint64(n))
		default:
			// Caught up: wait for this partition's watermark to pass offset,
			// or send an empty response carrying the watermark at max_wait.
			if !s.waitFor(ctx, part, offset, timer, maxWait) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-s.stopping:
					return status.Error(codes.Unavailable, "broker is shutting down")
				default:
				}
				resp.HighWatermark, resp.FirstOffset = part.HW(), part.First()
				if err := st.Send(resp); err != nil {
					return err
				}
			}
			continue
		}
		if err := st.Send(resp); err != nil {
			return err
		}
	}
}

// waitFor blocks until the partition's high watermark reaches offset (true),
// or maxWait passes, the client goes away or the broker stops (false).
func (s *Server) waitFor(ctx interface{ Done() <-chan struct{} }, part *plog.Partition, offset uint64,
	timer *time.Timer, maxWait time.Duration) bool {
	timer.Reset(maxWait)
	defer timer.Stop()
	for {
		hwc := s.store.HWChanged()
		if part.HW() >= offset {
			return true
		}
		select {
		case <-hwc:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		case <-s.stopping:
			return false
		}
	}
}
