package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"telemetry-streamer/internal/wire"
)

type serverTestStream struct {
	ctx      context.Context
	requests []any
	acks     []*wire.Ack
}

func (s *serverTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *serverTestStream) SendHeader(metadata.MD) error { return nil }
func (s *serverTestStream) SetTrailer(metadata.MD)       {}
func (s *serverTestStream) Context() context.Context     { return s.ctx }
func (s *serverTestStream) SendMsg(message any) error {
	ack, ok := message.(*wire.Ack)
	if !ok {
		return errors.New("unexpected server response type")
	}
	copy := *ack
	s.acks = append(s.acks, &copy)
	return nil
}
func (s *serverTestStream) RecvMsg(message any) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	request := s.requests[0]
	s.requests = s.requests[1:]
	if err, ok := request.(error); ok {
		return err
	}
	*message.(*wire.Record) = *request.(*wire.Record)
	return nil
}

func serverTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestServerStreamCountsDuplicatesGapsAndReceiveErrors(t *testing.T) {
	receiveErr := errors.New("receive broke")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(producerIDHeader, "test-producer"))
	stream := &serverTestStream{ctx: ctx, requests: []any{
		&wire.Record{Seq: 0}, &wire.Record{Seq: 1}, &wire.Record{Seq: 1}, &wire.Record{Seq: 3}, receiveErr,
	}}
	srv := NewServer(ServerConfig{AckInterval: time.Millisecond, PrintEvery: 1, Log: serverTestLogger()})
	if err := srv.Stream(stream); err != receiveErr {
		t.Fatalf("Stream error = %v, want receive error", err)
	}
	if srv.Received.Load() != 4 || srv.Accepted.Load() != 2 || srv.Dups.Load() != 1 || srv.Gaps.Load() != 1 {
		t.Fatalf("received=%d accepted=%d dups=%d gaps=%d", srv.Received.Load(), srv.Accepted.Load(), srv.Dups.Load(), srv.Gaps.Load())
	}
	if state := srv.producer("test-producer"); state.last != 1 {
		t.Fatalf("last accepted seq = %d, want 1", state.last)
	}
}

func TestServerStreamReturnsEOFAndCanInjectKill(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(producerIDHeader, "kill-producer"))
	srv := NewServer(ServerConfig{AckInterval: time.Millisecond, Log: serverTestLogger()})
	if err := srv.Stream(&serverTestStream{ctx: ctx}); err != nil {
		t.Fatalf("empty stream error = %v", err)
	}

	killer := NewServer(ServerConfig{AckInterval: time.Millisecond, KillEvery: 1, Log: serverTestLogger()})
	stream := &serverTestStream{ctx: ctx, requests: []any{&wire.Record{Seq: 0}}}
	err := killer.Stream(stream)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("injected kill status = %v, want Unavailable", err)
	}
	if killer.Kills.Load() != 1 || killer.Accepted.Load() != 1 {
		t.Fatalf("kills=%d accepted=%d", killer.Kills.Load(), killer.Accepted.Load())
	}
}

// After a failover a streamer reaches this server mid-sequence: its first
// record may have any sequence; only later ones must follow on.
func TestServerAcceptsAProducerArrivingMidSequence(t *testing.T) {
	s := NewServer(ServerConfig{})
	ps := s.producer("p")
	for _, seq := range []uint64{500, 501, 501, 503} {
		s.accept(ps, &wire.Record{Seq: seq})
	}
	if s.Accepted.Load() != 2 || s.Dups.Load() != 1 || s.Gaps.Load() != 1 {
		t.Fatalf("accepted=%d dups=%d gaps=%d, want 2, 1, 1", s.Accepted.Load(), s.Dups.Load(), s.Gaps.Load())
	}
}
