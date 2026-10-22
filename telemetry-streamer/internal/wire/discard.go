package wire

import (
	"context"
	"io"
	"log/slog"
	"sync"
)

// Discard is used when no -target is set: it drops every record, acks it
// immediately, and (optionally) prints one line per record.
type Discard struct {
	Verbose bool
	Log     *slog.Logger
}

func (d *Discard) OpenStream(ctx context.Context, producerID string) (Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &discardStream{d: d, acks: make(chan Ack, 4096), done: make(chan struct{})}, nil
}

type discardStream struct {
	d    *Discard
	acks chan Ack
	done chan struct{}
	once sync.Once
}

func (s *discardStream) Send(r Record) error {
	if s.d.Verbose {
		s.d.Log.Info("record", "seq", r.Seq, "ts", r.TimestampNanos, "metric", r.Metric,
			"gpu_id", r.GPUID, "uuid", r.UUID, "host", r.Hostname, "value", r.Value)
	}
	select {
	case s.acks <- Ack{Seq: r.Seq}:
		return nil
	case <-s.done:
		return io.EOF
	}
}

func (s *discardStream) Recv() (Ack, error) {
	select {
	case a := <-s.acks:
		return a, nil
	case <-s.done:
		return Ack{}, io.EOF
	}
}

func (s *discardStream) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}
