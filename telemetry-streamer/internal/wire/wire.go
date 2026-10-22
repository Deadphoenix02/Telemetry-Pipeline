// Package wire defines the transport-agnostic types and interfaces the streamer
// talks to. The gRPC implementation lives in internal/ingest.
package wire

import "context"

// Record is ONE CSV row on the stream (no batching). TimestampNanos is assigned
// once, when the row is packed, and never modified (resends reuse it). The CSV's
// own timestamp is not carried.
type Record struct {
	Seq            uint64 // per-producer sequence, starts at 0
	TimestampNanos int64
	Metric         string
	GPUID          string
	Device         string
	UUID           string
	ModelName      string
	Hostname       string
	Container      string
	Pod            string
	Namespace      string
	Value          float64
}

// Ack is cumulative: it acknowledges every record with Seq <= Ack.Seq.
type Ack struct{ Seq uint64 }

// Stream is one ordered, bidirectional connection. Send and Recv must unblock
// (with an error) when the stream is closed or the ctx given to OpenStream is
// cancelled. One goroutine may Send while another Recvs.
type Stream interface {
	Send(Record) error
	Recv() (Ack, error)
	Close() error
}

type Transport interface {
	OpenStream(ctx context.Context, producerID string) (Stream, error)
}
