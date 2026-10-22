// Package brokertest runs a real in-process broker (log, ingest, fetch and
// coordinator) on a loopback port, for integration tests of consumers such as
// the collector. Timings are shortened so rebalances take milliseconds.
package brokertest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"broker/internal/coordinator"
	"broker/internal/fetch"
	"broker/internal/ingest"
	"broker/internal/ingestpb"
	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
	"broker/pkg/partitioner"
	"broker/pkg/queuepb"
)

// Group is the consumer group the test broker coordinates.
const Group = "collectors"

type Broker struct {
	Addr       string // host:port of the gRPC server
	Partitions int

	t        testing.TB
	store    *plog.Store
	coord    *coordinator.Coordinator
	gs       *grpc.Server
	stopping chan struct{}
}

// Start serves a broker with the given partition count until the test ends.
func Start(t testing.TB, partitions int) *Broker {
	t.Helper()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	store, err := plog.Open(plog.Options{Dir: dir, Partitions: partitions, SegmentSize: 1 << 16,
		FlushInterval: 2 * time.Millisecond, TruncateInterval: 10 * time.Millisecond, QuotaBytes: 1 << 30}, m, log)
	if err != nil {
		t.Fatal(err)
	}
	store.Start()
	coord, err := coordinator.New(coordinator.Options{Group: Group, OffsetsPath: filepath.Join(dir, "offsets.json"),
		SessionTimeout: time.Second, HeartbeatInterval: 50 * time.Millisecond, JoinWindow: 30 * time.Millisecond,
		RebalanceTimeout: 2 * time.Second, CommitInterval: 2 * time.Millisecond, Tick: 5 * time.Millisecond}, store, m, log)
	if err != nil {
		t.Fatal(err)
	}
	coord.Start()
	b := &Broker{Partitions: partitions, t: t, store: store, coord: coord, gs: grpc.NewServer(), stopping: make(chan struct{})}
	ingestpb.RegisterIngestServer(b.gs, ingest.New(ingest.Options{Linger: time.Millisecond, MaxFrameBytes: 1 << 16,
		MaxProducers: 64, ProducerIdleTTL: time.Hour, RebuildTail: 1024, DrainTimeout: time.Second}, store, m, log))
	queuepb.RegisterBrokerServer(b.gs, fetch.New(fetch.Options{DefaultMaxBytes: 1 << 16, MaxMaxBytes: 1 << 20,
		DefaultMaxWait: 50 * time.Millisecond}, store, m, b.stopping))
	queuepb.RegisterCoordinatorServer(b.gs, coord)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.Addr = lis.Addr().String()
	go b.gs.Serve(lis)
	t.Cleanup(b.Close)
	return b
}

// Close stops the broker; it is also registered with t.Cleanup.
func (b *Broker) Close() {
	select {
	case <-b.stopping:
		return
	default:
	}
	close(b.stopping)
	b.gs.Stop()
	_ = b.coord.Close()
	_ = b.store.Close()
}

// Produce appends recs as producer (streamerID, epoch) with sequences from
// firstSeq, routed by GPU UUID exactly as the ingest path does, and fsyncs.
func (b *Broker) Produce(streamerID uint16, epoch uint32, firstSeq uint64, recs []frame.Record) {
	b.t.Helper()
	builders := make([]frame.Builder, b.Partitions)
	for i := range recs {
		p := partitioner.Partition(recs[i].UUID, b.Partitions)
		if builders[p].Count() == 0 {
			builders[p].Reset(uint16(p), streamerID, epoch)
		}
		if err := builders[p].Add(firstSeq+uint64(i), &recs[i]); err != nil {
			b.t.Fatal(err)
		}
	}
	for p := range builders {
		if builders[p].Count() > 0 {
			b.AppendRaw(p, builders[p].Bytes())
		}
	}
	b.store.Flush()
}

// AppendRaw appends an unsealed frame to partition p as-is (the broker seals
// it). Tests use it to plant frames whose records do not decode.
func (b *Broker) AppendRaw(p int, f []byte) {
	b.t.Helper()
	if _, err := b.store.Append(context.Background(), p, f); err != nil {
		b.t.Fatal(err)
	}
	b.store.Flush()
}

// Lag is the group's uncommitted records across all partitions.
func (b *Broker) Lag() uint64 { return b.coord.Lag().Lag }

// FirstOffsets is the first offset still on disk per partition: it moves up as
// committed entries are deleted.
func (b *Broker) FirstOffsets() []uint64 {
	out := make([]uint64, b.Partitions)
	for p := range out {
		out[p] = b.store.Partition(p).First()
	}
	return out
}
