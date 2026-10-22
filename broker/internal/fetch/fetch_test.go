package fetch

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
	"broker/pkg/queuepb"
)

func setup(t *testing.T) (*plog.Store, queuepb.BrokerClient, chan struct{}) {
	t.Helper()
	m := metrics.New()
	store, err := plog.Open(plog.Options{Dir: t.TempDir(), Partitions: 2, SegmentSize: 1 << 16, FlushInterval: time.Hour,
		TruncateInterval: time.Hour, QuotaBytes: 1 << 30}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stopping := make(chan struct{})
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	queuepb.RegisterBrokerServer(gs, New(Options{DefaultMaxBytes: 1 << 16, MaxMaxBytes: 1 << 20, DefaultMaxWait: 50 * time.Millisecond}, store, m, stopping))
	go gs.Serve(lis)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close(); gs.Stop(); store.Close() })
	return store, queuepb.NewBrokerClient(cc), stopping
}

func appendFrames(t *testing.T, s *plog.Store, p, n int) {
	for i := 0; i < n; i++ {
		var b frame.Builder
		b.Reset(uint16(p), 0, 0)
		_ = b.Add(uint64(i), &frame.Record{Metric: "m"})
		if _, err := s.Append(context.Background(), p, b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFetchStreamsOnlyFsyncedEntriesAndLongPolls(t *testing.T) {
	store, cl, _ := setup(t)
	appendFrames(t, store, 1, 3)
	store.Flush()
	appendFrames(t, store, 1, 2) // written, not fsynced
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := cl.Fetch(ctx, &queuepb.FetchRequest{Partition: 1, Offset: 1, MaxWaitMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.Recv()
	if err != nil || len(r.Entries) != 3 || r.HighWatermark != 3 || r.Entries[0].Offset != 1 {
		t.Fatalf("first response: %+v err=%v", r, err)
	}
	for _, e := range r.Entries {
		if _, err := frame.Verify(e.Frame); err != nil {
			t.Fatalf("entry %d: %v", e.Offset, err)
		}
	}
	// caught up: an empty response with the watermark after max_wait
	start := time.Now()
	r, err = st.Recv()
	if err != nil || len(r.Entries) != 0 || r.HighWatermark != 3 || time.Since(start) < 30*time.Millisecond {
		t.Fatalf("idle response: %+v err=%v after %v", r, err, time.Since(start))
	}
	// the flush wakes the waiting stream with the next entries
	go func() { time.Sleep(10 * time.Millisecond); store.Flush() }()
	for {
		r, err = st.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Entries) > 0 {
			break
		}
	}
	if r.Entries[0].Offset != 4 || len(r.Entries) != 2 {
		t.Fatalf("after flush: %+v", r)
	}
}

func TestFetchOutOfRangeAndBadPartition(t *testing.T) {
	store, cl, _ := setup(t)
	appendFrames(t, store, 0, 5)
	store.Flush()
	store.RequestTruncate(0, 3)
	store.TruncateNow()
	for _, off := range []uint64{1, 7} {
		st, _ := cl.Fetch(context.Background(), &queuepb.FetchRequest{Partition: 0, Offset: off})
		if _, err := st.Recv(); status.Code(err) != codes.OutOfRange {
			t.Fatalf("offset %d: %v, want OutOfRange", off, err)
		}
	}
	st, _ := cl.Fetch(context.Background(), &queuepb.FetchRequest{Partition: 9})
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad partition: %v", err)
	}
}

func TestFetchEndsOnShutdown(t *testing.T) {
	_, cl, stopping := setup(t)
	st, _ := cl.Fetch(context.Background(), &queuepb.FetchRequest{Partition: 0, Offset: 1, MaxWaitMs: 10_000})
	time.Sleep(20 * time.Millisecond)
	close(stopping)
	if _, err := st.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("after shutdown: %v", err)
	}
}
