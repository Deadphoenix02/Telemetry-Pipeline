package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"broker/internal/ingestpb"
	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type harness struct {
	t     *testing.T
	dir   string
	store *plog.Store
	srv   *Server
	m     *metrics.Metrics
	gs    *grpc.Server
	cc    *grpc.ClientConn
}

func storeOpts(dir string) plog.Options {
	return plog.Options{Dir: dir, Partitions: 4, SegmentSize: 1 << 16, FlushInterval: time.Hour,
		TruncateInterval: time.Hour, QuotaBytes: 1 << 30}
}

func srvOpts() Options {
	return Options{Linger: time.Millisecond, MaxFrameBytes: 1 << 10, MaxProducers: 16,
		ProducerIdleTTL: time.Hour, RebuildTail: 1024, DrainTimeout: time.Second}
}

// start opens the store in dir and serves Ingest over an in-memory listener.
// The flusher does not run on its own: tests call h.store.Flush to make data durable.
func start(t *testing.T, dir string) *harness {
	t.Helper()
	m := metrics.New()
	store, err := plog.Open(storeOpts(dir), m, quiet)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, dir: dir, store: store, m: m, srv: New(srvOpts(), store, m, quiet)}
	lis := bufconn.Listen(1 << 20)
	h.gs = grpc.NewServer()
	ingestpb.RegisterIngestServer(h.gs, h.srv)
	go h.gs.Serve(lis)
	h.cc, err = grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) stop() {
	_ = h.cc.Close()
	h.gs.Stop()
	_ = h.store.Close()
}

func (h *harness) open(ctx context.Context, streamer, epoch int) grpc.BidiStreamingClient[ingestpb.Record, ingestpb.Ack] {
	h.t.Helper()
	ctx = metadata.AppendToOutgoingContext(ctx, headerStreamerID, fmt.Sprint(streamer), headerEpoch, fmt.Sprint(epoch))
	st, err := ingestpb.NewIngestClient(h.cc).Stream(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// gpu returns a well-formed UUID string; distinct i give distinct GPUs.
func gpu(i int) string { return fmt.Sprintf("GPU-%08x-0000-4000-8000-%012x", uint32(i)*2654435761, i) }

func rec(seq uint64) *ingestpb.Record {
	return &ingestpb.Record{Seq: seq, TimestampNs: int64(seq) + 1, MetricName: "DCGM_FI_DEV_GPU_UTIL", Uuid: gpu(int(seq % 37)), Value: float64(seq)}
}

func send(t *testing.T, st grpc.BidiStreamingClient[ingestpb.Record, ingestpb.Ack], from, to uint64) {
	t.Helper()
	for s := from; s < to; s++ {
		if err := st.Send(rec(s)); err != nil {
			t.Fatal(err)
		}
	}
}

// waitAck flushes the log until an ack covering seq arrives.
func (h *harness) waitAck(st grpc.BidiStreamingClient[ingestpb.Record, ingestpb.Ack], seq uint64) {
	h.t.Helper()
	acks := make(chan uint64, 64)
	errc := make(chan error, 1)
	go func() {
		for {
			a, err := st.Recv()
			if err != nil {
				errc <- err
				return
			}
			acks <- a.AckedUpToSeq
			if a.AckedUpToSeq >= seq {
				return
			}
		}
	}()
	deadline := time.After(5 * time.Second)
	for {
		h.store.Flush()
		select {
		case a := <-acks:
			if a >= seq {
				return
			}
		case err := <-errc:
			h.t.Fatalf("waiting for ack %d: %v", seq, err)
		case <-deadline:
			h.t.Fatalf("no ack for seq %d", seq)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// records counts records in the log of every partition and checks each frame.
func (h *harness) records() int {
	h.t.Helper()
	h.store.Flush()
	n := 0
	for p := 0; p < h.store.Partitions(); p++ {
		part := h.store.Partition(p)
		ents, _, err := part.Read(part.First(), 1<<30)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, e := range ents {
			hd, err := frame.Decode(e.Frame, nil, func(*frame.Record) error { return nil })
			if err != nil || int(hd.Partition) != p {
				h.t.Fatalf("partition %d entry %d: header %+v err %v", p, e.Offset, hd, err)
			}
			n += int(hd.RecordCount)
		}
	}
	return n
}

func TestAckOnlyAfterFsync(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	st := h.open(context.Background(), 1, 7)
	send(t, st, 0, 50)
	got := make(chan uint64, 1)
	go func() {
		if a, err := st.Recv(); err == nil {
			got <- a.AckedUpToSeq
		}
	}()
	select {
	case a := <-got:
		t.Fatalf("ack %d arrived before any fsync", a)
	case <-time.After(50 * time.Millisecond):
	}
	h.store.Flush()
	select {
	case a := <-got:
		if a < 49 { // records that arrived after the first flush are acked later
			h.waitAck(st, 49)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no ack after fsync")
	}
	if n := h.records(); n != 50 {
		t.Fatalf("log holds %d records, want 50", n)
	}
	used := 0
	for p := 0; p < 4; p++ {
		if h.store.Partition(p).Last() > 0 {
			used++
		}
	}
	if used < 3 {
		t.Fatalf("records reached only %d of 4 partitions", used)
	}
}

func TestResendAfterReconnectIsDeduplicated(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	ctx, cancel := context.WithCancel(context.Background())
	st := h.open(ctx, 1, 7)
	send(t, st, 0, 100)
	h.waitAck(st, 99)
	cancel() // the streamer drops the stream; it resends from its oldest unacked record

	st2 := h.open(context.Background(), 1, 7)
	send(t, st2, 60, 150)
	h.waitAck(st2, 149)
	if n := h.records(); n != 150 {
		t.Fatalf("log holds %d records, want 150", n)
	}
	if d := h.m.RecordsDuplicate.Load(); d != 40 {
		t.Fatalf("duplicates = %d, want 40", d)
	}
	// a new epoch is a new producer: sequence 0 is accepted again
	st3 := h.open(context.Background(), 1, 8)
	send(t, st3, 0, 10)
	h.waitAck(st3, 9)
	if n := h.records(); n != 160 {
		t.Fatalf("log holds %d records, want 160", n)
	}
}

func TestSequenceGapClosesStream(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	st := h.open(context.Background(), 2, 1)
	send(t, st, 0, 3)
	_ = st.Send(rec(5))
	for {
		if _, err := st.Recv(); err != nil {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("err = %v, want InvalidArgument", err)
			}
			break
		}
	}
	if h.m.SequenceGaps.Load() != 1 {
		t.Fatal("gap not counted")
	}
}

func TestNewStreamFencesOldOne(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	old := h.open(context.Background(), 3, 1)
	send(t, old, 0, 5)
	h.waitAck(old, 4)
	nu := h.open(context.Background(), 3, 1)
	send(t, nu, 5, 10)
	for {
		if _, err := old.Recv(); err != nil {
			if status.Code(err) != codes.Aborted {
				t.Fatalf("old stream: %v, want Aborted", err)
			}
			break
		}
	}
	h.waitAck(nu, 9)
	if h.m.StreamsFenced.Load() != 1 {
		t.Fatal("fencing not counted")
	}
}

func TestMissingHeadersRejected(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	st, err := ingestpb.NewIngestClient(h.cc).Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Send(rec(0))
	if _, err := st.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestInvalidRecordsAreDroppedButAcked(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	st := h.open(context.Background(), 4, 1)
	send(t, st, 0, 2)
	_ = st.Send(&ingestpb.Record{Seq: 2, Uuid: "not-a-uuid", MetricName: "m"})
	long := make([]byte, 300)
	_ = st.Send(&ingestpb.Record{Seq: 3, Uuid: gpu(1), MetricName: string(long)})
	send(t, st, 4, 6)
	h.waitAck(st, 5)
	if h.m.RecordsInvalid.Load() != 2 || h.records() != 4 {
		t.Fatalf("invalid=%d records=%d", h.m.RecordsInvalid.Load(), h.records())
	}
}

// Picture 5 of the plan: after a crash cuts the tail of one partition, the
// producer's resend must restore exactly the lost records and nothing else.
func TestCrashLosingOnePartitionTailIsRepairedByResend(t *testing.T) {
	dir := t.TempDir()
	h := start(t, dir)
	st := h.open(context.Background(), 5, 9)
	for s := uint64(0); s < 200; s += 20 { // several frames per partition
		send(t, st, s, s+20)
		h.waitAck(st, s+19)
	}
	h.stop()

	// cut the last entry of partition 2 (as if its final write never reached disk)
	pdir := filepath.Join(dir, "wal", "2")
	ents, _ := os.ReadDir(pdir)
	seg := filepath.Join(pdir, ents[len(ents)-1].Name())
	b, _ := os.ReadFile(seg)
	_ = os.WriteFile(seg, b[:len(b)-3], 0o640)

	h = start(t, dir)
	defer h.stop()
	lost := 200 - h.records()
	if lost <= 0 {
		t.Fatalf("expected the cut to lose records, log holds %d", h.records())
	}
	st = h.open(context.Background(), 5, 9) // the producer reconnects and resends everything unacked
	send(t, st, 0, 200)
	h.waitAck(st, 199)
	if n := h.records(); n != 200 {
		t.Fatalf("after resend the log holds %d records, want 200 (lost %d)", n, lost)
	}
	if d := h.m.RecordsDuplicate.Load(); d != uint64(200-lost) {
		t.Fatalf("duplicates = %d, want %d", d, 200-lost)
	}
}

func TestDrainAcksBufferedRecords(t *testing.T) {
	h := start(t, t.TempDir())
	defer h.stop()
	stopFlush := make(chan struct{})
	go func() { // stand-in for the flusher while draining
		for {
			select {
			case <-stopFlush:
				return
			case <-time.After(time.Millisecond):
				h.store.Flush()
			}
		}
	}()
	defer close(stopFlush)
	st := h.open(context.Background(), 6, 1)
	send(t, st, 0, 30)
	h.waitAck(st, 29)
	send(t, st, 30, 40)
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h.srv.Drain(ctx)
	var last uint64
	for {
		a, err := st.Recv()
		if err != nil {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("after drain: %v, want Unavailable", err)
			}
			break
		}
		last = a.AckedUpToSeq
	}
	if last != 39 {
		t.Fatalf("last ack before shutdown = %d, want 39", last)
	}
}

func TestParseUUID(t *testing.T) {
	u, err := parseUUID("GPU-5fd4f087-86f3-7a43-b711-4771313afc50")
	if err != nil || u[0] != 0x5f || u[15] != 0x50 {
		t.Fatalf("u=%x err=%v", u, err)
	}
	if _, err := parseUUID("5FD4F087-86F3-7A43-B711-4771313AFC50"); err != nil {
		t.Fatalf("no prefix, upper case: %v", err)
	}
	for _, bad := range []string{"", "GPU-", "GPU-5fd4f087-86f3-7a43-b711-4771313afc5", "GPU-5fd4f087x86f3-7a43-b711-4771313afc50", "GPU-5fd4f087-86f3-7a43-b711-4771313afcZZ"} {
		if _, err := parseUUID(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
}
