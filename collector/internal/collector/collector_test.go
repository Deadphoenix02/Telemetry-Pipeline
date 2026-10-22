package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"broker/pkg/brokertest"
	"broker/pkg/consumer"
	"broker/pkg/frame"

	"collector/internal/metrics"
	"collector/internal/sink"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeSink stores rows in memory and can fail inserts on demand.
type fakeSink struct {
	mu       sync.Mutex
	rows     []sink.Row
	gpus     map[[16]byte]int
	failNext int
}

func newFakeSink() *fakeSink { return &fakeSink{gpus: map[[16]byte]int{}} }

func (f *fakeSink) InsertTelemetry(_ context.Context, rows []sink.Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return errors.New("clickhouse: injected failure")
	}
	f.rows = append(f.rows, rows...)
	return nil
}

func (f *fakeSink) UpsertGPUs(_ context.Context, gpus [][16]byte, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, g := range gpus {
		f.gpus[g]++
	}
	return nil
}

func (f *fakeSink) Ping(context.Context) error { return nil }
func (f *fakeSink) Close() error               { return nil }

type key struct {
	gpu [16]byte
	ts  int64
	m   string
	sid uint16
}

// distinct counts rows by the table's deduplication key; total includes replays.
func (f *fakeSink) distinct() (distinct, total int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[key]bool{}
	for _, r := range f.rows {
		seen[key{r.GPU, r.TsNanos, r.Metric, r.StreamerID}] = true
	}
	return len(seen), len(f.rows)
}

func gpu(i int) [16]byte {
	var u [16]byte
	u[0], u[7], u[15] = byte(i), byte(i*7), 0x42
	return u
}

var metricNames = []string{"DCGM_FI_DEV_GPU_UTIL", "DCGM_FI_DEV_POWER_USAGE", "DCGM_FI_DEV_MEM_CLOCK"}

func records(from, n int) []frame.Record {
	out := make([]frame.Record, n)
	for i := range out {
		k := from + i
		out[i] = frame.Record{TsNanos: int64(1_760_000_000_000_000_000 + k), UUID: gpu(k % 50), Metric: metricNames[k%3], Value: float64(k)}
	}
	return out
}

func defaultOpts() Options {
	return Options{BatchRows: 500, BatchWait: 50 * time.Millisecond, BufferBytes: 8 << 20, InsertTimeout: time.Second, PoisonPolicy: "skip"}
}

type running struct {
	cons   *consumer.Consumer
	col    *Collector
	m      *metrics.Metrics
	cancel context.CancelFunc
	done   chan struct{}
}

func start(t *testing.T, b *brokertest.Broker, s sink.Sink, opts Options, name string) *running {
	t.Helper()
	cc, err := grpc.NewClient(b.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	m := metrics.New()
	reg := NewRegistry(s, time.Hour, 10*time.Millisecond, m, quiet)
	col := New(opts, s, reg, m, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	cons := consumer.New(cc, consumer.Config{Group: brokertest.Group, ClientID: name, FetchMaxWait: 50 * time.Millisecond, Log: quiet})
	r := &running{cons: cons, col: col, m: m, cancel: cancel, done: make(chan struct{})}
	go reg.Run(ctx)
	go func() {
		defer close(r.done)
		_ = cons.Run(ctx, col.Handle)
	}()
	t.Cleanup(func() { cancel(); <-r.done })
	return r
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEndToEndThroughRealBroker(t *testing.T) {
	b := brokertest.Start(t, 4)
	s := newFakeSink()
	b.Produce(3, 77, 0, records(0, 2000))
	r := start(t, b, s, defaultOpts(), "c1")
	eventually(t, "2000 rows and zero lag", func() bool { d, _ := s.distinct(); return d == 2000 && b.Lag() == 0 })
	s.mu.Lock()
	for _, row := range s.rows {
		if row.StreamerID != 3 || row.Metric == "" {
			t.Fatalf("bad row %+v", row)
		}
	}
	s.mu.Unlock()
	eventually(t, "consumed entries deleted", func() bool {
		for _, f := range b.FirstOffsets() {
			if f <= 1 {
				return false
			}
		}
		return true
	})
	eventually(t, "50 GPUs registered", func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.gpus) == 50 })
	if r.m.Commits.Load() == 0 || r.m.Inserts.Load() == 0 {
		t.Fatalf("commits=%d inserts=%d", r.m.Commits.Load(), r.m.Inserts.Load())
	}
}

func TestFailedInsertsAreRetriedAndNothingIsCommittedMeanwhile(t *testing.T) {
	b := brokertest.Start(t, 2)
	s := newFakeSink()
	s.failNext = 3
	b.Produce(1, 1, 0, records(0, 300))
	r := start(t, b, s, defaultOpts(), "c1")
	eventually(t, "the first failure", func() bool { return r.m.InsertErrors.Load() >= 1 })
	if lag := b.Lag(); lag != 300 {
		t.Fatalf("lag during failing inserts = %d, want 300 (nothing committed)", lag)
	}
	eventually(t, "all rows after retries", func() bool { d, _ := s.distinct(); return d == 300 && b.Lag() == 0 })
	if r.m.InsertErrors.Load() != 3 {
		t.Fatalf("insert errors = %d, want 3", r.m.InsertErrors.Load())
	}
}

// poisonFrame has a valid header and CRC but its records do not match
// record_count, so it cannot be decoded.
func poisonFrame(p uint16) []byte {
	var fb frame.Builder
	fb.Reset(p, 9, 9)
	r := records(0, 1)[0]
	_ = fb.Add(0, &r)
	f := fb.Bytes()
	return f[:len(f)-3]
}

func TestPoisonBatchIsSkippedAndCommittedPast(t *testing.T) {
	b := brokertest.Start(t, 2)
	s := newFakeSink()
	b.Produce(1, 1, 0, records(0, 100))
	b.AppendRaw(0, poisonFrame(0))
	b.Produce(1, 1, 100, records(100, 100))
	r := start(t, b, s, defaultOpts(), "c1")
	eventually(t, "valid rows stored and lag zero", func() bool { d, _ := s.distinct(); return d == 200 && b.Lag() == 0 })
	if r.m.PoisonBatches.Load() != 1 {
		t.Fatalf("poison batches = %d, want 1", r.m.PoisonBatches.Load())
	}
}

func TestPoisonStopPolicyReportsFatal(t *testing.T) {
	b := brokertest.Start(t, 1)
	b.AppendRaw(0, poisonFrame(0))
	opts := defaultOpts()
	opts.PoisonPolicy = "stop"
	r := start(t, b, newFakeSink(), opts, "c1")
	select {
	case err := <-r.col.Fatal():
		if err == nil {
			t.Fatal("nil fatal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no fatal error for a poison batch with poison_policy=stop")
	}
}

func TestShutdownInsertsAndCommitsBufferedRows(t *testing.T) {
	b := brokertest.Start(t, 2)
	s := newFakeSink()
	opts := defaultOpts()
	opts.BatchRows, opts.BatchWait = 1_000_000, time.Hour // nothing flushes on its own
	b.Produce(1, 1, 0, records(0, 250))
	r := start(t, b, s, opts, "c1")
	eventually(t, "rows buffered", func() bool { return r.m.BufferRows.Load() == 250 })
	if d, _ := s.distinct(); d != 0 {
		t.Fatalf("%d rows inserted before shutdown", d)
	}
	r.cancel()
	<-r.done
	if d, _ := s.distinct(); d != 250 || b.Lag() != 0 {
		t.Fatalf("after shutdown: rows=%d lag=%d", d, b.Lag())
	}
}

func TestTwoCollectorsShareTheWorkWithoutLoss(t *testing.T) {
	b := brokertest.Start(t, 4)
	s := newFakeSink()
	r1 := start(t, b, s, defaultOpts(), "c1")
	b.Produce(1, 1, 0, records(0, 1000))
	eventually(t, "first batch", func() bool { d, _ := s.distinct(); return d == 1000 })
	r2 := start(t, b, s, defaultOpts(), "c2")
	eventually(t, "two partitions each", func() bool {
		return r1.cons.Stats.Assigned.Load() == 2 && r2.cons.Stats.Assigned.Load() == 2
	})
	b.Produce(2, 1, 0, records(1000, 1000))
	eventually(t, "second collector does work", func() bool { return r2.m.RowsInserted.Load() > 0 })
	r1.cancel() // scale down: c1 drains and leaves, c2 takes everything
	<-r1.done
	eventually(t, "c2 owns all partitions", func() bool { return r2.cons.Stats.Assigned.Load() == 4 })
	b.Produce(1, 1, 1000, records(2000, 1000))
	eventually(t, "every row stored once by key and zero lag", func() bool { d, _ := s.distinct(); return d == 3000 && b.Lag() == 0 })
}

// One collector consumes from several independent brokers at once, as the
// collector's main does: one consumer per broker, all handing their sessions
// to the same Collector. Losing one broker must not stop the others.
func TestOneCollectorConsumesFromEveryBroker(t *testing.T) {
	b1, b2 := brokertest.Start(t, 3), brokertest.Start(t, 3)
	s := newFakeSink()
	m := metrics.New()
	reg := NewRegistry(s, time.Hour, 10*time.Millisecond, m, quiet)
	col := New(defaultOpts(), s, reg, m, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	go reg.Run(ctx)
	for _, b := range []*brokertest.Broker{b1, b2} {
		cc, err := grpc.NewClient(b.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cc.Close() })
		cons := consumer.New(cc, consumer.Config{Group: brokertest.Group, ClientID: "c1", FetchMaxWait: 50 * time.Millisecond, Log: quiet})
		wg.Add(1)
		go func() { defer wg.Done(); _ = cons.Run(ctx, col.Handle) }()
	}

	// Different streamers on different brokers, as after a failover.
	b1.Produce(1, 1, 0, records(0, 1000))
	b2.Produce(2, 1, 0, records(1000, 1000))
	eventually(t, "rows from both brokers", func() bool { d, _ := s.distinct(); return d == 2000 && b1.Lag() == 0 && b2.Lag() == 0 })

	b1.Close() // broker-1 goes away; broker-2 keeps flowing
	b2.Produce(2, 1, 1000, records(2000, 500))
	eventually(t, "rows from the remaining broker", func() bool { d, _ := s.distinct(); return d == 2500 && b2.Lag() == 0 })
	eventually(t, "buffer gauges back to zero", func() bool { return m.BufferRows.Load() == 0 && m.BufferBytes.Load() == 0 })
}
