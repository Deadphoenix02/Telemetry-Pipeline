package streamer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telemetry-streamer/internal/metrics"
	"telemetry-streamer/internal/wire"
)

// fakeBroker is one broker for the worker: it can be down (open fails), hang
// (open never returns), stop acking, or be killed with streams open. Brokers
// of one test share a counter of streams that are sending, so the tests can
// assert the worker never sends on two streams at once.
type fakeBroker struct {
	name   string
	active *atomic.Int32 // shared: streams that have sent and are not closed
	overlp *atomic.Int32 // shared: sends seen while another stream was sending

	mu         sync.Mutex
	down       bool
	hang       bool
	stallAfter int // > 0: stop acking once this many records were received
	opens      int
	streams    []*fakeStream
	received   []uint64
}

func newBrokers(n int) []*fakeBroker {
	active, overlap := new(atomic.Int32), new(atomic.Int32)
	bs := make([]*fakeBroker, n)
	for i := range bs {
		bs[i] = &fakeBroker{name: fmt.Sprintf("broker-%d", i), active: active, overlp: overlap}
	}
	return bs
}

func targetsOf(bs []*fakeBroker) []Target {
	ts := make([]Target, len(bs))
	for i, b := range bs {
		ts[i] = Target{Name: b.name, Tr: b}
	}
	return ts
}

func (b *fakeBroker) set(f func(*fakeBroker)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f(b)
}

func (b *fakeBroker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.received)
}

func (b *fakeBroker) openCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opens
}

// kill takes the broker down and breaks its open streams, as a crash does.
func (b *fakeBroker) kill() {
	b.mu.Lock()
	b.down = true
	streams := append([]*fakeStream(nil), b.streams...)
	b.mu.Unlock()
	for _, st := range streams {
		_ = st.Close()
	}
}

func (b *fakeBroker) OpenStream(ctx context.Context, _ string) (wire.Stream, error) {
	b.mu.Lock()
	b.opens++
	down, hang := b.down, b.hang
	b.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if down {
		return nil, errors.New(b.name + " is down")
	}
	st := &fakeStream{b: b, acks: make(chan wire.Ack, 1024), done: make(chan struct{})}
	b.mu.Lock()
	b.streams = append(b.streams, st)
	b.mu.Unlock()
	return st, nil
}

type fakeStream struct {
	b       *fakeBroker
	acks    chan wire.Ack
	done    chan struct{}
	once    sync.Once
	sending atomic.Bool
	mu      sync.Mutex
	seqs    []uint64
}

func (s *fakeStream) Send(r wire.Record) error {
	select {
	case <-s.done:
		return errors.New("stream closed")
	default:
	}
	if s.sending.CompareAndSwap(false, true) {
		if s.b.active.Add(1) > 1 {
			s.b.overlp.Add(1)
		}
	}
	s.mu.Lock()
	s.seqs = append(s.seqs, r.Seq)
	s.mu.Unlock()
	s.b.mu.Lock()
	s.b.received = append(s.b.received, r.Seq)
	stall := s.b.stallAfter > 0 && len(s.b.received) > s.b.stallAfter
	s.b.mu.Unlock()
	if stall {
		return nil
	}
	select {
	case s.acks <- wire.Ack{Seq: r.Seq}:
	case <-s.done:
	}
	return nil
}

func (s *fakeStream) Recv() (wire.Ack, error) {
	select {
	case a := <-s.acks:
		return a, nil
	case <-s.done:
		return wire.Ack{}, errors.New("stream closed")
	}
}

func (s *fakeStream) Close() error {
	s.once.Do(func() {
		close(s.done)
		if s.sending.Load() {
			s.b.active.Add(-1)
		}
	})
	return nil
}

func (s *fakeStream) sent() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.seqs...)
}

func testOptions(home int) WorkerOptions {
	return WorkerOptions{Window: 5000, QueueCap: 64, Home: home,
		ConnectTimeout: 100 * time.Millisecond, AckTimeout: 150 * time.Millisecond, HomeProbe: 50 * time.Millisecond}
}

// pace feeds n records at about rate per second and closes the input. It
// returns how many times the queue was full, that is how often a real pacer
// would have paused.
func pace(w *worker, n, rate int) (blocked int) {
	const batch = 10
	every := time.Duration(float64(time.Second) * batch / float64(rate))
	for i := 0; i < n; i++ {
		r := wire.Record{Seq: uint64(i)}
		select {
		case w.in <- r:
		default:
			blocked++
			w.in <- r
		}
		if i%batch == batch-1 {
			time.Sleep(every)
		}
	}
	close(w.in)
	return blocked
}

type runResult struct {
	pending, blocked int
}

// runPaced runs the worker while pace feeds it; during runs alongside.
func runPaced(t *testing.T, w *worker, n, rate int, during func()) runResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- w.run(ctx) }()
	blockedc := make(chan int, 1)
	go func() { blockedc <- pace(w, n, rate) }()
	if during != nil {
		during()
	}
	blocked := <-blockedc
	select {
	case p := <-done:
		return runResult{pending: p, blocked: blocked}
	case <-ctx.Done():
		t.Fatal("worker did not finish")
		return runResult{}
	}
}

// delivered checks that every seq 0..n-1 reached some broker.
func delivered(t *testing.T, n int, bs ...*fakeBroker) {
	t.Helper()
	seen := make([]bool, n)
	for _, b := range bs {
		b.mu.Lock()
		for _, s := range b.received {
			if int(s) < n {
				seen[s] = true
			}
		}
		b.mu.Unlock()
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("seq %d never reached a broker", i)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkerDrainsInputAndAccountsCumulativeAcks(t *testing.T) {
	bs := newBrokers(1)
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(0), m, quietLogger())
	res := runPaced(t, w, 50, 5000, nil)
	if res.pending != 0 || m.RecordsAcked.Load() != 50 || m.RecordsSent.Load() != 50 {
		t.Fatalf("pending=%d acked=%d sent=%d", res.pending, m.RecordsAcked.Load(), m.RecordsSent.Load())
	}
}

// The heart of the design: a broker dies under load, the worker moves to the
// next one at once, and the pacer is never held up.
func TestWorkerFailsOverWithoutPausing(t *testing.T) {
	bs := newBrokers(3)
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(0), m, quietLogger())
	const n = 3000
	res := runPaced(t, w, n, 5000, func() {
		waitFor(t, "broker-0 to receive records", func() bool { return bs[0].count() > n/3 })
		bs[0].kill()
	})
	if res.pending != 0 {
		t.Fatalf("%d records left unacked", res.pending)
	}
	if res.blocked != 0 {
		t.Fatalf("the pacer would have paused %d times with two brokers up", res.blocked)
	}
	delivered(t, n, bs...)
	if bs[1].count() == 0 || bs[2].count() != 0 {
		t.Fatalf("expected the failover to go to broker-1 only: broker-1 %d, broker-2 %d", bs[1].count(), bs[2].count())
	}
	if m.Failovers("error") != 1 || bs[0].active.Load() != 0 || bs[0].overlp.Load() != 0 {
		t.Fatalf("failovers=%d active=%d overlapping sends=%d", m.Failovers("error"), bs[0].active.Load(), bs[0].overlp.Load())
	}
}

// Unacked records go to the next broker in order, on one stream, starting at
// the oldest unacked one: brokers require consecutive sequences per stream.
func TestWorkerResendsUnackedInOrderOnOneStream(t *testing.T) {
	bs := newBrokers(2)
	bs[0].stallAfter = 100 // acks 0..99, then holds the rest unacked
	m := metrics.New()
	opt := testOptions(0)
	opt.AckTimeout = time.Hour // the kill, not the ack timeout, triggers the failover
	w := newWorker("producer", targetsOf(bs), opt, m, quietLogger())
	const n = 400
	res := runPaced(t, w, n, 5000, func() {
		waitFor(t, "broker-0 to receive past its stall point", func() bool { return bs[0].count() > 200 })
		bs[0].kill()
	})
	if res.pending != 0 {
		t.Fatalf("%d records left unacked", res.pending)
	}
	if len(bs[1].streams) != 1 {
		t.Fatalf("broker-1 got %d streams, want 1", len(bs[1].streams))
	}
	got := bs[1].streams[0].sent()
	if len(got) == 0 || got[0] != 100 {
		t.Fatalf("resend started at %v, want the oldest unacked seq 100", got[:min(3, len(got))])
	}
	for i := 1; i < len(got); i++ {
		if got[i] != got[i-1]+1 {
			t.Fatalf("resend not consecutive at %d: %d after %d", i, got[i], got[i-1])
		}
	}
	if m.Retries.Load() == 0 {
		t.Fatal("resend not counted")
	}
}

// A broker that accepts records but never acks (hung, or quota-blocked) is
// left after the ack timeout.
func TestWorkerAckTimeoutFailsOver(t *testing.T) {
	bs := newBrokers(2)
	bs[0].stallAfter = 50
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(0), m, quietLogger())
	const n = 1500
	res := runPaced(t, w, n, 5000, nil)
	if res.pending != 0 || res.blocked != 0 {
		t.Fatalf("pending=%d blocked=%d", res.pending, res.blocked)
	}
	if m.Failovers("ack_timeout") == 0 {
		t.Fatal("no ack-timeout failover")
	}
	delivered(t, n, bs...)
	// broker-0 acked seqs 0..49; everything from 50 on, unacked there, went to broker-1.
	on1 := map[uint64]bool{}
	bs[1].mu.Lock()
	for _, s := range bs[1].received {
		on1[s] = true
	}
	bs[1].mu.Unlock()
	for s := uint64(50); s < n; s++ {
		if !on1[s] {
			t.Fatalf("seq %d, unacked on broker-0, never reached broker-1", s)
		}
	}
	if bs[0].overlp.Load() != 0 {
		t.Fatalf("%d overlapping sends", bs[0].overlp.Load())
	}
}

// An address that never answers costs one connect timeout, not a pause.
func TestWorkerSkipsHungBrokerWithinConnectTimeout(t *testing.T) {
	bs := newBrokers(2)
	bs[0].hang = true
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(0), m, quietLogger())
	res := runPaced(t, w, 1000, 5000, nil)
	if res.pending != 0 || res.blocked != 0 {
		t.Fatalf("pending=%d blocked=%d", res.pending, res.blocked)
	}
	if bs[1].count() != 1000 || m.StreamErrors.Load() == 0 {
		t.Fatalf("broker-1 got %d records, stream errors %d", bs[1].count(), m.StreamErrors.Load())
	}
}

// Once the home broker is back, the worker returns to it, but only after
// everything sent elsewhere is acked, so the streams never overlap.
func TestWorkerReturnsHomeAfterAcks(t *testing.T) {
	bs := newBrokers(2)
	bs[0].down = true
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(0), m, quietLogger())
	const n = 3000
	res := runPaced(t, w, n, 5000, func() {
		waitFor(t, "records on broker-1", func() bool { return bs[1].count() > 300 })
		bs[0].set(func(b *fakeBroker) { b.down = false })
		waitFor(t, "the return home", func() bool { return m.Broker() == 0 })
	})
	if res.pending != 0 || res.blocked != 0 {
		t.Fatalf("pending=%d blocked=%d", res.pending, res.blocked)
	}
	if m.Failovers("home") != 1 || bs[0].count() == 0 {
		t.Fatalf("home switches %d, records on broker-0 %d", m.Failovers("home"), bs[0].count())
	}
	if bs[0].overlp.Load() != 0 {
		t.Fatalf("%d sends overlapped another stream", bs[0].overlp.Load())
	}
	delivered(t, n, bs...)
}

// Only when every broker has failed does the worker back off; the window
// then fills and the pacer waits. One broker coming back resumes the flow.
func TestWorkerBacksOffOnlyWhenEveryBrokerIsDown(t *testing.T) {
	bs := newBrokers(3)
	for _, b := range bs {
		b.down = true
	}
	m := metrics.New()
	opt := testOptions(1)
	w := newWorker("producer", targetsOf(bs), opt, m, quietLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.in <- wire.Record{Seq: 0}
	close(w.in)
	done := make(chan int, 1)
	go func() { done <- w.run(ctx) }()

	waitFor(t, "all brokers marked down", func() bool { return metricLine(m, "streamer_all_brokers_down 1") })
	for _, b := range bs {
		if b.openCount() != 1 {
			t.Fatalf("%s tried %d times in the first round, want 1", b.name, b.openCount())
		}
	}
	bs[2].set(func(b *fakeBroker) { b.down = false })
	if p := <-done; p != 0 {
		t.Fatalf("%d left unacked after broker-2 came back", p)
	}
	if bs[2].count() != 1 || metricLine(m, "streamer_all_brokers_down 1") {
		t.Fatalf("broker-2 got %d records; still marked all-down", bs[2].count())
	}
}

func TestWorkerCountsQueuedRecordsOnCancellation(t *testing.T) {
	bs := newBrokers(1)
	bs[0].down = true
	w := newWorker("producer", targetsOf(bs), testOptions(0), metrics.New(), quietLogger())
	w.in <- wire.Record{Seq: 9}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- w.run(ctx) }()
	waitFor(t, "an open attempt", func() bool { return bs[0].openCount() > 0 })
	cancel()
	select {
	case p := <-done:
		if p != 1 {
			t.Fatalf("pending = %d, want the 1 queued record", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestWorkerReportsBrokerWhileConnected(t *testing.T) {
	bs := newBrokers(2)
	m := metrics.New()
	w := newWorker("producer", targetsOf(bs), testOptions(1), m, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- w.run(ctx) }()
	waitFor(t, "a connected stream", func() bool { return m.StreamConnected() && m.Broker() == 1 })
	cancel()
	<-done
	if m.StreamConnected() || m.Broker() != -1 {
		t.Fatal("still reported connected after the worker exited")
	}
}

func TestWorkerBackoff(t *testing.T) {
	w := newWorker("producer", targetsOf(newBrokers(1)), testOptions(0), metrics.New(), quietLogger())
	w.rng.Seed(3)
	for _, attempt := range []int{1, 2, 7, 20} {
		if d := w.backoff(attempt); d < 80*time.Millisecond || d > 6*time.Second {
			t.Fatalf("backoff(%d) = %v", attempt, d)
		}
	}
}

// metricLine reports whether the metrics output contains line.
func metricLine(m *metrics.Metrics, line string) bool {
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return strings.Contains(rec.Body.String(), line+"\n")
}
