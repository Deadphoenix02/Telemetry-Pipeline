package streamer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"telemetry-streamer/internal/metrics"
	"telemetry-streamer/internal/source"
	"telemetry-streamer/internal/wire"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type rowsSource struct {
	rows []source.Row
	next int
	err  error
}

func (s *rowsSource) Next() (source.Row, error) {
	if s.err != nil {
		return source.Row{}, s.err
	}
	row := s.rows[s.next%len(s.rows)]
	s.next++
	return row, nil
}
func (*rowsSource) Close() error     { return nil }
func (*rowsSource) Describe() string { return "test source" }

type autoAckTransport struct {
	mu        sync.Mutex
	streams   []*autoAckStream
	openErrs  []error
	failFirst bool
}

func (tr *autoAckTransport) OpenStream(context.Context, string) (wire.Stream, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	index := len(tr.streams)
	if index < len(tr.openErrs) && tr.openErrs[index] != nil {
		tr.streams = append(tr.streams, nil)
		return nil, tr.openErrs[index]
	}
	st := &autoAckStream{acks: make(chan wire.Ack, 16), done: make(chan struct{})}
	if index == 0 && tr.failFirst {
		st.failSend = true
	}
	tr.streams = append(tr.streams, st)
	return st, nil
}

type autoAckStream struct {
	mu       sync.Mutex
	sent     []wire.Record
	acks     chan wire.Ack
	done     chan struct{}
	once     sync.Once
	failSend bool
}

func (s *autoAckStream) Send(r wire.Record) error {
	s.mu.Lock()
	if s.failSend {
		s.failSend = false
		s.mu.Unlock()
		return errors.New("injected send failure")
	}
	s.sent = append(s.sent, r)
	s.mu.Unlock()
	s.acks <- wire.Ack{Seq: r.Seq}
	return nil
}

func (s *autoAckStream) Recv() (wire.Ack, error) {
	select {
	case ack := <-s.acks:
		return ack, nil
	case <-s.done:
		return wire.Ack{}, io.EOF
	}
}

func (s *autoAckStream) Close() error {
	s.once.Do(func() { close(s.done) })
	return nil
}

type observedTransport struct {
	mu        sync.Mutex
	openCalls int
	opened    chan struct{}
	delegate  wire.Transport
}

func (tr *observedTransport) OpenStream(ctx context.Context, producer string) (wire.Stream, error) {
	tr.mu.Lock()
	tr.openCalls++
	tr.mu.Unlock()
	select {
	case tr.opened <- struct{}{}:
	default:
	}
	if tr.delegate != nil {
		return tr.delegate.OpenStream(ctx, producer)
	}
	return nil, errors.New("injected open failure")
}

func testRow(value float64) source.Row {
	return source.Row{S: &source.Series{
		Metric: "power", GPUID: "0", Device: "gpu", UUID: "uuid", ModelName: "A100",
		Hostname: "host", Container: "container", Pod: "pod", Namespace: "ns",
	}, Value: value}
}

func TestWindowFor(t *testing.T) {
	for _, tc := range []struct {
		rate   int
		window time.Duration
		max    int
		want   int
	}{
		{1, 3 * time.Second, 60000, 100},      // the floor
		{1000, 3 * time.Second, 60000, 3000},  // 3 s of data
		{5000, 3 * time.Second, 60000, 15000}, // the e2e broker-crash rate
		{100000, 3 * time.Second, 60000, 60000},
		{100000, 3 * time.Second, 0, 300000}, // no cap
	} {
		if got := WindowFor(tc.rate, tc.window, tc.max); got != tc.want {
			t.Errorf("WindowFor(%d, %v, %d) = %d, want %d", tc.rate, tc.window, tc.max, got, tc.want)
		}
	}
}

func TestQueueFor(t *testing.T) {
	for rate, want := range map[int]int{1: 256, 1000: 256, 5000: 1000, 100000: 20000} {
		if got := QueueFor(rate); got != want {
			t.Errorf("QueueFor(%d) = %d, want %d", rate, got, want)
		}
	}
}

func one(tr wire.Transport) []Target { return []Target{{Name: "test", Tr: tr}} }

func TestOptionsDefaults(t *testing.T) {
	o := Options{}
	o.defaults()
	if o.Window != 3*time.Second || o.WindowMaxRecords != 60000 || o.ConnectTimeout != time.Second ||
		o.AckTimeout != 2*time.Second || o.HomeProbe != 10*time.Second {
		t.Fatalf("defaults: %+v", o)
	}
}

func TestTickStampsRowsAndDispatches(t *testing.T) {
	m := metrics.New()
	src := &rowsSource{rows: []source.Row{testRow(1), testRow(2)}}
	s := New(Options{ProducerID: "p", Rate: 100, Tick: time.Millisecond}, src, one(&autoAckTransport{}), m, quietLogger())
	time.Sleep(25 * time.Millisecond)
	w := &worker{in: make(chan wire.Record, 256)}
	if err := s.tick(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	close(w.in)
	first, second := <-w.in, <-w.in
	if first.Seq != 0 || second.Seq != 1 || first.TimestampNanos <= 0 || second.TimestampNanos <= 0 {
		t.Fatalf("unexpected sequence or timestamps: %+v %+v", first, second)
	}
	if first.Metric != "power" || first.UUID != "uuid" || first.Value != 1 || second.Value != 2 {
		t.Fatalf("source row was not copied into records: %+v %+v", first, second)
	}
	if got := m.RowsEmitted.Load(); got != 2 {
		t.Fatalf("RowsEmitted = %d, want 2", got)
	}
}

func TestTickReturnsSourceError(t *testing.T) {
	s := New(Options{Rate: 100, Tick: time.Millisecond}, &rowsSource{err: errors.New("read failed")}, one(&autoAckTransport{}), metrics.New(), quietLogger())
	time.Sleep(15 * time.Millisecond)
	err := s.tick(context.Background(), &worker{in: make(chan wire.Record, 1)})
	if err == nil || !strings.Contains(err.Error(), "source: read failed") {
		t.Fatalf("tick error = %v", err)
	}
}

func TestDispatchCancellationOnFullQueue(t *testing.T) {
	m := metrics.New()
	s := New(Options{Rate: 100, Tick: time.Millisecond}, &rowsSource{rows: []source.Row{testRow(1)}}, one(&autoAckTransport{}), m, quietLogger())
	w := &worker{in: make(chan wire.Record, 1)}
	w.in <- wire.Record{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.dispatch(ctx, w, wire.Record{})
	if err != context.Canceled {
		t.Fatalf("dispatch error = %v, want context canceled", err)
	}
	if m.BlockedNanos.Load() <= 0 {
		t.Fatal("blocked dispatch time was not recorded")
	}
}

func TestRunFlushesAndWaitsForAcknowledgements(t *testing.T) {
	m := metrics.New()
	tr := &autoAckTransport{}
	s := New(Options{ProducerID: "run-test", Rate: 1000, Tick: time.Millisecond, Grace: time.Second},
		&rowsSource{rows: []source.Row{testRow(1)}}, one(tr), m, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if m.RowsEmitted.Load() == 0 || m.RecordsAcked.Load() == 0 {
		t.Fatalf("run did not emit and ack records: emitted=%d acked=%d", m.RowsEmitted.Load(), m.RecordsAcked.Load())
	}
}

func TestRunReturnsSourceFailure(t *testing.T) {
	s := New(Options{ProducerID: "failure-test", Rate: 1000, Tick: time.Millisecond, Grace: time.Second},
		&rowsSource{err: errors.New("source broke")}, one(&autoAckTransport{}), metrics.New(), quietLogger())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "source: source broke") {
		t.Fatalf("Run error = %v", err)
	}
}
