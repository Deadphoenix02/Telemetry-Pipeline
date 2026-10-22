package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"broker/pkg/brokertest"
	"broker/pkg/frame"

	"collector/internal/config"
	"collector/internal/sink"
)

// memSink is an in-memory sink.Sink.
type memSink struct {
	mu   sync.Mutex
	rows int
	gpus map[[16]byte]bool
}

func (s *memSink) InsertTelemetry(_ context.Context, rows []sink.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows += len(rows)
	return nil
}

func (s *memSink) UpsertGPUs(_ context.Context, gpus [][16]byte, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range gpus {
		s.gpus[g] = true
	}
	return nil
}

func (*memSink) Ping(context.Context) error { return nil }
func (*memSink) Close() error               { return nil }

func (s *memSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func get(addr, path string) (int, string) {
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func records(n int) []frame.Record {
	out := make([]frame.Record, n)
	for i := range out {
		var u [16]byte
		u[0], u[15] = byte(i%7), 0x42
		out[i] = frame.Record{TsNanos: int64(1_760_000_000_000_000_000 + i), UUID: u, Metric: "DCGM_FI_DEV_GPU_UTIL", Value: float64(i)}
	}
	return out
}

func testConfig(t *testing.T, brokers []string, extra ...string) config.Config {
	t.Helper()
	args := append([]string{"-brokers", strings.Join(brokers, ","), "-ch_addr", "unused:9000", "-http_addr", freeAddr(t),
		"-client_id", "test", "-batch_wait", "20ms", "-registry_refresh", "1s", "-insert_timeout", "1s",
		"-shutdown_grace", "5s", "-log_level", "error"}, extra...)
	cfg, err := config.Parse(args)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The collector's main path: one consumer per broker into one sink, health and
// metrics per broker, and a clean drain on cancellation (not the watchdog).
func TestRunConsumesFromEveryBrokerAndDrains(t *testing.T) {
	b1, b2 := brokertest.Start(t, 3), brokertest.Start(t, 3)
	b1.Produce(1, 1, 0, records(300))
	b2.Produce(2, 1, 0, records(200))
	s := &memSink{gpus: map[[16]byte]bool{}}
	cfg := testConfig(t, []string{b1.Addr, b2.Addr})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, func(sink.Options) (sink.Sink, error) { return s, nil }) }()

	eventually(t, "rows from both brokers", func() bool { return s.count() == 500 && b1.Lag() == 0 && b2.Lag() == 0 })
	eventually(t, "readiness", func() bool { code, _ := get(cfg.HTTPAddr, "/readyz"); return code == 200 })
	if code, _ := get(cfg.HTTPAddr, "/healthz"); code != 200 {
		t.Fatalf("/healthz: %d", code)
	}
	_, metrics := get(cfg.HTTPAddr, "/metrics")
	for _, want := range []string{
		`collector_broker_in_group{broker="` + b1.Addr + `"} 1`,
		`collector_broker_in_group{broker="` + b2.Addr + `"} 1`,
		"collector_clickhouse_up 1",
		"collector_ready 1",
	} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(4 * time.Second): // well inside the 5 s grace: drained, not the watchdog
		t.Fatal("run did not drain and return")
	}
}

func TestRunFailsWhenClickHouseCannotBeOpened(t *testing.T) {
	cfg := testConfig(t, []string{"127.0.0.1:1"})
	err := run(context.Background(), cfg, func(sink.Options) (sink.Sink, error) { return nil, errors.New("connection refused") })
	if err == nil || !strings.Contains(err.Error(), "clickhouse") {
		t.Fatalf("run error = %v", err)
	}
}

// poison_policy=stop: an undecodable batch ends run with an error.
func TestRunStopsOnPoisonWithStopPolicy(t *testing.T) {
	b := brokertest.Start(t, 1)
	var fb frame.Builder
	fb.Reset(0, 9, 9)
	r := records(1)[0]
	_ = fb.Add(0, &r)
	f := fb.Bytes()
	b.AppendRaw(0, f[:len(f)-3]) // a valid header and CRC position, but records cut short
	cfg := testConfig(t, []string{b.Addr}, "-poison_policy", "stop")
	s := &memSink{gpus: map[[16]byte]bool{}}

	done := make(chan error, 1)
	go func() {
		done <- run(context.Background(), cfg, func(sink.Options) (sink.Sink, error) { return s, nil })
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "undecodable") {
			t.Fatalf("run error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop on the poison batch")
	}
}
