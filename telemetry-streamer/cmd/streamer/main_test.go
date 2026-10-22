package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"telemetry-streamer/internal/config"
	"telemetry-streamer/internal/ingest"
)

const csvPath = "../../internal/resource/dcgm.csv"

// fakeBroker is the module's in-process ingest server on a loopback port.
type fakeBroker struct {
	addr string
	srv  *ingest.Server
	gs   *grpc.Server
}

func startBroker(t *testing.T) *fakeBroker {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ingest.NewServer(ingest.ServerConfig{AckInterval: 5 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	gs := grpc.NewServer(grpc.ForceServerCodec(ingest.Codec{}))
	ingest.RegisterIngestServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	b := &fakeBroker{addr: lis.Addr().String(), srv: srv, gs: gs}
	t.Cleanup(gs.Stop)
	return b
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

func get(addr, path string) (int, string) {
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
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

func parse(t *testing.T, args ...string) config.Config {
	t.Helper()
	cfg, err := config.Parse(append([]string{"-csv_path", csvPath, "-metrics_addr", freeAddr(t),
		"-rate_rows_per_sec", "2000", "-stats_interval", "50ms", "-grace", "5s", "-log_level", "error"}, args...))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The streamer's main path: stream to the home broker, fail over to the other
// when it stops, report it in /metrics, then flush everything on cancellation.
func TestRunStreamsAndFailsOverBetweenBrokers(t *testing.T) {
	home, other := startBroker(t), startBroker(t)
	cfg := parse(t, "-streamer_id", "0", "-targets", home.addr+","+other.addr,
		"-ack_timeout", "500ms", "-connect_timeout", "200ms", "-window", "1s", "-home_probe", "1h")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()

	eventually(t, "records on the home broker", func() bool { return home.srv.Accepted.Load() > 200 })
	eventually(t, "readiness", func() bool { code, _ := get(cfg.MetricsAddr, "/readyz"); return code == 200 })
	if _, m := get(cfg.MetricsAddr, "/metrics"); !strings.Contains(m, "streamer_connected_broker 0\n") ||
		!strings.Contains(m, "streamer_home_broker 0\n") {
		t.Fatal("not reported as connected to its home broker")
	}

	home.gs.Stop() // the home broker goes away
	eventually(t, "records on the other broker", func() bool { return other.srv.Accepted.Load() > 200 })
	_, m := get(cfg.MetricsAddr, "/metrics")
	for _, want := range []string{"streamer_connected_broker 1", `streamer_failovers_total{reason="error"} 1`, "streamer_all_brokers_down 0"} {
		if !strings.Contains(m, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v (records left unacked?)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not flush and return")
	}
	if other.srv.Gaps.Load() != 0 {
		t.Fatalf("%d out-of-order records on the new broker", other.srv.Gaps.Load())
	}
}

func TestRunDiscardsWithoutTargets(t *testing.T) {
	cfg := parse(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := run(ctx, cfg); err != nil {
		t.Fatalf("discard run: %v", err)
	}
}

func TestRunFailsOnAMissingCSV(t *testing.T) {
	cfg := parse(t)
	cfg.CSVPath = "/does/not/exist.csv"
	if err := run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "load csv") {
		t.Fatalf("run error = %v", err)
	}
}

func TestNewEpochIsRandom(t *testing.T) {
	if newEpoch() == newEpoch() && newEpoch() == newEpoch() {
		t.Fatal("epochs repeat")
	}
}
