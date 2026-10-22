package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"broker/internal/config"
	"broker/internal/ingestpb"
	"broker/pkg/consumer"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func testConfig(t *testing.T, dir string) config.Config {
	t.Helper()
	cfg, err := config.Parse([]string{"-data_dir", dir, "-listen", freeAddr(t), "-http_addr", freeAddr(t),
		"-partitions", "4", "-join_window", "50ms", "-session_timeout", "2s", "-heartbeat_interval", "200ms",
		"-drain_timeout", "1s", "-log_level", "error"})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// start runs the broker until the returned stop is called; stop returns run's error.
func start(t *testing.T, cfg config.Config) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	eventually(t, "readiness", func() bool { code, _ := get(cfg.HTTPAddr, "/readyz"); return code == 200 })
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			t.Fatal("broker did not stop")
			return nil
		}
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

// produce sends n records as one producer and waits for the cumulative ack.
func produce(t *testing.T, addr string, n int) {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-streamer-id", "7", "x-epoch", "42")
	st, err := ingestpb.NewIngestClient(cc).Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r := &ingestpb.Record{Seq: uint64(i), TimestampNs: time.Now().UnixNano(), MetricName: "DCGM_FI_DEV_GPU_UTIL",
			Uuid: fmt.Sprintf("GPU-5fd4f087-86f3-7a43-b711-%012d", i%5), Value: float64(i)}
		if err := st.Send(r); err != nil {
			t.Fatal(err)
		}
	}
	for {
		a, err := st.Recv()
		if err != nil {
			t.Fatalf("waiting for acks: %v", err)
		}
		if a.AckedUpToSeq == uint64(n-1) {
			return
		}
	}
}

// consumeAll joins the group and commits everything it receives until wantLag0 holds.
func consumeAll(t *testing.T, addr string, wantLag0 func() bool) {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cons := consumer.New(cc, consumer.Config{Group: "collectors", ClientID: "test", FetchMaxWait: 50 * time.Millisecond})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cons.Run(ctx, func(ctx context.Context, s *consumer.Session) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case b, ok := <-s.Batches:
					if !ok {
						return nil
					}
					_ = s.Commit(ctx, map[uint32]uint64{b.Partition: b.Next})
				}
			}
		})
	}()
	eventually(t, "lag 0 after consuming", wantLag0)
	cancel()
	<-done
}

func lagOf(t *testing.T, httpAddr string) uint64 {
	t.Helper()
	code, body := get(httpAddr, "/lag")
	if code != 200 {
		t.Fatalf("/lag: %d %s", code, body)
	}
	var l struct{ Lag uint64 }
	if err := json.Unmarshal([]byte(body), &l); err != nil {
		t.Fatal(err)
	}
	return l.Lag
}

// The broker's main path end to end: recover an empty log, serve producers and
// a consumer group, expose health, lag and metrics, drain on shutdown, and
// recover the same data directory on the next start.
func TestRunServesProducesConsumesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	stop := start(t, cfg)

	if code, body := get(cfg.HTTPAddr, "/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("/healthz: %d %q", code, body)
	}
	produce(t, cfg.Listen, 200)
	if lag := lagOf(t, cfg.HTTPAddr); lag == 0 {
		t.Fatal("no lag after producing 200 records")
	}
	_, metrics := get(cfg.HTTPAddr, "/metrics")
	for _, want := range []string{"broker_ready 1", "broker_records_accepted_total 200", "broker_producers 1"} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	consumeAll(t, cfg.Listen, func() bool { return lagOf(t, cfg.HTTPAddr) == 0 })
	if err := stop(); err != nil {
		t.Fatalf("run returned %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "offsets.json")); err != nil {
		t.Fatalf("offsets not persisted at shutdown: %v", err)
	}

	// The same data directory comes back with its offsets: nothing to consume.
	cfg2 := testConfig(t, dir)
	stop2 := start(t, cfg2)
	if lag := lagOf(t, cfg2.HTTPAddr); lag != 0 {
		t.Fatalf("lag %d after a restart, want 0 (committed offsets recovered)", lag)
	}
	if err := stop2(); err != nil {
		t.Fatal(err)
	}
}

func TestRunFailsOnUnusableDataDirOrPort(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, file)
	if err := run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "open log") {
		t.Fatalf("data dir is a file: %v", err)
	}

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	cfg = testConfig(t, t.TempDir())
	cfg.Listen = busy.Addr().String()
	if err := run(context.Background(), cfg); err == nil {
		t.Fatal("run succeeded on a port already in use")
	}
}
