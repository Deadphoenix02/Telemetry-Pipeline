package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"api-gateway/internal/config"
	"api-gateway/internal/store"
)

// memStore is an in-memory store.Store holding one GPU with three entries.
type memStore struct {
	pingErr error
}

var gpu = uuid.MustParse("5fd4f087-86f3-7a43-b711-4771313afc50")

func (memStore) GPUs(_ context.Context, _ int, fn func(store.GPU) error) error {
	return ignoreStop(fn(store.GPU{ID: gpu, LastSeen: time.Unix(1_760_000_000, 0)}))
}

func (memStore) Telemetry(_ context.Context, q store.TelemetryQuery, fn func(store.Entry) error) error {
	if q.GPU != gpu {
		return nil
	}
	for i := 0; i < 3; i++ {
		if err := fn(store.Entry{Ts: time.Unix(1_760_000_000+int64(i), 0).UTC(), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: float64(i)}); err != nil {
			return ignoreStop(err)
		}
	}
	return nil
}

func (memStore) HasGPU(_ context.Context, id uuid.UUID) (bool, error) { return id == gpu, nil }
func (s memStore) Ping(context.Context) error                         { return s.pingErr }
func (memStore) Close() error                                         { return nil }

func ignoreStop(err error) error {
	if errors.Is(err, store.ErrStop) {
		return nil
	}
	return err
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
		time.Sleep(20 * time.Millisecond)
	}
}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Parse([]string{"-http_addr", freeAddr(t), "-admin_addr", freeAddr(t),
		"-drain_delay", "100ms", "-shutdown_grace", "2s", "-log_level", "error"})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The gateway's main path: the API and the admin endpoints served together,
// readiness following ClickHouse, and a graceful drain on cancellation.
func TestRunServesAPIAndAdminAndDrains(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, func(store.Options) (store.Store, error) { return memStore{}, nil }) }()

	eventually(t, "readiness", func() bool { code, _ := get(cfg.AdminAddr, "/readyz"); return code == 200 })
	if code, body := get(cfg.AdminAddr, "/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("/healthz: %d %q", code, body)
	}

	code, body := get(cfg.HTTPAddr, "/api/v1/gpus/GPU-"+gpu.String()+"/telemetry")
	var list struct {
		Count     int  `json:"count"`
		Truncated bool `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(body), &list); err != nil || code != 200 || list.Count != 3 || list.Truncated {
		t.Fatalf("telemetry: %d %s (%v)", code, body, err)
	}
	if code, _ := get(cfg.HTTPAddr, "/api/v1/gpus"); code != 200 {
		t.Fatalf("/gpus: %d", code)
	}
	_, metrics := get(cfg.AdminAddr, "/metrics")
	for _, want := range []string{"gateway_clickhouse_up 1", "gateway_ready 1",
		`gateway_requests_total{route="/api/v1/gpus/{id}/telemetry",code="200"} 1`} {
		if !strings.Contains(metrics, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}

	cancel()
	// While draining, readiness is already off but the API still answers.
	eventually(t, "readiness off during the drain", func() bool { code, _ := get(cfg.AdminAddr, "/readyz"); return code == 503 })
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not drain and return")
	}
}

// ClickHouse not answering: alive, but not ready.
func TestRunNotReadyWhileClickHouseIsDown(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg, func(store.Options) (store.Store, error) { return memStore{pingErr: errors.New("down")}, nil })
	}()
	eventually(t, "the admin server", func() bool { code, _ := get(cfg.AdminAddr, "/healthz"); return code == 200 })
	if code, body := get(cfg.AdminAddr, "/readyz"); code != 503 || !strings.Contains(body, "clickhouse") {
		t.Fatalf("/readyz: %d %q", code, body)
	}
	cancel()
	<-done
}

func TestRunFailsWhenTheStoreOrPortIsUnusable(t *testing.T) {
	cfg := testConfig(t)
	if err := run(context.Background(), cfg, func(store.Options) (store.Store, error) { return nil, errors.New("bad dsn") }); err == nil ||
		!strings.Contains(err.Error(), "clickhouse") {
		t.Fatalf("store error: %v", err)
	}

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	cfg = testConfig(t)
	cfg.HTTPAddr = busy.Addr().String()
	if err := run(context.Background(), cfg, func(store.Options) (store.Store, error) { return memStore{}, nil }); err == nil ||
		!strings.Contains(err.Error(), "http server") {
		t.Fatalf("busy port: %v", err)
	}
}
