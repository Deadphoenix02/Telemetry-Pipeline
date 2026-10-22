package sink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// These tests need a ClickHouse with the chart's migrations and the dev users
// (clickhouse/config/dev-users.xml). clickhouse/tests/test-container.sh starts
// one; make coverage runs them. They TRUNCATE telemetry and gpus.

func openTest(t *testing.T) (*ClickHouse, clickhouse.Conn) {
	t.Helper()
	addr := os.Getenv("COLLECTOR_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("COLLECTOR_TEST_CH_ADDR not set (see clickhouse/tests/test-container.sh)")
	}
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Username: "default"}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Options{Addr: addr, Database: "default", User: "collector", Password: "collector-dev-pw",
		Table: "telemetry", GPUsTable: "gpus", DialTimeout: 5 * time.Second, MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = admin.Close() })
	for _, q := range []string{"TRUNCATE TABLE telemetry", "TRUNCATE TABLE gpus"} {
		if err := admin.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	return s, admin
}

func count(t *testing.T, admin clickhouse.Conn, q string) uint64 {
	t.Helper()
	var n uint64
	if err := admin.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

var gpuA = uuid.MustParse("5fd4f087-86f3-7a43-b711-4771313afc50")

func rows(n int, base time.Time) []Row {
	out := make([]Row, n)
	for i := range out {
		out[i] = Row{GPU: gpuA, TsNanos: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Metric: "DCGM_FI_DEV_GPU_UTIL", StreamerID: 3, Value: float64(i)}
	}
	return out
}

func TestClickHouseInsertTelemetry(t *testing.T) {
	s, admin := openTest(t)
	ctx := context.Background()
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second).Add(123456789) // nanoseconds survive
	block := rows(500, base)
	if err := s.InsertTelemetry(ctx, block); err != nil {
		t.Fatal(err)
	}
	// A replayed block (redelivery after a crash) collapses by the table key.
	if err := s.InsertTelemetry(ctx, block); err != nil {
		t.Fatal(err)
	}
	if n := count(t, admin, "SELECT count() FROM telemetry FINAL"); n != 500 {
		t.Fatalf("%d distinct rows, want 500", n)
	}
	var ts time.Time
	var sid uint16
	if err := admin.QueryRow(ctx, "SELECT ts, streamer_id FROM telemetry ORDER BY ts LIMIT 1").Scan(&ts, &sid); err != nil {
		t.Fatal(err)
	}
	if !ts.Equal(base) || sid != 3 {
		t.Fatalf("first row ts=%v sid=%d, want %v and 3", ts, sid, base)
	}
}

func TestClickHouseUpsertGPUs(t *testing.T) {
	s, admin := openTest(t)
	ctx := context.Background()
	other := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	first := time.Now().Add(-time.Minute)
	if err := s.UpsertGPUs(ctx, [][16]byte{gpuA, other}, first); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertGPUs(ctx, [][16]byte{gpuA}, first.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if n := count(t, admin, "SELECT count() FROM gpus FINAL"); n != 2 {
		t.Fatalf("%d GPUs, want 2 (repeated registrations collapse)", n)
	}
	var last time.Time
	if err := admin.QueryRow(ctx, "SELECT last_seen FROM gpus FINAL WHERE gpu_uuid = ?", gpuA).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if want := first.Add(30 * time.Second).Truncate(time.Second); !last.Equal(want) {
		t.Fatalf("last_seen %v, want the newest, %v", last, want)
	}
}

// The collector user may only insert: it cannot read what it wrote.
func TestClickHouseCollectorUserIsInsertOnly(t *testing.T) {
	s, _ := openTest(t)
	var n uint64
	if err := s.conn.QueryRow(context.Background(), "SELECT count() FROM telemetry").Scan(&n); err == nil {
		t.Fatal("the collector user could read telemetry")
	}
}

func TestClickHouseInsertFailsCleanlyOnClosedConnection(t *testing.T) {
	s, _ := openTest(t)
	_ = s.Close()
	if err := s.InsertTelemetry(context.Background(), rows(1, time.Now())); err == nil {
		t.Fatal("insert on a closed connection succeeded")
	}
	if err := s.UpsertGPUs(context.Background(), [][16]byte{gpuA}, time.Now()); err == nil {
		t.Fatal("upsert on a closed connection succeeded")
	}
}
