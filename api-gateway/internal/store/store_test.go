package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

func TestClassify(t *testing.T) {
	for _, c := range []struct {
		err  error
		want error
	}{
		{&clickhouse.Exception{Code: 201}, ErrOverloaded},
		{&clickhouse.Exception{Code: 202}, ErrOverloaded},
		{&clickhouse.Exception{Code: 159}, ErrTimeout},
		{fmt.Errorf("read: %w", context.DeadlineExceeded), ErrTimeout},
		{&clickhouse.Exception{Code: 158}, ErrTooLarge},
		{&clickhouse.Exception{Code: 241}, ErrTooLarge},
		{&clickhouse.Exception{Code: 396}, ErrTooLarge},
	} {
		if got := classify(c.err); !errors.Is(got, c.want) {
			t.Errorf("%v classified as %v", c.err, got)
		}
	}
	other := &clickhouse.Exception{Code: 60}
	if got := classify(other); got != other {
		t.Errorf("unknown code rewritten: %v", got)
	}
	if classify(nil) != nil {
		t.Error("nil error rewritten")
	}
}

// The tests below need a ClickHouse with the chart's migrations and the dev
// users (clickhouse/config/dev-users.xml): api-gateway/tests/clickhouse-test.sh
// starts one in Docker and runs them. They TRUNCATE telemetry and gpus.

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// openTest connects as the api user, reading chunkRows rows per query.
func openTest(t *testing.T, chunkRows int) (*ClickHouse, clickhouse.Conn) {
	t.Helper()
	addr := os.Getenv("GATEWAY_TEST_CH_ADDR")
	if addr == "" {
		t.Skip("GATEWAY_TEST_CH_ADDR not set (run api-gateway/tests/clickhouse-test.sh)")
	}
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr},
		Auth: clickhouse.Auth{Username: env("GATEWAY_TEST_CH_ADMIN_USER", "default"), Password: os.Getenv("GATEWAY_TEST_CH_ADMIN_PASSWORD")}})
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(Options{Addr: addr, Database: "default", User: "api", Password: env("GATEWAY_TEST_CH_API_PASSWORD", "api-dev-pw"),
		Table: "telemetry", GPUsTable: "gpus", MaxConns: 2, DialTimeout: 5 * time.Second,
		QueryTimeout: 10 * time.Second, ChunkRows: chunkRows})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close(); _ = admin.Close() })
	return st, admin
}

// testCtx carries a deadline, as every gateway query does: the driver must not
// turn it into a max_execution_time setting, which the api user may not change.
func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func exec(t *testing.T, c clickhouse.Conn, q string, args ...any) {
	t.Helper()
	if err := c.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

var (
	gpuA = uuid.MustParse("5fd4f087-86f3-7a43-b711-4771313afc50")
	gpuB = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	gpuC = uuid.MustParse("ffffffff-0000-0000-0000-000000000001")
)

// seed: gpuA has 10 seconds x 2 metrics, inserted twice with merges stopped
// so the duplicates are really there; gpuB has one row; the registry holds
// gpuA twice (two last_seen values), gpuB, and gpuC, which has no telemetry.
func seed(t *testing.T, admin clickhouse.Conn) time.Time {
	ctx := context.Background()
	exec(t, admin, "TRUNCATE TABLE telemetry")
	exec(t, admin, "TRUNCATE TABLE gpus")
	exec(t, admin, "SYSTEM STOP MERGES telemetry")
	exec(t, admin, "SYSTEM STOP MERGES gpus")
	t.Cleanup(func() {
		_ = admin.Exec(ctx, "SYSTEM START MERGES telemetry")
		_ = admin.Exec(ctx, "SYSTEM START MERGES gpus")
	})
	base := time.Now().UTC().Truncate(time.Minute).Add(-10*time.Minute + 123456789) // sub-second, to the nanosecond
	for range 2 {
		b, err := admin.PrepareBatch(ctx, "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value)")
		if err != nil {
			t.Fatal(err)
		}
		for i := range 10 {
			ts := base.Add(time.Duration(i) * time.Second)
			_ = b.Append(gpuA, ts, "DCGM_FI_DEV_GPU_UTIL", uint16(1), float64(i))
			_ = b.Append(gpuA, ts, "it's_a_metric", uint16(1), float64(-i)) // a quote must survive a chunk boundary
		}
		_ = b.Append(gpuB, base, "DCGM_FI_DEV_GPU_UTIL", uint16(2), 42.0)
		if err := b.Send(); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range []struct {
		id   uuid.UUID
		seen time.Time
	}{{gpuA, base}, {gpuA, base.Add(time.Minute)}, {gpuB, base}, {gpuC, base}} {
		exec(t, admin, "INSERT INTO gpus (gpu_uuid, last_seen) VALUES (?, ?)", g.id, g.seen.Truncate(time.Second))
	}
	return base
}

func allGPUs(t *testing.T, st *ClickHouse, max int) []GPU {
	t.Helper()
	var out []GPU
	if err := st.GPUs(testCtx(t), max, func(g GPU) error { out = append(out, g); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func allEntries(t *testing.T, st *ClickHouse, q TelemetryQuery) []Entry {
	t.Helper()
	var out []Entry
	if err := st.Telemetry(testCtx(t), q, func(e Entry) error { out = append(out, e); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestClickHouseGPUs(t *testing.T) {
	big, admin := openTest(t, 1000)
	base := seed(t, admin)

	all := allGPUs(t, big, 0)
	if len(all) != 3 {
		t.Fatalf("got %d GPUs, want 3 (registry duplicates collapsed): %+v", len(all), all)
	}
	for _, g := range all {
		if g.ID == gpuA && !g.LastSeen.Equal(base.Add(time.Minute).Truncate(time.Second)) {
			t.Fatalf("gpuA last_seen %v, want the newest", g.LastSeen)
		}
	}
	// One GPU per chunk: the same list, in the same order, across every chunk boundary.
	small, _ := openTest(t, 1)
	if got := allGPUs(t, small, 0); !reflect.DeepEqual(ids(got), ids(all)) {
		t.Fatalf("chunked: %v, want %v", ids(got), ids(all))
	}
	// max stops the scan: the first two of the same order.
	if two := allGPUs(t, small, 2); !reflect.DeepEqual(ids(two), ids(all[:2])) {
		t.Fatalf("max 2: %v, want %v", ids(two), ids(all[:2]))
	}
}

func ids(gs []GPU) []uuid.UUID {
	out := make([]uuid.UUID, len(gs))
	for i, g := range gs {
		out[i] = g.ID
	}
	return out
}

func TestClickHouseTelemetry(t *testing.T) {
	st, admin := openTest(t, 1000)
	base := seed(t, admin)

	full := allEntries(t, st, TelemetryQuery{GPU: gpuA})
	if len(full) != 20 {
		t.Fatalf("got %d entries, want 20 (unmerged duplicates hidden)", len(full))
	}
	for i := 1; i < len(full); i++ {
		a, b := full[i-1], full[i]
		if b.Ts.Before(a.Ts) || (b.Ts.Equal(a.Ts) && b.Metric <= a.Metric) {
			t.Fatalf("entry %d out of order: %+v then %+v", i, a, b)
		}
	}
	if !full[0].Ts.Equal(base) || full[0].Ts.Location() != time.UTC {
		t.Fatalf("first ts %v, want %v in UTC (nanoseconds kept)", full[0].Ts, base)
	}

	// Inclusive bounds at nanosecond precision: seconds 2..5 are 8 entries.
	start, end := base.Add(2*time.Second), base.Add(5*time.Second)
	win := allEntries(t, st, TelemetryQuery{GPU: gpuA, Start: &start, End: &end})
	if len(win) != 8 || !win[0].Ts.Equal(start) || !win[7].Ts.Equal(end) {
		t.Fatalf("window: %d entries, %v..%v", len(win), win[0].Ts, win[len(win)-1].Ts)
	}
	// One nanosecond past each bound excludes that second.
	start1, end1 := start.Add(1), end.Add(-1)
	if win = allEntries(t, st, TelemetryQuery{GPU: gpuA, Start: &start1, End: &end1}); len(win) != 4 {
		t.Fatalf("exclusive-by-1ns window: %d entries, want 4", len(win))
	}

	// Chunks of 3 (every boundary crossed, duplicates on both sides of one,
	// the quoted metric name in a resume key) give exactly the single read.
	small, _ := openTest(t, 3)
	if got := allEntries(t, small, TelemetryQuery{GPU: gpuA}); !reflect.DeepEqual(got, full) {
		t.Fatalf("chunked read differs:\n got %+v\nwant %+v", got, full)
	}
	if got := allEntries(t, small, TelemetryQuery{GPU: gpuA, Start: &start, End: &end}); len(got) != 8 {
		t.Fatalf("chunked window: %d entries, want 8", len(got))
	}
	// Max stops the scan after that many distinct entries, the earliest.
	if got := allEntries(t, small, TelemetryQuery{GPU: gpuA, Max: 7}); !reflect.DeepEqual(got, full[:7]) {
		t.Fatalf("max 7: %+v", got)
	}
	// ErrStop from the callback ends the scan without an error.
	n := 0
	if err := small.Telemetry(testCtx(t), TelemetryQuery{GPU: gpuA}, func(Entry) error {
		if n++; n == 5 {
			return ErrStop
		}
		return nil
	}); err != nil || n != 5 {
		t.Fatalf("ErrStop: n=%d err=%v", n, err)
	}

	// Other GPUs' rows never leak in.
	if b := allEntries(t, st, TelemetryQuery{GPU: gpuB}); len(b) != 1 || b[0].Value != 42 || b[0].StreamerID != 2 {
		t.Fatalf("gpuB: %+v", b)
	}
}

func TestClickHouseHasGPU(t *testing.T) {
	st, admin := openTest(t, 1000)
	seed(t, admin)
	for id, want := range map[uuid.UUID]bool{gpuA: true, gpuC: true, uuid.New(): false} {
		got, err := st.HasGPU(testCtx(t), id)
		if err != nil || got != want {
			t.Errorf("%s: %v %v, want %v", id, got, err, want)
		}
	}
}

func TestClickHouseDeadlineIsCancellation(t *testing.T) {
	st, _ := openTest(t, 1000)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	// Above the driver's 1 s threshold, so it would add max_execution_time without noDeadline.
	if err := st.GPUs(ctx, 1, func(GPU) error { return nil }); err != nil {
		t.Fatalf("query with a deadline: %v", err)
	}
	expired, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	<-expired.Done()
	if err := st.GPUs(expired, 1, func(GPU) error { return nil }); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expired context: %v, want ErrTimeout", err)
	}
}

func TestClickHouseAPIUserIsReadOnly(t *testing.T) {
	st, _ := openTest(t, 1000)
	err := st.conn.Exec(testCtx(t), "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value) VALUES (?, now64(9), 'x', 1, 1)", gpuA)
	if err == nil {
		t.Fatal("the api user could insert")
	}
}
