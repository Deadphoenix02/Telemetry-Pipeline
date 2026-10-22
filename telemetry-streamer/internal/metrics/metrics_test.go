package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHistogramAndFullness(t *testing.T) {
	m := New()
	m.Latency.Observe(time.Millisecond)
	m.Latency.Observe(2 * time.Millisecond)
	m.Latency.Observe(11 * time.Second)
	if got := m.Latency.total.Load(); got != 3 {
		t.Fatalf("histogram count = %d, want 3", got)
	}
	if got := m.Latency.counts[0].Load(); got != 1 {
		t.Fatalf("first bucket count = %d, want 1", got)
	}
	if got := m.Latency.counts[1].Load(); got != 1 {
		t.Fatalf("second bucket count = %d, want 1", got)
	}
	if got := m.Latency.counts[len(m.Latency.counts)-1].Load(); got != 1 {
		t.Fatalf("infinity bucket count = %d, want 1", got)
	}

	m.SetFullness(3, 4)
	if got := m.Fullness(); got != .75 {
		t.Fatalf("fullness = %v, want 0.75", got)
	}
	m.SetFullness(1, 0)
	if got := m.Fullness(); got != .75 {
		t.Fatalf("nonpositive capacity changed fullness to %v", got)
	}
}

func TestHandlerExportsCountersAndOptionalMalformedMetric(t *testing.T) {
	m := New()
	m.RowsEmitted.Store(7)
	m.RecordsSent.Store(8)
	m.RecordsAcked.Store(6)
	m.Retries.Store(2)
	m.StreamErrors.Store(1)
	m.BlockedNanos.Store(int64(1500 * time.Millisecond))
	m.SetFullness(1, 2)
	m.Latency.Observe(5 * time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	res := httptest.NewRecorder()
	m.Handler().ServeHTTP(res, req)
	body := res.Body.String()
	if res.Header().Get("Content-Type") != "text/plain; version=0.0.4" {
		t.Fatalf("content type = %q", res.Header().Get("Content-Type"))
	}
	for _, want := range []string{
		"streamer_rows_emitted_total 7",
		"streamer_records_sent_total 8",
		"streamer_records_acked_total 6",
		"streamer_retries_total 2",
		"streamer_stream_errors_total 1",
		"streamer_backpressure_blocked_seconds_total 1.5",
		"streamer_buffer_fullness 0.5",
		"streamer_send_latency_seconds_count 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	if strings.Contains(body, "streamer_malformed_rows_total") {
		t.Fatal("malformed counter should be omitted when no callback is set")
	}

	m.MalformedFn = func() uint64 { return 9 }
	res = httptest.NewRecorder()
	m.Handler().ServeHTTP(res, req)
	if !strings.Contains(res.Body.String(), "streamer_malformed_rows_total 9") {
		t.Fatal("malformed callback value was not exported")
	}
}

func TestReadinessRequiresCSVStreamAndNoShutdown(t *testing.T) {
	m := New()
	if m.Ready() {
		t.Fatal("ready before anything is set")
	}
	m.SetCSVLoaded()
	if m.Ready() {
		t.Fatal("ready without a connected stream")
	}
	m.SetStreamConnected(true)
	if !m.Ready() || !m.StreamConnected() {
		t.Fatal("not ready with CSV loaded and stream connected")
	}
	m.SetStreamConnected(false)
	if m.Ready() {
		t.Fatal("still ready after the stream dropped")
	}
	m.SetStreamConnected(true)
	m.SetShuttingDown()
	if m.Ready() {
		t.Fatal("still ready while shutting down")
	}
}

func TestReadyHandlerReportsStatusAndReason(t *testing.T) {
	m := New()
	check := func(wantCode int, wantBody string) {
		t.Helper()
		res := httptest.NewRecorder()
		m.ReadyHandler().ServeHTTP(res, httptest.NewRequest("GET", "/readyz", nil))
		if res.Code != wantCode {
			t.Fatalf("status = %d, want %d (body %q)", res.Code, wantCode, res.Body.String())
		}
		if !strings.Contains(res.Body.String(), wantBody) {
			t.Fatalf("body = %q, want it to contain %q", res.Body.String(), wantBody)
		}
	}
	check(503, "csv not loaded")
	m.SetCSVLoaded()
	check(503, "stream not connected")
	m.SetStreamConnected(true)
	check(200, "ready")
	m.SetShuttingDown()
	check(503, "shutting down")
}

func TestHandlerExportsReadyGauge(t *testing.T) {
	m := New()
	scrape := func() string {
		res := httptest.NewRecorder()
		m.Handler().ServeHTTP(res, httptest.NewRequest("GET", "/metrics", nil))
		return res.Body.String()
	}
	if !strings.Contains(scrape(), "streamer_ready 0") {
		t.Fatal("streamer_ready should be 0 before readiness")
	}
	m.SetCSVLoaded()
	m.SetStreamConnected(true)
	if !strings.Contains(scrape(), "streamer_ready 1") {
		t.Fatal("streamer_ready should be 1 when ready")
	}
}

func TestBlockedIncludesBlockInProgress(t *testing.T) {
	m := New()
	m.BlockedNanos.Store(int64(time.Second))
	m.BeginBlocked(time.Now().Add(-2 * time.Second))
	if d := m.Blocked(); d < 3*time.Second || d > 4*time.Second {
		t.Fatalf("Blocked() during a block = %v, want about 3s", d)
	}
	m.EndBlocked(time.Now())
	if d := time.Duration(m.BlockedNanos.Load()); d < 3*time.Second {
		t.Fatalf("completed block not added: %v", d)
	}
	before := m.Blocked()
	m.EndBlocked(time.Now()) // no block in progress: no change
	if m.Blocked() != before {
		t.Fatal("EndBlocked without BeginBlocked changed the total")
	}
}
