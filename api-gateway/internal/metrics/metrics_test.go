package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(m *Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestRequestsByRouteAndCode(t *testing.T) {
	m := New()
	m.ObserveRequest("/api/v1/gpus", 200, 3*time.Millisecond)
	m.ObserveRequest("/api/v1/gpus", 200, 30*time.Millisecond)
	m.ObserveRequest("/api/v1/gpus", 429, time.Millisecond)
	m.ObserveRequest("unmatched", 404, 0)

	out := scrape(m)
	for _, want := range []string{
		`gateway_requests_total{route="/api/v1/gpus",code="200"} 2`,
		`gateway_requests_total{route="/api/v1/gpus",code="429"} 1`,
		`gateway_requests_total{route="unmatched",code="404"} 1`,
		`gateway_request_duration_seconds_bucket{route="/api/v1/gpus",le="0.005"} 2`, // 1 ms and 3 ms
		`gateway_request_duration_seconds_bucket{route="/api/v1/gpus",le="0.05"} 3`,
		`gateway_request_duration_seconds_bucket{route="/api/v1/gpus",le="+Inf"} 3`,
		`gateway_request_duration_seconds_count{route="/api/v1/gpus"} 3`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	// Routes and codes are listed in a stable order, whatever the map order.
	if strings.Index(out, `code="200"`) > strings.Index(out, `code="429"`) {
		t.Error("codes not sorted")
	}
}

func TestCountersHistogramAndGauges(t *testing.T) {
	m := New()
	m.QueryLatency.Observe(20 * time.Millisecond)
	m.QueryLatency.Observe(20 * time.Second) // past the last bound
	m.QueryErrors.Add(2)
	m.Rejected.Add(3)
	m.StreamAborts.Add(1)
	m.CacheHits.Add(4)
	m.CacheMisses.Add(5)
	m.InFlight.Store(2)
	m.Gauges = func(w io.Writer) {
		Gauge(w, "gateway_ready", "1 when ready.", "", 1)
		Gauge(w, "gateway_example", "", `shard="0"`, 7)
	}

	out := scrape(m)
	for _, want := range []string{
		`gateway_query_duration_seconds_bucket{le="0.025"} 1`,
		`gateway_query_duration_seconds_bucket{le="10"} 1`,
		`gateway_query_duration_seconds_bucket{le="+Inf"} 2`,
		"gateway_query_duration_seconds_count 2",
		"gateway_query_errors_total 2",
		"gateway_rejected_total 3",
		"gateway_stream_aborts_total 1",
		"gateway_gpu_cache_hits_total 4",
		"gateway_gpu_cache_misses_total 5",
		"gateway_inflight_queries 2",
		"# TYPE gateway_ready gauge",
		"gateway_ready 1",
		`gateway_example{shard="0"} 7`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out, "# HELP gateway_example") {
		t.Error("a gauge without help text printed a HELP line")
	}
}

func TestHandlerContentType(t *testing.T) {
	rec := httptest.NewRecorder()
	New().Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Fatalf("content type %q", ct)
	}
}
