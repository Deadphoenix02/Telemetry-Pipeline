package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerWritesCountersHistogramsAndGauges(t *testing.T) {
	m := New()
	m.RecordsAccepted.Add(7)
	m.QuotaBlockedNs.Add(int64(1500 * time.Millisecond))
	m.AckLatency.Observe(3 * time.Millisecond)
	m.AckLatency.Observe(2 * time.Second)
	m.Gauges = func(w io.Writer) {
		Gauge(w, "broker_test_gauge", "A test gauge.", `partition="0"`, 1)
		Gauge(w, "broker_test_gauge", "", `partition="1"`, 2)
		Gauge(w, "broker_plain", "Plain.", "", 3)
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"broker_records_accepted_total 7",
		"broker_quota_blocked_seconds_total 1.5",
		`broker_ack_latency_seconds_bucket{le="0.005"} 1`,
		`broker_ack_latency_seconds_bucket{le="+Inf"} 2`,
		"broker_ack_latency_seconds_count 2",
		`broker_test_gauge{partition="1"} 2`,
		"broker_plain 3",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
	if n := strings.Count(body, "# TYPE broker_test_gauge gauge"); n != 1 {
		t.Fatalf("TYPE line for a labelled gauge written %d times", n)
	}
}
