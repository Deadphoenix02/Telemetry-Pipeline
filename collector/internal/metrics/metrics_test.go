package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerWritesCountersHistogramAndGauges(t *testing.T) {
	m := New()
	m.RowsInserted.Add(5000)
	m.PoisonBatches.Add(1)
	m.BufferRows.Store(42)
	m.InsertLatency.Observe(30 * time.Millisecond)
	m.Gauges = func(w io.Writer) { Gauge(w, "collector_assigned_partitions", "Owned.", "", 5) }
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"collector_rows_inserted_total 5000",
		"collector_poison_batches_total 1",
		"collector_buffer_rows 42",
		`collector_insert_duration_seconds_bucket{le="0.05"} 1`,
		"collector_insert_duration_seconds_count 1",
		"collector_assigned_partitions 5",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
