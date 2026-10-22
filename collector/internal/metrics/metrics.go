// Package metrics exposes collector counters in Prometheus text format
// without any third-party dependency.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

var latencyBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type Histogram struct {
	counts   []atomic.Uint64 // len(bounds)+1, last is +Inf
	sumNanos atomic.Int64
	total    atomic.Uint64
}

func newHistogram() *Histogram {
	return &Histogram{counts: make([]atomic.Uint64, len(latencyBounds)+1)}
}

func (h *Histogram) Observe(d time.Duration) {
	i := sort.SearchFloat64s(latencyBounds, d.Seconds())
	h.counts[i].Add(1)
	h.sumNanos.Add(int64(d))
	h.total.Add(1)
}

type Metrics struct {
	RowsInserted     atomic.Uint64
	Inserts          atomic.Uint64
	InsertErrors     atomic.Uint64
	InsertLatency    *Histogram
	Commits          atomic.Uint64
	CommitsFenced    atomic.Uint64
	CommitErrors     atomic.Uint64
	PoisonBatches    atomic.Uint64
	SkippedRanges    atomic.Uint64 // ranges the broker could not read from disk
	MisroutedRecords atomic.Uint64 // records whose UUID hashes to another partition
	RegistryUpserts  atomic.Uint64
	RegistryErrors   atomic.Uint64
	BufferRows       atomic.Int64
	BufferBytes      atomic.Int64

	// Gauges is called on every scrape for state owned elsewhere (group, lag).
	Gauges func(w io.Writer)
}

func New() *Metrics { return &Metrics{InsertLatency: newHistogram()} }

// Gauge writes one gauge series; labels is either empty or `name="value",...`.
func Gauge(w io.Writer, name, help, labels string, v float64) {
	if help != "" {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
	}
	if labels != "" {
		fmt.Fprintf(w, "%s{%s} %g\n", name, labels, v)
		return
	}
	fmt.Fprintf(w, "%s %g\n", name, v)
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counter := func(name, help string, v uint64) {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
		}
		counter("collector_rows_inserted_total", "Rows inserted into ClickHouse (includes replays).", m.RowsInserted.Load())
		counter("collector_inserts_total", "Successful block inserts.", m.Inserts.Load())
		counter("collector_insert_errors_total", "Failed insert attempts (retried).", m.InsertErrors.Load())
		counter("collector_commits_total", "Offset commits accepted by the broker.", m.Commits.Load())
		counter("collector_commits_fenced_total", "Commits rejected because the generation had ended.", m.CommitsFenced.Load())
		counter("collector_commit_errors_total", "Commits that failed for other reasons.", m.CommitErrors.Load())
		counter("collector_poison_batches_total", "Batches that could not be decoded.", m.PoisonBatches.Load())
		counter("collector_skipped_ranges_total", "Log ranges the broker could not read (corruption).", m.SkippedRanges.Load())
		counter("collector_misrouted_records_total", "Records found in a partition their GPU does not hash to.", m.MisroutedRecords.Load())
		counter("collector_registry_upserts_total", "GPU registry rows written.", m.RegistryUpserts.Load())
		counter("collector_registry_errors_total", "Failed GPU registry writes (retried).", m.RegistryErrors.Load())
		Gauge(w, "collector_buffer_rows", "Rows buffered for the next insert.", "", float64(m.BufferRows.Load()))
		Gauge(w, "collector_buffer_bytes", "Approximate memory of the buffered rows.", "", float64(m.BufferBytes.Load()))

		h := m.InsertLatency
		fmt.Fprint(w, "# HELP collector_insert_duration_seconds Duration of successful inserts.\n# TYPE collector_insert_duration_seconds histogram\n")
		var cum uint64
		for i, b := range latencyBounds {
			cum += h.counts[i].Load()
			fmt.Fprintf(w, "collector_insert_duration_seconds_bucket{le=\"%g\"} %d\n", b, cum)
		}
		cum += h.counts[len(latencyBounds)].Load()
		fmt.Fprintf(w, "collector_insert_duration_seconds_bucket{le=\"+Inf\"} %d\n", cum)
		fmt.Fprintf(w, "collector_insert_duration_seconds_sum %g\ncollector_insert_duration_seconds_count %d\n",
			time.Duration(h.sumNanos.Load()).Seconds(), h.total.Load())
		if m.Gauges != nil {
			m.Gauges(w)
		}
	})
}
