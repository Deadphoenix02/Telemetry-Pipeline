// Package metrics exposes broker counters in Prometheus text format without
// any third-party dependency.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

var latencyBounds = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

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

func (h *Histogram) write(w io.Writer, name, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	var cum uint64
	for i, b := range latencyBounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, cum)
	}
	cum += h.counts[len(latencyBounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cum)
	fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", name, time.Duration(h.sumNanos.Load()).Seconds(), name, h.total.Load())
}

type Metrics struct {
	// produce
	StreamsOpened    atomic.Uint64
	StreamsFenced    atomic.Uint64 // replaced by a newer stream of the same producer
	RecordsAccepted  atomic.Uint64
	RecordsDuplicate atomic.Uint64
	RecordsInvalid   atomic.Uint64
	SequenceGaps     atomic.Uint64
	QuotaBlockedNs   atomic.Int64
	AckLatency       *Histogram // first record of a batch received -> ack sent

	// log
	FramesAppended   atomic.Uint64
	BytesAppended    atomic.Uint64
	Fsyncs           atomic.Uint64
	FsyncLatency     *Histogram
	Truncations      atomic.Uint64
	TruncateErrors   atomic.Uint64
	RecoveryTruncBs  atomic.Uint64 // bytes cut from torn tails at startup
	CorruptRanges    atomic.Uint64 // unreadable ranges skipped by Fetch
	FetchFrames      atomic.Uint64
	FetchBytes       atomic.Uint64
	CommitsAccepted  atomic.Uint64
	CommitsRejected  atomic.Uint64
	Rebalances       atomic.Uint64
	ConsumerSessions atomic.Uint64 // members expired by session timeout

	// Gauges is called on every scrape to write state-derived series
	// (disk use, watermarks, lag, group size).
	Gauges func(w io.Writer)
}

func New() *Metrics { return &Metrics{AckLatency: newHistogram(), FsyncLatency: newHistogram()} }

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counter := func(name, help string, v uint64) {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
		}
		counter("broker_streams_opened_total", "Producer streams opened.", m.StreamsOpened.Load())
		counter("broker_streams_fenced_total", "Producer streams closed because the same producer reconnected.", m.StreamsFenced.Load())
		counter("broker_records_accepted_total", "Records appended to the log.", m.RecordsAccepted.Load())
		counter("broker_records_duplicate_total", "Records dropped as producer retries already in the log.", m.RecordsDuplicate.Load())
		counter("broker_records_invalid_total", "Records dropped as undecodable (bad UUID, long metric name).", m.RecordsInvalid.Load())
		counter("broker_sequence_gaps_total", "Streams closed because a producer sequence skipped.", m.SequenceGaps.Load())
		fmt.Fprintf(w, "# HELP broker_quota_blocked_seconds_total Time producer streams spent paused at the disk quota.\n"+
			"# TYPE broker_quota_blocked_seconds_total counter\nbroker_quota_blocked_seconds_total %g\n",
			time.Duration(m.QuotaBlockedNs.Load()).Seconds())
		counter("broker_frames_appended_total", "Frames (WAL entries) appended.", m.FramesAppended.Load())
		counter("broker_bytes_appended_total", "Frame bytes appended.", m.BytesAppended.Load())
		counter("broker_fsyncs_total", "Partition fsyncs by the group-commit flusher.", m.Fsyncs.Load())
		counter("broker_truncations_total", "Consumed log prefixes deleted after a commit.", m.Truncations.Load())
		counter("broker_truncate_errors_total", "Failed truncations.", m.TruncateErrors.Load())
		counter("broker_recovery_truncated_bytes_total", "Bytes cut from torn log tails at startup.", m.RecoveryTruncBs.Load())
		counter("broker_corrupt_ranges_total", "Unreadable log ranges skipped by Fetch.", m.CorruptRanges.Load())
		counter("broker_fetch_frames_total", "Frames sent to consumers.", m.FetchFrames.Load())
		counter("broker_fetch_bytes_total", "Frame bytes sent to consumers.", m.FetchBytes.Load())
		counter("broker_commits_accepted_total", "Offset commits accepted.", m.CommitsAccepted.Load())
		counter("broker_commits_rejected_total", "Offset commits rejected (fencing, range).", m.CommitsRejected.Load())
		counter("broker_rebalances_total", "Consumer group generations formed.", m.Rebalances.Load())
		counter("broker_member_expirations_total", "Consumer members dropped after their session timed out.", m.ConsumerSessions.Load())
		m.AckLatency.write(w, "broker_ack_latency_seconds", "Time from receiving a record to acknowledging it.")
		m.FsyncLatency.write(w, "broker_fsync_duration_seconds", "Duration of one partition fsync.")
		if m.Gauges != nil {
			m.Gauges(w)
		}
	})
}

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
