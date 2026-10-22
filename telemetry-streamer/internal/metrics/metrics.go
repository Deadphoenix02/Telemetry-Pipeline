// Package metrics exposes streamer counters in Prometheus text format
// without any third-party dependency.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var latencyBounds = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

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
	RowsEmitted  atomic.Uint64
	RecordsSent  atomic.Uint64 // includes resends
	RecordsAcked atomic.Uint64
	Retries      atomic.Uint64 // records resent after a stream failure
	StreamErrors atomic.Uint64
	BlockedNanos atomic.Int64 // pacer time blocked by backpressure, for completed blocks
	Latency      *Histogram   // send -> ack, per record
	MalformedFn  func() uint64

	fullness atomic.Int64 // permille of (unacked + queued) / capacity

	blockMu      sync.Mutex
	blockedSince time.Time // start of the block in progress; zero when not blocked

	csvLoaded       atomic.Bool
	streamConnected atomic.Bool
	shuttingDown    atomic.Bool

	broker         atomic.Int64 // index of the broker streamed to; -1 while none
	homeBroker     atomic.Int64
	allBrokersDown atomic.Bool
	failoverError  atomic.Uint64
	failoverAck    atomic.Uint64
	failoverHome   atomic.Uint64
}

func New() *Metrics {
	m := &Metrics{Latency: newHistogram()}
	m.broker.Store(-1)
	m.homeBroker.Store(-1)
	return m
}

// SetBroker records the broker (target index) the stream is open to; -1 for none.
func (m *Metrics) SetBroker(i int)     { m.broker.Store(int64(i)) }
func (m *Metrics) SetHomeBroker(i int) { m.homeBroker.Store(int64(i)) }
func (m *Metrics) Broker() int         { return int(m.broker.Load()) }

// SetAllBrokersDown is true while every broker failed in the last round: the
// only state in which the pacer may end up blocked.
func (m *Metrics) SetAllBrokersDown(v bool) { m.allBrokersDown.Store(v) }

// Failover counts a switch of broker by reason: error, ack_timeout or home.
func (m *Metrics) Failover(reason string) {
	switch reason {
	case "ack_timeout":
		m.failoverAck.Add(1)
	case "home":
		m.failoverHome.Add(1)
	default:
		m.failoverError.Add(1)
	}
}

func (m *Metrics) Failovers(reason string) uint64 {
	switch reason {
	case "ack_timeout":
		return m.failoverAck.Load()
	case "home":
		return m.failoverHome.Load()
	default:
		return m.failoverError.Load()
	}
}

func (m *Metrics) SetFullness(used, capacity int) {
	if capacity <= 0 {
		return
	}
	m.fullness.Store(int64(used) * 1000 / int64(capacity))
}

func (m *Metrics) Fullness() float64 { return float64(m.fullness.Load()) / 1000 }

// BeginBlocked and EndBlocked bracket a dispatch blocked by backpressure.
func (m *Metrics) BeginBlocked(t time.Time) {
	m.blockMu.Lock()
	m.blockedSince = t
	m.blockMu.Unlock()
}

func (m *Metrics) EndBlocked(t time.Time) {
	m.blockMu.Lock()
	if !m.blockedSince.IsZero() {
		m.BlockedNanos.Add(int64(t.Sub(m.blockedSince)))
		m.blockedSince = time.Time{}
	}
	m.blockMu.Unlock()
}

// Blocked is the total time blocked, including a block still in progress, so
// a long stall shows while it happens rather than only after it ends.
func (m *Metrics) Blocked() time.Duration {
	m.blockMu.Lock()
	defer m.blockMu.Unlock()
	d := time.Duration(m.BlockedNanos.Load())
	if !m.blockedSince.IsZero() {
		d += time.Since(m.blockedSince)
	}
	return d
}

// SetCSVLoaded marks the CSV source as loaded (readiness input).
func (m *Metrics) SetCSVLoaded() { m.csvLoaded.Store(true) }

// SetStreamConnected records whether a stream to the ingest service is open.
func (m *Metrics) SetStreamConnected(v bool) { m.streamConnected.Store(v) }

// StreamConnected reports whether a stream to the ingest service is open.
func (m *Metrics) StreamConnected() bool { return m.streamConnected.Load() }

// SetShuttingDown flips readiness off as soon as SIGTERM arrives.
func (m *Metrics) SetShuttingDown() { m.shuttingDown.Store(true) }

// Ready is true when the CSV is loaded, a stream is open and we are not shutting down.
func (m *Metrics) Ready() bool {
	return m.csvLoaded.Load() && m.streamConnected.Load() && !m.shuttingDown.Load()
}

// ReadyHandler serves /readyz: 200 when Ready, otherwise 503 with the reason.
func (m *Metrics) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch {
		case m.shuttingDown.Load():
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
		case !m.csvLoaded.Load():
			http.Error(w, "csv not loaded", http.StatusServiceUnavailable)
		case !m.streamConnected.Load():
			http.Error(w, "stream not connected", http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte("ready\n"))
		}
	})
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counter := func(name, help string, v uint64) {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
		}
		counter("streamer_rows_emitted_total", "Rows stamped and emitted as records.", m.RowsEmitted.Load())
		counter("streamer_records_sent_total", "Records written to the stream (includes resends).", m.RecordsSent.Load())
		counter("streamer_records_acked_total", "Records acknowledged by the server.", m.RecordsAcked.Load())
		counter("streamer_retries_total", "Records resent after a stream failure.", m.Retries.Load())
		counter("streamer_stream_errors_total", "Stream open/send/recv failures.", m.StreamErrors.Load())
		if m.MalformedFn != nil {
			counter("streamer_malformed_rows_total", "CSV rows skipped as malformed.", m.MalformedFn())
		}
		fmt.Fprintf(w, "# HELP streamer_backpressure_blocked_seconds_total Time the pacer spent blocked on a full queue.\n"+
			"# TYPE streamer_backpressure_blocked_seconds_total counter\nstreamer_backpressure_blocked_seconds_total %g\n",
			m.Blocked().Seconds())
		fmt.Fprint(w, "# HELP streamer_failovers_total Switches of broker, by reason (error, ack_timeout, home).\n# TYPE streamer_failovers_total counter\n")
		for _, r := range []string{"error", "ack_timeout", "home"} {
			fmt.Fprintf(w, "streamer_failovers_total{reason=%q} %d\n", r, m.Failovers(r))
		}
		fmt.Fprintf(w, "# HELP streamer_connected_broker Index of the broker streamed to; -1 while none.\n# TYPE streamer_connected_broker gauge\nstreamer_connected_broker %d\n", m.broker.Load())
		fmt.Fprintf(w, "# HELP streamer_home_broker Index of this streamer's home broker (streamer_id mod brokers).\n# TYPE streamer_home_broker gauge\nstreamer_home_broker %d\n", m.homeBroker.Load())
		down := 0
		if m.allBrokersDown.Load() {
			down = 1
		}
		fmt.Fprintf(w, "# HELP streamer_all_brokers_down 1 while every broker failed in the last round; the only state in which streaming pauses.\n# TYPE streamer_all_brokers_down gauge\nstreamer_all_brokers_down %d\n", down)
		ready := 0
		if m.Ready() {
			ready = 1
		}
		fmt.Fprintf(w, "# HELP streamer_ready 1 when the CSV is loaded and a stream is connected.\n# TYPE streamer_ready gauge\nstreamer_ready %d\n", ready)
		fmt.Fprintf(w, "# HELP streamer_buffer_fullness (Unacked + queued) / capacity, 0..1.\n# TYPE streamer_buffer_fullness gauge\nstreamer_buffer_fullness %g\n", m.Fullness())

		fmt.Fprint(w, "# HELP streamer_send_latency_seconds Time from send to ack.\n# TYPE streamer_send_latency_seconds histogram\n")
		var cum uint64
		for i, b := range latencyBounds {
			cum += m.Latency.counts[i].Load()
			fmt.Fprintf(w, "streamer_send_latency_seconds_bucket{le=\"%g\"} %d\n", b, cum)
		}
		cum += m.Latency.counts[len(latencyBounds)].Load()
		fmt.Fprintf(w, "streamer_send_latency_seconds_bucket{le=\"+Inf\"} %d\n", cum)
		fmt.Fprintf(w, "streamer_send_latency_seconds_sum %g\nstreamer_send_latency_seconds_count %d\n",
			time.Duration(m.Latency.sumNanos.Load()).Seconds(), m.Latency.total.Load())
	})
}
