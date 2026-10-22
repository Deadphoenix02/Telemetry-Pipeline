// Package metrics exposes gateway counters in Prometheus text format
// without any third-party dependency.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
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

func (h *Histogram) write(w io.Writer, name, labels string) {
	sep := ""
	if labels != "" {
		sep = ","
	}
	var cum uint64
	for i, b := range latencyBounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{%s%sle=\"%g\"} %d\n", name, labels, sep, b, cum)
	}
	cum += h.counts[len(latencyBounds)].Load()
	fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", name, labels, sep, cum)
	if labels != "" {
		labels = "{" + labels + "}"
	}
	fmt.Fprintf(w, "%s_sum%s %g\n%s_count%s %d\n", name, labels, time.Duration(h.sumNanos.Load()).Seconds(), name, labels, h.total.Load())
}

type routeCode struct {
	route string
	code  int
}

type Metrics struct {
	mu        sync.Mutex
	requests  map[routeCode]uint64
	durations map[string]*Histogram // by route

	QueryLatency *Histogram
	QueryErrors  atomic.Uint64
	Rejected     atomic.Uint64 // requests turned away: no query slot within queue_wait
	StreamAborts atomic.Uint64 // streamed responses cut off by a failure after the status was sent
	CacheHits    atomic.Uint64
	CacheMisses  atomic.Uint64
	InFlight     atomic.Int64 // ClickHouse queries running now

	// Gauges is called on every scrape for state owned elsewhere (ready, ClickHouse up).
	Gauges func(w io.Writer)
}

func New() *Metrics {
	return &Metrics{
		requests:     map[routeCode]uint64{},
		durations:    map[string]*Histogram{},
		QueryLatency: newHistogram(),
	}
}

// ObserveRequest records one finished HTTP request. route is the matched
// pattern, never the raw path, so the label set stays bounded.
func (m *Metrics) ObserveRequest(route string, code int, d time.Duration) {
	m.mu.Lock()
	m.requests[routeCode{route, code}]++
	h := m.durations[route]
	if h == nil {
		h = newHistogram()
		m.durations[route] = h
	}
	m.mu.Unlock()
	h.Observe(d)
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

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		counter := func(name, help string, v uint64) {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
		}

		m.mu.Lock()
		keys := make([]routeCode, 0, len(m.requests))
		for k := range m.requests {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].route != keys[j].route {
				return keys[i].route < keys[j].route
			}
			return keys[i].code < keys[j].code
		})
		fmt.Fprint(w, "# HELP gateway_requests_total HTTP requests by route and status code.\n# TYPE gateway_requests_total counter\n")
		for _, k := range keys {
			fmt.Fprintf(w, "gateway_requests_total{route=%q,code=\"%s\"} %d\n", k.route, strconv.Itoa(k.code), m.requests[k])
		}
		routes := make([]string, 0, len(m.durations))
		for r := range m.durations {
			routes = append(routes, r)
		}
		sort.Strings(routes)
		fmt.Fprint(w, "# HELP gateway_request_duration_seconds HTTP request duration by route.\n# TYPE gateway_request_duration_seconds histogram\n")
		for _, r := range routes {
			m.durations[r].write(w, "gateway_request_duration_seconds", fmt.Sprintf("route=%q", r))
		}
		m.mu.Unlock()

		fmt.Fprint(w, "# HELP gateway_query_duration_seconds Time a request spends reading ClickHouse, all chunks included.\n# TYPE gateway_query_duration_seconds histogram\n")
		m.QueryLatency.write(w, "gateway_query_duration_seconds", "")
		counter("gateway_query_errors_total", "ClickHouse queries that failed.", m.QueryErrors.Load())
		counter("gateway_rejected_total", "Requests rejected with 429 because no query slot freed up in time.", m.Rejected.Load())
		counter("gateway_stream_aborts_total", "Streamed responses aborted by a failure after the status was sent.", m.StreamAborts.Load())
		counter("gateway_gpu_cache_hits_total", "GET /gpus pages served from the cache.", m.CacheHits.Load())
		counter("gateway_gpu_cache_misses_total", "GET /gpus pages read from ClickHouse.", m.CacheMisses.Load())
		Gauge(w, "gateway_inflight_queries", "ClickHouse queries running now.", "", float64(m.InFlight.Load()))
		if m.Gauges != nil {
			m.Gauges(w)
		}
	})
}
