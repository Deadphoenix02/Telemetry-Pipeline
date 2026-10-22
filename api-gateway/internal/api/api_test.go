package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"api-gateway/internal/metrics"
	"api-gateway/internal/store"
)

// fakeStore is an in-memory store with the same ordering, Max and ErrStop rules as the ClickHouse one.
type fakeStore struct {
	mu        sync.Mutex
	gpus      []store.GPU
	entries   map[uuid.UUID][]store.Entry
	err       error         // returned before any row
	failAfter int           // > 0: Telemetry fails after delivering this many entries
	block     chan struct{} // when set, every query waits on it
	calls     map[string]int
	lastQ     store.TelemetryQuery
}

var errMidStream = errors.New("clickhouse: connection reset mid-stream")

func newFake() *fakeStore {
	return &fakeStore{entries: map[uuid.UUID][]store.Entry{}, calls: map[string]int{}}
}

func (f *fakeStore) enter(name string) error {
	f.mu.Lock()
	f.calls[name]++
	block, err := f.block, f.err
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	return err
}

func (f *fakeStore) GPUs(_ context.Context, max int, fn func(store.GPU) error) error {
	if err := f.enter("GPUs"); err != nil {
		return err
	}
	out := append([]store.GPU(nil), f.gpus...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0 })
	for i, g := range out {
		if max > 0 && i == max {
			return nil
		}
		if err := fn(g); err != nil {
			return stopped(err)
		}
	}
	return nil
}

func stopped(err error) error {
	if errors.Is(err, store.ErrStop) {
		return nil
	}
	return err
}

func (f *fakeStore) Telemetry(_ context.Context, q store.TelemetryQuery, fn func(store.Entry) error) error {
	if err := f.enter("Telemetry"); err != nil {
		return err
	}
	f.mu.Lock()
	f.lastQ = q
	f.mu.Unlock()
	n := 0
	for _, e := range f.entries[q.GPU] {
		if (q.Start != nil && e.Ts.Before(*q.Start)) || (q.End != nil && e.Ts.After(*q.End)) {
			continue
		}
		if q.Max > 0 && n == q.Max {
			return nil
		}
		if f.failAfter > 0 && n == f.failAfter {
			return errMidStream
		}
		if err := fn(e); err != nil {
			return stopped(err)
		}
		n++
	}
	return nil
}

func (f *fakeStore) HasGPU(_ context.Context, id uuid.UUID) (bool, error) {
	if err := f.enter("HasGPU"); err != nil {
		return false, err
	}
	for _, g := range f.gpus {
		if g.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Close() error               { return nil }

var (
	gpuA = uuid.MustParse("5fd4f087-86f3-7a43-b711-4771313afc50")
	gpuB = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	t0   = time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)
)

// seed gives gpuA n seconds of two metrics each, so 2n entries in sort order.
func seed(f *fakeStore, n int) {
	f.gpus = []store.GPU{{ID: gpuA, LastSeen: t0}, {ID: gpuB, LastSeen: t0}}
	for i := 0; i < n; i++ {
		ts := t0.Add(time.Duration(i) * time.Second)
		f.entries[gpuA] = append(f.entries[gpuA],
			store.Entry{Ts: ts, Metric: "DCGM_FI_DEV_FB_USED", StreamerID: 1, Value: float64(i)},
			store.Entry{Ts: ts, Metric: "DCGM_FI_DEV_GPU_UTIL", StreamerID: 1, Value: float64(i)})
	}
}

func newServer(f *fakeStore, mod func(*Options)) (*Server, *metrics.Metrics) {
	opt := Options{MaxInFlight: 4, QueueWait: 50 * time.Millisecond, QueryTimeout: time.Second}
	if mod != nil {
		mod(&opt)
	}
	m := metrics.New()
	return New(f, opt, m, slog.New(slog.NewTextHandler(io.Discard, nil))), m
}

func get(t *testing.T, h http.Handler, url string, v any) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if v != nil {
		if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
			t.Fatalf("GET %s: %d %q: %v", url, rec.Code, rec.Body.String(), err)
		}
	}
	return rec
}

func TestListGPUs(t *testing.T) {
	f := newFake()
	seed(f, 1)
	s, _ := newServer(f, nil)
	var list gpuList
	rec := get(t, s.Handler(), "/api/v1/gpus", &list)
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("code=%d type=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if list.Count != 2 || list.GPUs[0].ID != "GPU-11111111-1111-1111-1111-111111111111" ||
		list.GPUs[1].ID != "GPU-5fd4f087-86f3-7a43-b711-4771313afc50" || !list.GPUs[0].LastSeen.Equal(t0) || list.Truncated {
		t.Fatalf("list: %+v", list)
	}
	if !strings.Contains(rec.Body.String(), `"truncated":false`) {
		t.Fatalf("truncated must always be present: %s", rec.Body)
	}
}

func TestListGPUsLimitAndTruncated(t *testing.T) {
	f := newFake()
	for i := 0; i < 25; i++ {
		f.gpus = append(f.gpus, store.GPU{ID: uuid.New(), LastSeen: t0})
	}
	s, _ := newServer(f, nil)
	h := s.Handler()
	for _, c := range []struct {
		query     string
		count     int
		truncated bool
	}{
		{"", 25, false},          // no limit: all of them
		{"?limit=10", 10, true},  // fewer than exist
		{"?limit=25", 25, false}, // exactly all: nothing was left out
		{"?limit=100", 25, false},
	} {
		var list gpuList
		if rec := get(t, h, "/api/v1/gpus"+c.query, &list); rec.Code != 200 || list.Count != c.count ||
			len(list.GPUs) != c.count || list.Truncated != c.truncated {
			t.Errorf("%q: %d count=%d truncated=%v, want count=%d truncated=%v", c.query, rec.Code, list.Count, list.Truncated, c.count, c.truncated)
		}
	}
}

func TestListGPUsCache(t *testing.T) {
	f := newFake()
	seed(f, 1)
	s, m := newServer(f, func(o *Options) { o.GPUCacheTTL = time.Minute })
	h := s.Handler()
	get(t, h, "/api/v1/gpus", nil)
	get(t, h, "/api/v1/gpus", nil)
	get(t, h, "/api/v1/gpus?limit=1", nil) // another limit, another key
	if f.calls["GPUs"] != 2 || m.CacheHits.Load() != 1 || m.CacheMisses.Load() != 2 {
		t.Fatalf("store calls=%d hits=%d misses=%d", f.calls["GPUs"], m.CacheHits.Load(), m.CacheMisses.Load())
	}

	nc, _ := newServer(f, nil) // TTL 0: no cache
	get(t, nc.Handler(), "/api/v1/gpus", nil)
	get(t, nc.Handler(), "/api/v1/gpus", nil)
	if f.calls["GPUs"] != 4 {
		t.Fatalf("uncached store calls=%d", f.calls["GPUs"])
	}
}

func TestTelemetryOrderedLimitedTruncated(t *testing.T) {
	f := newFake()
	seed(f, 10) // 20 entries
	s, _ := newServer(f, nil)
	h := s.Handler()
	tel := "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry"

	var all telemetryList
	if rec := get(t, h, tel, &all); rec.Code != 200 || all.Count != 20 || len(all.Telemetry) != 20 || all.Truncated ||
		all.GPUID != "GPU-5fd4f087-86f3-7a43-b711-4771313afc50" {
		t.Fatalf("no limit: %d %+v", rec.Code, all)
	}
	if f.lastQ.Max != 0 {
		t.Fatalf("no limit asked the store for at most %d rows", f.lastQ.Max)
	}
	for i := 1; i < len(all.Telemetry); i++ {
		if all.Telemetry[i].Timestamp.Before(all.Telemetry[i-1].Timestamp) {
			t.Fatalf("entry %d out of order", i)
		}
	}

	var first telemetryList
	if rec := get(t, h, tel+"?limit=7", &first); rec.Code != 200 || first.Count != 7 || !first.Truncated {
		t.Fatalf("limit 7: %d count=%d truncated=%v", rec.Code, first.Count, first.Truncated)
	}
	for i := range first.Telemetry { // the earliest entries
		if first.Telemetry[i] != all.Telemetry[i] {
			t.Fatalf("limit 7 entry %d: %+v, want %+v", i, first.Telemetry[i], all.Telemetry[i])
		}
	}
	if f.lastQ.Max != 8 {
		t.Fatalf("store asked for %d rows, want limit+1", f.lastQ.Max)
	}

	var exact telemetryList
	if get(t, h, tel+"?limit=20", &exact); exact.Count != 20 || exact.Truncated {
		t.Fatalf("limit equal to the match count: count=%d truncated=%v", exact.Count, exact.Truncated)
	}
}

// A history much larger than the stream's buffer and its deadline batches
// arrives whole, as one JSON document with exactly the documented fields.
func TestTelemetryStreamsLargeHistory(t *testing.T) {
	f := newFake()
	seed(f, 3000) // 6000 entries: several 32 KiB buffers, five deadline batches
	s, _ := newServer(f, nil)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2025-07-18T20:42:34Z")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" || resp.ContentLength != -1 {
		t.Fatalf("status %d type %q length %d (want streamed)", resp.StatusCode, resp.Header.Get("Content-Type"), resp.ContentLength)
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("streamed body is not one JSON document: %v", err)
	}
	var keys []string
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if want := []string{"count", "gpu_id", "start_time", "telemetry", "truncated"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("fields %v, want %v", keys, want)
	}
	var list telemetryList
	dec := json.NewDecoder(bytes.NewReader(mustJSON(t, raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&list); err != nil {
		t.Fatal(err)
	}
	if list.Count != 6000 || len(list.Telemetry) != 6000 || list.Truncated || list.StartTime == nil {
		t.Fatalf("count=%d entries=%d truncated=%v", list.Count, len(list.Telemetry), list.Truncated)
	}
	for i := 1; i < len(list.Telemetry); i++ {
		if list.Telemetry[i].Timestamp.Before(list.Telemetry[i-1].Timestamp) {
			t.Fatalf("entry %d out of order", i)
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A failure after the 200 is sent cannot change the status: the gateway
// aborts the connection, so the client sees a broken transfer, not a body
// that looks complete, and the abort is counted.
func TestTelemetryMidStreamFailureAborts(t *testing.T) {
	f := newFake()
	seed(f, 2000)
	f.failAfter = 1500
	s, m := newServer(f, nil)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || readErr == nil {
		t.Fatalf("status %d, read error %v: the transfer must break", resp.StatusCode, readErr)
	}
	if json.Valid(body) {
		t.Fatal("the cut-off body parses as JSON")
	}
	if m.StreamAborts.Load() != 1 {
		t.Fatalf("stream aborts %d", m.StreamAborts.Load())
	}
	out := httptest.NewRecorder()
	m.Handler().ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(out.Body.String(), `gateway_requests_total{route="/api/v1/gpus/{id}/telemetry",code="200"} 1`) {
		t.Fatal("aborted request not recorded")
	}

	// A failure before the first entry is still a proper error status.
	f.failAfter = 0
	f.err = errMidStream
	var e errorBody
	if rec := get(t, s.Handler(), "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", &e); rec.Code != 503 {
		t.Fatalf("early failure: %d %+v", rec.Code, e)
	}
}

func TestTelemetryTimeWindowInclusive(t *testing.T) {
	f := newFake()
	seed(f, 10)
	s, _ := newServer(f, nil)
	start := t0.Add(2 * time.Second).Format(time.RFC3339)
	// +05:30 written unescaped arrives as a space; the gateway accepts it.
	end := t0.Add(5 * time.Second).In(time.FixedZone("IST", 19800)).Format(time.RFC3339)
	var page telemetryList
	rec := get(t, s.Handler(), "/api/v1/gpus/5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time="+start+"&end_time="+end, &page)
	if rec.Code != 200 || page.Count != 8 { // seconds 2,3,4,5 x two metrics
		t.Fatalf("code=%d count=%d %s", rec.Code, page.Count, rec.Body)
	}
	if !page.Telemetry[0].Timestamp.Equal(t0.Add(2*time.Second)) || !page.Telemetry[7].Timestamp.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("bounds: first %v last %v", page.Telemetry[0].Timestamp, page.Telemetry[7].Timestamp)
	}
	if page.EndTime == nil || page.EndTime.Location() != time.UTC || !page.EndTime.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("end_time echoed as %v", page.EndTime)
	}
}

func TestTelemetryUnknownAndQuietGPU(t *testing.T) {
	f := newFake()
	seed(f, 3)
	s, _ := newServer(f, nil)
	h := s.Handler()

	var e errorBody
	if rec := get(t, h, "/api/v1/gpus/GPU-99999999-9999-9999-9999-999999999999/telemetry", &e); rec.Code != 404 || e.RequestID == "" {
		t.Fatalf("unknown GPU: %d %+v", rec.Code, e)
	}
	// gpuB is registered but has no rows in the window: an empty list, not a 404.
	var list telemetryList
	if rec := get(t, h, "/api/v1/gpus/GPU-11111111-1111-1111-1111-111111111111/telemetry", &list); rec.Code != 200 ||
		list.Count != 0 || list.Telemetry == nil || list.Truncated {
		t.Fatalf("quiet GPU: %d %+v", rec.Code, list)
	}
	// A GPU with rows never needs the registry.
	before := f.calls["HasGPU"]
	if rec := get(t, h, "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", &list); rec.Code != 200 || list.Count != 6 {
		t.Fatalf("known GPU: %d %+v", rec.Code, list)
	}
	if f.calls["HasGPU"] != before {
		t.Fatal("registry consulted although rows came back")
	}
}

func TestBadRequests(t *testing.T) {
	f := newFake()
	seed(f, 1)
	s, _ := newServer(f, nil)
	h := s.Handler()
	tel := "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry"
	for _, url := range []string{
		"/api/v1/gpus/not-a-gpu/telemetry",
		"/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc5/telemetry",
		"/api/v1/gpus/{5fd4f087-86f3-7a43-b711-4771313afc50}/telemetry",
		tel + "?start_time=yesterday",
		tel + "?end_time=2025-07-18",
		tel + "?start_time=2025-07-18T21:00:00Z&end_time=2025-07-18T20:00:00Z",
		tel + "?limit=0",
		tel + "?limit=-3",
		tel + "?limit=1000000001",
		tel + "?limit=ten",
		tel + "?cursor=abc", // paging was removed: an unknown parameter now
		tel + "?startTime=2025-07-18T20:00:00Z",
		"/api/v1/gpus?limit=-1",
		"/api/v1/gpus?cursor=abc",
		"/api/v1/gpus?page=2",
	} {
		var e errorBody
		if rec := get(t, h, url, &e); rec.Code != 400 || e.Error == "" {
			t.Errorf("%s: %d %q", url, rec.Code, rec.Body.String())
		}
	}
	if n := f.calls["Telemetry"] + f.calls["GPUs"]; n != 0 {
		t.Fatalf("%d bad requests reached the store", n)
	}
}

func TestStoreErrorsMapToStatus(t *testing.T) {
	for _, c := range []struct {
		err  error
		code int
	}{
		{fmt.Errorf("%w: code 202", store.ErrOverloaded), 429},
		{fmt.Errorf("%w: code 159", store.ErrTimeout), 504},
		{fmt.Errorf("%w: code 158", store.ErrTooLarge), 422},
		{fmt.Errorf("dial tcp: connection refused"), 503},
	} {
		f := newFake()
		f.err = c.err
		s, m := newServer(f, nil)
		var e errorBody
		rec := get(t, s.Handler(), "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", &e)
		if rec.Code != c.code || strings.Contains(e.Error, "code") || strings.Contains(e.Error, "refused") {
			t.Errorf("%v: %d %+v (internal detail must not leak)", c.err, rec.Code, e)
		}
		if c.code == 429 && rec.Header().Get("Retry-After") == "" {
			t.Error("429 without Retry-After")
		}
		if m.QueryErrors.Load() != 1 {
			t.Errorf("%v: query errors %d", c.err, m.QueryErrors.Load())
		}
	}
}

func TestQuerySlotsBoundConcurrency(t *testing.T) {
	f := newFake()
	seed(f, 1)
	f.block = make(chan struct{})
	s, m := newServer(f, func(o *Options) { o.MaxInFlight = 2; o.QueueWait = 20 * time.Millisecond })
	h := s.Handler()

	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- get(t, h, "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", nil).Code
		}()
	}
	for deadline := time.Now().Add(2 * time.Second); m.InFlight.Load() < 2; {
		if time.Now().After(deadline) {
			t.Fatal("the two queries never started")
		}
		time.Sleep(time.Millisecond)
	}
	if rec := get(t, h, "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", nil); rec.Code != 429 {
		t.Fatalf("third concurrent request: %d", rec.Code)
	}
	close(f.block)
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != 200 {
			t.Fatalf("queued request finished with %d", c)
		}
	}
	if m.Rejected.Load() != 1 || m.InFlight.Load() != 0 {
		t.Fatalf("rejected=%d inflight=%d", m.Rejected.Load(), m.InFlight.Load())
	}
}

func TestNonFiniteValuesEncodeAsNull(t *testing.T) {
	f := newFake()
	f.gpus = []store.GPU{{ID: gpuA, LastSeen: t0}}
	f.entries[gpuA] = []store.Entry{
		{Ts: t0, Metric: "a", Value: math.NaN()},
		{Ts: t0, Metric: "b", Value: math.Inf(1)},
		{Ts: t0, Metric: "c", Value: 0.5},
	}
	s, _ := newServer(f, nil)
	rec := get(t, s.Handler(), "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", nil)
	body := rec.Body.String()
	if rec.Code != 200 || strings.Count(body, `"value":null`) != 2 || !strings.Contains(body, `"value":0.5`) {
		t.Fatalf("%d %s", rec.Code, body)
	}
}

func TestRequestIDAndRouteMetrics(t *testing.T) {
	f := newFake()
	seed(f, 1)
	s, m := newServer(f, nil)
	h := s.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gpus", nil)
	req.Header.Set("X-Request-Id", "trace-abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") != "trace-abc" {
		t.Fatalf("request id not propagated: %q", rec.Header().Get("X-Request-Id"))
	}
	if rec := get(t, h, "/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry", nil); len(rec.Header().Get("X-Request-Id")) != 16 {
		t.Fatalf("generated request id %q", rec.Header().Get("X-Request-Id"))
	}
	get(t, h, "/nope", nil)
	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/api/v1/gpus", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %d", post.Code)
	}

	out := httptest.NewRecorder()
	m.Handler().ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`gateway_requests_total{route="/api/v1/gpus",code="200"} 1`,
		`gateway_requests_total{route="/api/v1/gpus/{id}/telemetry",code="200"} 1`,
		`gateway_requests_total{route="unmatched",code="404"} 1`,
		`gateway_request_duration_seconds_count{route="/api/v1/gpus"} 1`,
		`gateway_query_duration_seconds_count 2`,
	} {
		if !strings.Contains(out.Body.String(), want) {
			t.Errorf("metrics missing %s", want)
		}
	}
}

func TestParseGPUID(t *testing.T) {
	for _, s := range []string{
		"GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
		"gpu-5fd4f087-86f3-7a43-b711-4771313afc50",
		"5fd4f087-86f3-7a43-b711-4771313afc50",
		"5FD4F087-86F3-7A43-B711-4771313AFC50",
	} {
		id, err := ParseGPUID(s)
		if err != nil || id != gpuA {
			t.Errorf("%s: %v %v", s, id, err)
		}
	}
	if FormatGPUID(gpuA) != "GPU-5fd4f087-86f3-7a43-b711-4771313afc50" {
		t.Fatal(FormatGPUID(gpuA))
	}
	for _, s := range []string{"", "GPU-", "5fd4f08786f37a43b7114771313afc50", "urn:uuid:5fd4f087-86f3-7a43-b711-4771313afc50",
		"GPU-5fd4f087-86f3-7a43-b711-4771313afcZZ"} {
		if _, err := ParseGPUID(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}
