// Package api serves the read API in front of ClickHouse:
//
//	GET /api/v1/gpus                          GPUs that have reported telemetry
//	GET /api/v1/gpus/{id}/telemetry           one GPU's telemetry, ordered by time
//	    ?start_time=&end_time=                inclusive RFC 3339 bounds, both optional
//	GET /api/v1/swagger.json, swagger.yaml    the Swagger document `make swagger` generates
//	GET /api/v1/docs                          Swagger UI for it
//
// The swag annotations on the handlers below are the source of that document
// (api-gateway/openapi); openapi_test.go fails when they disagree with the code.
//
// Both return everything that matches unless the request sets limit, and say
// when a limit left items out (truncated). The store reads large results in
// chunks; telemetry is streamed to the client chunk by chunk, so the gateway's
// memory stays bounded by one chunk whatever the result size. Each request
// runs in one of a fixed number of query slots, so a burst queues briefly and
// then gets 429 instead of piling up on ClickHouse.
package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/metrics"
	"api-gateway/internal/store"
	"api-gateway/openapi"
)

type Options struct {
	MaxInFlight  int
	QueueWait    time.Duration
	QueryTimeout time.Duration // per ClickHouse query; a stream's write deadline moves on by about this much per chunk
	GPUCacheTTL  time.Duration // 0 disables
}

type Server struct {
	st    store.Store
	opt   Options
	m     *metrics.Metrics
	log   *slog.Logger
	slots chan struct{}
	cache *responseCache
}

func New(st store.Store, opt Options, m *metrics.Metrics, log *slog.Logger) *Server {
	s := &Server{st: st, opt: opt, m: m, log: log, slots: make(chan struct{}, opt.MaxInFlight)}
	if opt.GPUCacheTTL > 0 {
		s.cache = &responseCache{ttl: opt.GPUCacheTTL, entries: map[string]cachedResponse{}}
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/gpus", s.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", s.telemetry)
	mux.HandleFunc("GET /api/v1/swagger.json", serveFile("application/json", openapi.JSON))
	mux.HandleFunc("GET /api/v1/swagger.yaml", serveFile("application/yaml", openapi.YAML))
	mux.HandleFunc("GET /api/v1/docs", serveDocs)
	return s.instrument(mux)
}

func serveFile(contentType string, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	}
}

// The browser loads Swagger UI from a CDN; the gateway serves only this page
// and the spec, which it resolves relative to /api/v1/docs.
const docsPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GPU Telemetry API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js" crossorigin></script>
<script>window.ui = SwaggerUIBundle({ url: "swagger.json", dom_id: "#swagger-ui" });</script>
</body>
</html>
`

func serveDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsPage))
}

// ---- responses -------------------------------------------------------------------------------

// Field comments become the schema descriptions; @name sets the public schema name.

type gpuJSON struct {
	ID       string    `json:"id" validate:"required" example:"GPU-43a42a8a-c5bb-7c37-0096-f5098e8ea951"`       // GPU id in the DCGM form
	LastSeen time.Time `json:"last_seen" validate:"required" format:"date-time" example:"2026-10-20T17:15:30Z"` // When a collector last recorded telemetry for this GPU (refreshed about once a minute)
} //	@name	GPU

type gpuList struct {
	GPUs      []gpuJSON `json:"gpus" validate:"required"`
	Count     int       `json:"count" validate:"required" example:"2"`         // Number of GPUs returned
	Truncated bool      `json:"truncated" validate:"required" example:"false"` // True only when limit was set and more GPUs exist
} //	@name	GPUList

type entryJSON struct {
	Timestamp  time.Time `json:"timestamp" validate:"required" format:"date-time" example:"2026-10-20T17:15:29.59750465Z"` // Sample time, UTC, nanosecond precision
	Metric     string    `json:"metric" validate:"required" example:"DCGM_FI_DEV_POWER_USAGE"`                             // DCGM field name
	Value      number    `json:"value" validate:"required" swaggertype:"number" extensions:"x-nullable" example:"71.32"`   // The sample value; null when it is NaN or infinite
	StreamerID uint16    `json:"streamer_id" validate:"required" minimum:"0" maximum:"65535" example:"0"`                  // The streamer that produced the sample
} //	@name	TelemetryEntry

type telemetryList struct {
	GPUID     string      `json:"gpu_id" validate:"required" example:"GPU-43a42a8a-c5bb-7c37-0096-f5098e8ea951"` // The GPU, in the DCGM form
	StartTime *time.Time  `json:"start_time,omitempty" format:"date-time"`                                       // The requested lower bound, in UTC; absent when not given
	EndTime   *time.Time  `json:"end_time,omitempty" format:"date-time"`                                         // The requested upper bound, in UTC; absent when not given
	Count     int         `json:"count" validate:"required" example:"2"`                                         // Number of entries returned
	Truncated bool        `json:"truncated" validate:"required" example:"false"`                                 // True only when limit was set and more entries matched; those returned are the earliest
	Telemetry []entryJSON `json:"telemetry" validate:"required"`
} //	@name	TelemetryList

// number is a float that encodes NaN and ±Inf as null; encoding/json refuses them.
type number float64

func (n number) MarshalJSON() ([]byte, error) {
	f := float64(n)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return []byte("null"), nil
	}
	return strconv.AppendFloat(nil, f, 'g', -1, 64), nil
}

// ---- handlers --------------------------------------------------------------------------------

// The query parameters each route accepts; openapi_test.go holds the swag @Param lines to these.
var (
	gpuParams       = []string{"limit"}
	telemetryParams = []string{"start_time", "end_time", "limit"}
)

// listGPUs godoc
//
//	@Summary		List GPUs
//	@ID				listGPUs
//	@Description	Returns all GPUs for which telemetry is available, from the GPU registry, or the first limit of them when limit is set. A GPU leaves the list when its telemetry ages out of retention. Responses are cached by the gateway for a few seconds.
//	@Tags			gpus
//	@Produce		json
//	@Param			limit			query		int			false	"Optional. Return at most this many GPUs; all of them when omitted."	minimum(1)
//	@Param			X-Request-Id	header		string		false	"Correlation id, echoed back and logged. Up to 64 printable ASCII characters; one is generated otherwise."	maxlength(64)
//	@Success		200				{object}	gpuList
//	@Failure		400				{object}	errorBody	"Invalid limit, or an unknown query parameter."
//	@Failure		422				{object}	errorBody	"The query would read or hold too much; lower limit."
//	@Failure		429				{object}	errorBody	"At the concurrency or quota limit; retry after Retry-After seconds."
//	@Header			429				{integer}	Retry-After	"Seconds to wait before retrying."
//	@Failure		503				{object}	errorBody	"The telemetry store is unreachable or failed."
//	@Failure		504				{object}	errorBody	"The query timed out."
//	@Header			200,400,422,429,503,504	{string}	X-Request-Id	"The request's correlation id."
//	@Router			/api/v1/gpus [get]
func (s *Server) listGPUs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if err := checkParams(q, gpuParams...); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	key := strconv.Itoa(limit)
	if body, ok := s.cache.get(key); ok {
		s.m.CacheHits.Add(1)
		writeBody(w, http.StatusOK, body)
		return
	}
	s.m.CacheMisses.Add(1)

	// The registry is small, so the list is built in memory and cached whole.
	list := gpuList{GPUs: []gpuJSON{}}
	if err := s.query(r.Context(), func(ctx context.Context) error {
		return s.st.GPUs(ctx, readMax(limit), func(g store.GPU) error {
			if limit > 0 && len(list.GPUs) == limit { // the extra row: more exist
				list.Truncated = true
				return store.ErrStop
			}
			list.GPUs = append(list.GPUs, gpuJSON{ID: FormatGPUID(g.ID), LastSeen: g.LastSeen.UTC()})
			return nil
		})
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	list.Count = len(list.GPUs)
	body, err := json.Marshal(list)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.cache.put(key, body)
	writeBody(w, http.StatusOK, body)
}

// telemetry godoc
//
//	@Summary		Query telemetry by GPU
//	@ID				getGPUTelemetry
//	@Description	Returns all of the GPU's telemetry entries ordered by time (then metric, then streamer), optionally within an inclusive time window; either bound may be left out. When limit is set, only the earliest limit entries are returned. Duplicates from at-least-once delivery are removed. The response is streamed, so a large history arrives progressively. A GPU that has never reported gives 404; a known GPU with nothing in the window gives 200 with an empty telemetry list.
//	@Tags			gpus
//	@Produce		json
//	@Param			id				path		string		true	"GPU id, with or without the GPU- prefix, e.g. GPU-43a42a8a-c5bb-7c37-0096-f5098e8ea951. List GPUs gives the ids."
//	@Param			start_time		query		string		false	"Inclusive lower bound, RFC 3339, e.g. 2026-10-20T17:15:00Z. Must not be after end_time."	format(date-time)
//	@Param			end_time		query		string		false	"Inclusive upper bound, RFC 3339."	format(date-time)
//	@Param			limit			query		int			false	"Optional. Return at most this many entries, the earliest; all of them when omitted."	minimum(1)
//	@Param			X-Request-Id	header		string		false	"Correlation id, echoed back and logged. Up to 64 printable ASCII characters; one is generated otherwise."	maxlength(64)
//	@Success		200				{object}	telemetryList
//	@Failure		400				{object}	errorBody	"Invalid id, time or limit; start_time after end_time; or an unknown query parameter."
//	@Failure		404				{object}	errorBody	"No telemetry has ever been recorded for this GPU."
//	@Failure		422				{object}	errorBody	"The query would read or hold too much; narrow the window or lower limit."
//	@Failure		429				{object}	errorBody	"At the concurrency or quota limit; retry after Retry-After seconds."
//	@Header			429				{integer}	Retry-After	"Seconds to wait before retrying."
//	@Failure		503				{object}	errorBody	"The telemetry store is unreachable or failed."
//	@Failure		504				{object}	errorBody	"The query timed out; narrow the window or lower limit."
//	@Header			200,400,404,422,429,503,504	{string}	X-Request-Id	"The request's correlation id."
//	@Router			/api/v1/gpus/{id}/telemetry [get]
func (s *Server) telemetry(w http.ResponseWriter, r *http.Request) {
	id, err := ParseGPUID(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	q := r.URL.Query()
	if err := checkParams(q, telemetryParams...); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	start, err := parseTime("start_time", q.Get("start_time"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	end, err := parseTime("end_time", q.Get("end_time"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if start != nil && end != nil && start.After(*end) {
		writeError(w, r, http.StatusBadRequest, "start_time must not be after end_time")
		return
	}
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	// The response starts with the first entry: until then a failure can
	// still be a proper error status, and an empty result can still be a 404.
	sw := &telemetryStream{w: w, ext: 2*s.opt.QueryTimeout + 5*time.Second,
		head: telemetryList{GPUID: FormatGPUID(id), StartTime: utc(start), EndTime: utc(end)}}
	truncated := false
	err = s.query(r.Context(), func(ctx context.Context) error {
		q := store.TelemetryQuery{GPU: id, Start: start, End: end, Max: readMax(limit)}
		return s.st.Telemetry(ctx, q, func(e store.Entry) error {
			if limit > 0 && sw.n == limit { // the extra row: more matched
				truncated = true
				return store.ErrStop
			}
			return sw.entry(entryJSON{Timestamp: e.Ts, Metric: e.Metric, Value: number(e.Value), StreamerID: e.StreamerID})
		})
	})
	if sw.started {
		switch {
		case err == nil:
			if err := sw.finish(truncated); err != nil {
				s.log.Debug("stream: client went away", "request_id", requestID(r), "err", err)
			}
		case r.Context().Err() != nil:
			s.log.Debug("stream: client went away", "request_id", requestID(r), "entries", sw.n)
		default:
			// The status line is gone; abort the connection so the client sees a
			// broken transfer, never a body that looks complete.
			s.log.Error("stream failed mid-response", "request_id", requestID(r), "entries", sw.n, "err", err)
			s.m.StreamAborts.Add(1)
			panic(http.ErrAbortHandler)
		}
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// No entries is either a quiet window or a GPU we have never seen; only
	// the second is a 404. The registry lookup is a single-key read.
	var known bool
	if err := s.query(r.Context(), func(ctx context.Context) (err error) {
		known, err = s.st.HasGPU(ctx, id)
		return err
	}); err != nil {
		s.fail(w, r, err)
		return
	}
	if !known {
		writeError(w, r, http.StatusNotFound, "no telemetry for GPU "+FormatGPUID(id))
		return
	}

	empty := sw.head
	empty.Telemetry = []entryJSON{}
	writeJSON(w, r, http.StatusOK, empty)
}

// readMax is how many rows to read for a limit: one extra says more matched.
func readMax(limit int) int {
	if limit == 0 {
		return 0
	}
	return limit + 1
}

// telemetryStream writes a telemetryList as its entries arrive:
// {"gpu_id":…,"start_time":…,"telemetry":[…],"count":n,"truncated":b}.
type telemetryStream struct {
	w       http.ResponseWriter
	head    telemetryList // gpu_id and the bounds
	ext     time.Duration // how far each batch moves the write deadline
	bw      *bufio.Writer
	enc     *json.Encoder
	started bool
	n       int
}

func (t *telemetryStream) start() error {
	head, err := json.Marshal(struct {
		GPUID     string     `json:"gpu_id"`
		StartTime *time.Time `json:"start_time,omitempty"`
		EndTime   *time.Time `json:"end_time,omitempty"`
	}{t.head.GPUID, t.head.StartTime, t.head.EndTime})
	if err != nil {
		return err
	}
	t.started = true
	t.w.Header().Set("Content-Type", "application/json")
	t.w.WriteHeader(http.StatusOK)
	t.bw = bufio.NewWriterSize(t.w, 32<<10)
	t.enc = json.NewEncoder(t.bw)
	t.extend()
	_, err = t.bw.Write(append(head[:len(head)-1], `,"telemetry":[`...))
	return err
}

func (t *telemetryStream) entry(e entryJSON) error {
	if !t.started {
		if err := t.start(); err != nil {
			return err
		}
	} else if _, err := t.bw.WriteString(","); err != nil {
		return err
	}
	t.n++
	if t.n%1024 == 0 { // the server's WriteTimeout covers one batch, not the whole stream
		t.extend()
	}
	return t.enc.Encode(e)
}

func (t *telemetryStream) finish(truncated bool) error {
	if _, err := fmt.Fprintf(t.bw, `],"count":%d,"truncated":%t}`+"\n", t.n, truncated); err != nil {
		return err
	}
	return t.bw.Flush()
}

func (t *telemetryStream) extend() {
	_ = http.NewResponseController(t.w).SetWriteDeadline(time.Now().Add(t.ext))
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// ---- query slots and errors ------------------------------------------------------------------

var errBusy = errors.New("no query slot free within queue_wait")

// query runs fn in a query slot, bounded by the query timeout.
func (s *Server) query(ctx context.Context, fn func(context.Context) error) error {
	select {
	case s.slots <- struct{}{}:
	default:
		t := time.NewTimer(s.opt.QueueWait)
		defer t.Stop()
		select {
		case s.slots <- struct{}{}:
		case <-t.C:
			s.m.Rejected.Add(1)
			return errBusy
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer func() { <-s.slots }()
	s.m.InFlight.Add(1)
	defer s.m.InFlight.Add(-1)

	t0 := time.Now() // the store applies the per-query timeout to each chunk
	err := fn(ctx)
	s.m.QueryLatency.Observe(time.Since(t0))
	if err != nil && ctx.Err() == nil { // a client hanging up is not a query error
		s.m.QueryErrors.Add(1)
	}
	return err
}

// statusClientClosed is nginx's code for a client that left before the reply.
const statusClientClosed = 499

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case r.Context().Err() != nil:
		w.WriteHeader(statusClientClosed)
	case errors.Is(err, errBusy), errors.Is(err, store.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusTooManyRequests, "too many requests; retry shortly")
	case errors.Is(err, store.ErrTimeout):
		writeError(w, r, http.StatusGatewayTimeout, "query timed out; narrow the time window or lower limit")
	case errors.Is(err, store.ErrTooLarge):
		writeError(w, r, http.StatusUnprocessableEntity, "query too large; narrow the time window or lower limit")
	default:
		s.log.Error("query failed", "request_id", requestID(r), "path", r.URL.Path, "err", err)
		writeError(w, r, http.StatusServiceUnavailable, "telemetry store unavailable")
	}
}

type errorBody struct {
	Error     string `json:"error" validate:"required" example:"no telemetry for GPU GPU-00000000-0000-0000-0000-000000000000"` // What went wrong, for a person to read
	RequestID string `json:"request_id,omitempty" example:"3f9c2a7d1e5b8c40"`                                                   // The request's correlation id, also in X-Request-Id
} //	@name	Error

func writeError(w http.ResponseWriter, r *http.Request, code int, msg string) {
	writeJSON(w, r, code, errorBody{Error: msg, RequestID: requestID(r)})
}

func writeJSON(w http.ResponseWriter, r *http.Request, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"error":"encoding the response failed"}`, http.StatusInternalServerError)
		return
	}
	writeBody(w, code, body)
}

func writeBody(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
	w.WriteHeader(code)
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

// ---- GET /gpus cache -------------------------------------------------------------------------

// responseCache holds encoded GET /gpus responses for a few seconds: the registry
// changes slowly and dashboards poll it. A nil cache never hits.
type responseCache struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]cachedResponse
}

type cachedResponse struct {
	body    []byte
	expires time.Time
}

const maxCachedResponses = 256

func (c *responseCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.entries[key]
	if !ok || time.Now().After(p.expires) {
		return nil, false
	}
	return p.body, true
}

func (c *responseCache) put(key string, body []byte) {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= maxCachedResponses {
		for k, p := range c.entries {
			if now.After(p.expires) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= maxCachedResponses {
			clear(c.entries)
		}
	}
	c.entries[key] = cachedResponse{body: body, expires: now.Add(c.ttl)}
}

// ---- request id, logging, metrics ------------------------------------------------------------

type ctxKey struct{}

func requestID(r *http.Request) string {
	id, _ := r.Context().Value(ctxKey{}).(string)
	return id
}

// A caller's X-Request-Id is kept when it is short and printable, so a trace
// can follow a request through a proxy in front of the gateway.
func validRequestID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if c < '!' || c > '~' {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the connection, which the
// telemetry stream needs to move its write deadline along.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		id := r.Header.Get("X-Request-Id")
		if !validRequestID(id) {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, id))
		rec := &recorder{ResponseWriter: w, code: http.StatusOK}

		// Deferred, so a stream aborted with http.ErrAbortHandler is still counted.
		defer func() {
			// The mux sets r.Pattern on the request it served: a bounded label, unlike the path.
			route := "unmatched"
			if _, p, ok := strings.Cut(r.Pattern, " "); ok {
				route = p
			}
			d := time.Since(t0)
			s.m.ObserveRequest(route, rec.code, d)
			lvl := slog.LevelDebug
			if rec.code >= 500 && rec.code != statusClientClosed {
				lvl = slog.LevelWarn
			}
			s.log.Log(r.Context(), lvl, "request", "request_id", id, "method", r.Method, "path", r.URL.Path,
				"query", r.URL.RawQuery, "code", rec.code, "duration", d)
		}()
		next.ServeHTTP(rec, r)
	})
}
