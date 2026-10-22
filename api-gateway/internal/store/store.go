// Package store reads the GPU registry and telemetry from ClickHouse as the
// read-only api user. Every value from a request reaches ClickHouse as a typed
// query parameter, never as SQL text.
package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Errors the API maps to status codes. ClickHouse enforces the api user's
// caps; these say which one a query ran into.
var (
	ErrOverloaded = errors.New("clickhouse: quota or concurrent query limit reached")
	ErrTimeout    = errors.New("clickhouse: query timed out")
	ErrTooLarge   = errors.New("clickhouse: query reads or returns too much")
)

type GPU struct {
	ID       uuid.UUID
	LastSeen time.Time
}

// Entry is one telemetry sample. (Ts, Metric, StreamerID) is its key within a GPU.
type Entry struct {
	Ts         time.Time
	Metric     string
	StreamerID uint16
	Value      float64
}

type TelemetryQuery struct {
	GPU        uuid.UUID
	Start, End *time.Time // inclusive; nil is unbounded
	Max        int        // read at most this many entries; 0 reads them all
}

// ErrStop, returned by a callback, ends a scan early without an error.
var ErrStop = errors.New("stop")

// The scans read a result of any size in chunks of chunkRows rows, one
// ClickHouse query each (the api user may receive at most 100,000 rows per
// query). Each chunk resumes exactly after the previous one's last key, so
// rows that arrive meanwhile never shift or repeat a row. A chunk is read in
// full before fn sees it, so a slow caller never holds a query open.
type Store interface {
	// GPUs calls fn for every GPU in the registry, in gpu_uuid order, or for
	// the first max of them when max > 0.
	GPUs(ctx context.Context, max int, fn func(GPU) error) error
	// Telemetry calls fn for every entry of the query, ordered by time,
	// duplicates removed.
	Telemetry(ctx context.Context, q TelemetryQuery, fn func(Entry) error) error
	// HasGPU reports whether the registry knows the GPU.
	HasGPU(ctx context.Context, id uuid.UUID) (bool, error)
	Ping(ctx context.Context) error
	Close() error
}

type Options struct {
	Addr, Database, User, Password string
	Table, GPUsTable               string
	MaxConns                       int
	DialTimeout                    time.Duration
	QueryTimeout                   time.Duration // per query, so per chunk
	ChunkRows                      int
}

type ClickHouse struct {
	conn      driver.Conn
	table     string
	gpusTbl   string
	timeout   time.Duration
	chunkRows int
}

// Open connects with the native protocol. In replicated mode the api user's
// profile sets optimize_skip_unused_shards, so a query that pins gpu_uuid to a
// constant reads only the shard that GPU hashes to.
func Open(o Options) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:         []string{o.Addr},
		Auth:         clickhouse.Auth{Database: o.Database, Username: o.User, Password: o.Password},
		DialTimeout:  o.DialTimeout,
		MaxOpenConns: o.MaxConns,
		MaxIdleConns: o.MaxConns,
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}
	return &ClickHouse{conn: conn, table: o.Table, gpusTbl: o.GPUsTable, timeout: o.QueryTimeout, chunkRows: o.ChunkRows}, nil
}

// FINAL collapses the registry's repeated last_seen rows; the table is small
// and sharded like telemetry, so each shard merges only its own GPUs.
func (c *ClickHouse) GPUs(ctx context.Context, max int, fn func(GPU) error) error {
	var after *uuid.UUID
	for seen := 0; ; {
		n := c.chunkSize(max, seen)
		params := clickhouse.Parameters{}
		where := ""
		if after != nil {
			where = "WHERE gpu_uuid > {after:UUID}"
			params["after"] = after.String()
		}
		q := fmt.Sprintf("SELECT gpu_uuid, last_seen FROM %s FINAL %s ORDER BY gpu_uuid LIMIT %d", c.gpusTbl, where, n)
		chunk, err := readChunk(ctx, c, q, params, func(r driver.Rows) (GPU, error) {
			var g GPU
			err := r.Scan(&g.ID, &g.LastSeen)
			return g, err
		})
		if err != nil {
			return err
		}
		for _, g := range chunk {
			if err := fn(g); err != nil {
				return stopped(err)
			}
		}
		if seen += len(chunk); len(chunk) < n || seen == max {
			return nil
		}
		after = &chunk[len(chunk)-1].ID
	}
}

// Telemetry is the query shape clickhouse/tests/run.sh pins: the sort key
// prefix (gpu_uuid, ts) bounds the scan, ORDER BY follows the sort key so
// ClickHouse reads in order and stops at the LIMIT, LIMIT 1 BY hides
// at-least-once duplicates that have not been merged away yet, and the next
// chunk resumes after the last (ts, metric_name, streamer_id).
func (c *ClickHouse) Telemetry(ctx context.Context, tq TelemetryQuery, fn func(Entry) error) error {
	var last *Entry
	for seen := 0; ; {
		n := c.chunkSize(tq.Max, seen)
		params := clickhouse.Parameters{"gpu": tq.GPU.String()}
		where := "gpu_uuid = {gpu:UUID}"
		if tq.Start != nil {
			where += " AND ts >= fromUnixTimestamp64Nano({start:Int64})"
			params["start"] = strconv.FormatInt(tq.Start.UnixNano(), 10)
		}
		if tq.End != nil {
			where += " AND ts <= fromUnixTimestamp64Nano({end:Int64})"
			params["end"] = strconv.FormatInt(tq.End.UnixNano(), 10)
		}
		if last != nil {
			// The plain ts bound lets the primary key skip earlier granules;
			// the tuple comparison resumes exactly after the last row.
			where += " AND ts >= fromUnixTimestamp64Nano({after_ts:Int64})" +
				" AND (ts, metric_name, streamer_id) > (fromUnixTimestamp64Nano({after_ts:Int64}), {after_metric:String}, {after_sid:UInt16})"
			params["after_ts"] = strconv.FormatInt(last.Ts.UnixNano(), 10)
			params["after_metric"] = last.Metric
			params["after_sid"] = strconv.FormatUint(uint64(last.StreamerID), 10)
		}
		q := fmt.Sprintf(`SELECT ts, metric_name, streamer_id, value FROM %s
WHERE %s
ORDER BY ts, metric_name, streamer_id
LIMIT 1 BY gpu_uuid, ts, metric_name, streamer_id
LIMIT %d`, c.table, where, n)
		chunk, err := readChunk(ctx, c, q, params, func(r driver.Rows) (Entry, error) {
			var e Entry
			err := r.Scan(&e.Ts, &e.Metric, &e.StreamerID, &e.Value)
			e.Ts = e.Ts.UTC()
			return e, err
		})
		if err != nil {
			return err
		}
		for _, e := range chunk {
			if err := fn(e); err != nil {
				return stopped(err)
			}
		}
		if seen += len(chunk); len(chunk) < n || seen == tq.Max {
			return nil
		}
		last = &chunk[len(chunk)-1]
	}
}

// chunkSize is the next chunk's row count: chunkRows, or what is left of max.
func (c *ClickHouse) chunkSize(max, seen int) int {
	if max > 0 && max-seen < c.chunkRows {
		return max - seen
	}
	return c.chunkRows
}

// readChunk runs one chunk's query under the per-query timeout and reads all of its rows.
func readChunk[T any](ctx context.Context, c *ClickHouse, q string, params clickhouse.Parameters, scan func(driver.Rows) (T, error)) ([]T, error) {
	qctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	rows, err := c.conn.Query(query(qctx, params), q)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, classify(err)
		}
		out = append(out, v)
	}
	return out, classify(rows.Err())
}

func stopped(err error) error {
	if errors.Is(err, ErrStop) {
		return nil
	}
	return err
}

func (c *ClickHouse) HasGPU(ctx context.Context, id uuid.UUID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var n uint64
	q := fmt.Sprintf("SELECT count() FROM (SELECT 1 FROM %s WHERE gpu_uuid = {gpu:UUID} LIMIT 1)", c.gpusTbl)
	if err := c.conn.QueryRow(query(ctx, clickhouse.Parameters{"gpu": id.String()}), q).Scan(&n); err != nil {
		return false, classify(err)
	}
	return n > 0, nil
}

// noDeadline hides the context's deadline but keeps its cancellation. The
// driver turns a deadline into a max_execution_time setting on every query,
// which the api user's profile locks, so ClickHouse would reject the query.
// The profile's own max_execution_time is the server-side cap; cancellation
// still stops the query when the gateway's timeout fires.
type noDeadline struct{ context.Context }

func (noDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }

func query(ctx context.Context, params clickhouse.Parameters) context.Context {
	return clickhouse.Context(noDeadline{ctx}, clickhouse.WithParameters(params))
}

func (c *ClickHouse) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }
func (c *ClickHouse) Close() error                   { return c.conn.Close() }

// ClickHouse error codes the api user's profile and quota produce.
const (
	codeTooManyRows        = 158 // max_result_rows, max_rows_to_read
	codeTimeoutExceeded    = 159 // max_execution_time
	codeQuotaExceeded      = 201
	codeTooManySimultQuery = 202 // max_concurrent_queries_for_user
	codeMemoryLimit        = 241
	codeTooManyRowsOrBytes = 396
)

func classify(err error) error {
	if err == nil {
		return nil
	}
	if ex := (*clickhouse.Exception)(nil); errors.As(err, &ex) {
		switch ex.Code {
		case codeQuotaExceeded, codeTooManySimultQuery:
			return fmt.Errorf("%w: %v", ErrOverloaded, err)
		case codeTimeoutExceeded:
			return fmt.Errorf("%w: %v", ErrTimeout, err)
		case codeTooManyRows, codeTooManyRowsOrBytes, codeMemoryLimit:
			return fmt.Errorf("%w: %v", ErrTooLarge, err)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	return err
}
