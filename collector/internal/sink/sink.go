// Package sink writes telemetry rows and GPU registry entries to ClickHouse.
package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Row is one telemetry row. Its natural key (GPU, TsNanos, Metric, StreamerID)
// is the table's ReplacingMergeTree key, so inserting a row twice is harmless.
type Row struct {
	GPU        [16]byte
	TsNanos    int64
	Metric     string
	StreamerID uint16
	Value      float64
}

type Sink interface {
	// InsertTelemetry writes rows as one block. A failed call may have
	// written some of them; the caller retries the whole block.
	InsertTelemetry(ctx context.Context, rows []Row) error
	// UpsertGPUs records the GPUs as seen at seen.
	UpsertGPUs(ctx context.Context, gpus [][16]byte, seen time.Time) error
	Ping(ctx context.Context) error
	Close() error
}

type Options struct {
	Addr, Database, User, Password string
	Table, GPUsTable               string
	DialTimeout                    time.Duration
	MaxConns                       int // concurrent inserts plus the registry; at least 2
}

type ClickHouse struct {
	conn        driver.Conn
	insertTelem string
	insertGPUs  string
}

// Open connects with the native protocol. The collector's ClickHouse profile
// sets async_insert=0 (and insert_distributed_sync=1 on a cluster), so a
// successful insert means the rows are stored, which is what makes committing
// the offsets afterwards safe.
func Open(o Options) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:         []string{o.Addr},
		Auth:         clickhouse.Auth{Database: o.Database, Username: o.User, Password: o.Password},
		DialTimeout:  o.DialTimeout,
		MaxOpenConns: max(2, o.MaxConns), // one insert per concurrent session plus the registry
		MaxIdleConns: max(2, o.MaxConns),
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}
	return &ClickHouse{
		conn:        conn,
		insertTelem: fmt.Sprintf("INSERT INTO %s (gpu_uuid, ts, metric_name, streamer_id, value)", o.Table),
		insertGPUs:  fmt.Sprintf("INSERT INTO %s (gpu_uuid, last_seen)", o.GPUsTable),
	}, nil
}

func (c *ClickHouse) InsertTelemetry(ctx context.Context, rows []Row) error {
	b, err := c.conn.PrepareBatch(ctx, c.insertTelem)
	if err != nil {
		return err
	}
	ids := make([]uuid.UUID, len(rows))
	ts := make([]time.Time, len(rows))
	metric := make([]string, len(rows))
	sid := make([]uint16, len(rows))
	val := make([]float64, len(rows))
	for i := range rows {
		ids[i] = uuid.UUID(rows[i].GPU)
		ts[i] = time.Unix(0, rows[i].TsNanos).UTC()
		metric[i] = rows[i].Metric
		sid[i] = rows[i].StreamerID
		val[i] = rows[i].Value
	}
	for i, col := range []any{ids, ts, metric, sid, val} {
		if err := b.Column(i).Append(col); err != nil {
			_ = b.Abort()
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return b.Send()
}

func (c *ClickHouse) UpsertGPUs(ctx context.Context, gpus [][16]byte, seen time.Time) error {
	b, err := c.conn.PrepareBatch(ctx, c.insertGPUs)
	if err != nil {
		return err
	}
	seen = seen.UTC().Truncate(time.Second)
	for _, g := range gpus {
		if err := b.Append(uuid.UUID(g), seen); err != nil {
			_ = b.Abort()
			return err
		}
	}
	return b.Send()
}

func (c *ClickHouse) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }
func (c *ClickHouse) Close() error                   { return c.conn.Close() }
