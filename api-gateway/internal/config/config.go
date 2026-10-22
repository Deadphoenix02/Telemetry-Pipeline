// Package config parses flags; every flag can also come from a GATEWAY_<NAME>
// environment variable (e.g. GATEWAY_CH_PASSWORD), which maps cleanly to Helm
// values and Secrets. Flags win over env.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxResultRows is the api user's locked max_result_rows: one ClickHouse
// query may return no more. The gateway reads larger results in chunks below it.
const maxResultRows = 100000

type Config struct {
	HTTPAddr  string // the public API
	AdminAddr string // /healthz /readyz /metrics

	CHAddr      string // ClickHouse native protocol host:port
	CHDatabase  string
	CHUser      string
	CHPassword  string
	CHTable     string
	CHGPUsTable string

	MaxInFlight  int           // concurrent ClickHouse queries per pod
	QueueWait    time.Duration // a request waits this long for a query slot, then gets 429
	QueryTimeout time.Duration // per ClickHouse query, so per chunk of a large result
	ChunkRows    int           // rows read per ClickHouse query; results of any size arrive in chunks
	GPUCacheTTL  time.Duration // 0 disables the GET /gpus cache

	DrainDelay    time.Duration // SIGTERM: stay up, unready, so the Service drops this pod...
	ShutdownGrace time.Duration // ...then finish in-flight requests within this
	LogLevel      string
}

func env(name string) (string, bool) {
	return os.LookupEnv("GATEWAY_" + strings.ToUpper(name))
}

func Parse(args []string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("api-gateway", flag.ContinueOnError)

	str := func(p *string, name, def, usage string) {
		if v, ok := env(name); ok {
			def = v
		}
		fs.StringVar(p, name, def, usage)
	}
	num := func(p *int, name string, def int, usage string) {
		if v, ok := env(name); ok {
			if n, err := strconv.Atoi(v); err == nil {
				def = n
			}
		}
		fs.IntVar(p, name, def, usage)
	}
	dur := func(p *time.Duration, name string, def time.Duration, usage string) {
		if v, ok := env(name); ok {
			if d, err := time.ParseDuration(v); err == nil {
				def = d
			}
		}
		fs.DurationVar(p, name, def, usage)
	}

	str(&c.HTTPAddr, "http_addr", ":8080", "HTTP address of the API")
	str(&c.AdminAddr, "admin_addr", ":9090", "HTTP address for /healthz /readyz /metrics (empty disables)")
	str(&c.CHAddr, "ch_addr", "clickhouse-telemetry-store:9000", "ClickHouse native protocol address")
	str(&c.CHDatabase, "ch_database", "default", "ClickHouse database")
	str(&c.CHUser, "ch_user", "api", "ClickHouse user (read-only)")
	str(&c.CHPassword, "ch_password", "", "ClickHouse password (prefer the GATEWAY_CH_PASSWORD env var)")
	str(&c.CHTable, "ch_table", "telemetry", "telemetry table")
	str(&c.CHGPUsTable, "ch_gpus_table", "gpus", "GPU registry table")
	num(&c.MaxInFlight, "max_inflight", 4, "concurrent ClickHouse queries per pod")
	dur(&c.QueueWait, "queue_wait", time.Second, "how long a request waits for a query slot before 429")
	dur(&c.QueryTimeout, "query_timeout", 15*time.Second, "timeout of one ClickHouse query (one chunk)")
	num(&c.ChunkRows, "chunk_rows", 10000, "rows read per ClickHouse query; larger results are read in chunks")
	dur(&c.GPUCacheTTL, "gpu_cache_ttl", 5*time.Second, "how long a GET /gpus response is cached (0 disables)")
	dur(&c.DrainDelay, "drain_delay", 5*time.Second, "SIGTERM: keep serving, unready, this long before shutting down")
	dur(&c.ShutdownGrace, "shutdown_grace", 20*time.Second, "SIGTERM: finish in-flight requests within this")
	str(&c.LogLevel, "log_level", "info", "debug | info | warn | error")

	if err := fs.Parse(args); err != nil {
		return c, err
	}

	switch {
	case c.HTTPAddr == "" || c.CHAddr == "":
		return c, fmt.Errorf("http_addr and ch_addr are required")
	case c.MaxInFlight < 1:
		return c, fmt.Errorf("max_inflight must be at least 1")
	case c.QueueWait < 0:
		return c, fmt.Errorf("queue_wait must not be negative")
	case c.QueryTimeout < 100*time.Millisecond:
		return c, fmt.Errorf("query_timeout must be at least 100ms")
	case c.ChunkRows < 1 || c.ChunkRows > maxResultRows:
		return c, fmt.Errorf("chunk_rows must be 1..%d (the api user's result-row cap)", maxResultRows)
	case c.GPUCacheTTL < 0:
		return c, fmt.Errorf("gpu_cache_ttl must not be negative")
	case c.ShutdownGrace < time.Second:
		return c, fmt.Errorf("shutdown_grace must be at least 1s")
	}
	return c, nil
}
