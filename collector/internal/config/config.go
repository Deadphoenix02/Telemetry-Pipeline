// Package config parses flags; every flag can also come from a COLLECTOR_<NAME>
// environment variable (e.g. COLLECTOR_CH_PASSWORD), which maps cleanly to Helm
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

type Config struct {
	Brokers  []string // broker gRPC addresses; the collector consumes from every one
	Group    string
	ClientID string // shown in the broker's logs; defaults to HOSTNAME

	CHAddr      string // ClickHouse native protocol host:port
	CHDatabase  string
	CHUser      string
	CHPassword  string
	CHTable     string
	CHGPUsTable string

	BatchRows     int           // a block is inserted at this many rows...
	BatchWait     time.Duration // ...or this long after its first row
	BufferBytes   int           // ...or when its rows take this much memory
	InsertTimeout time.Duration
	PoisonPolicy  string // skip | stop

	RegistryRefresh time.Duration
	HTTPAddr        string
	ShutdownGrace   time.Duration
	LogLevel        string
}

func env(name string) (string, bool) {
	return os.LookupEnv("COLLECTOR_" + strings.ToUpper(name))
}

func Parse(args []string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("collector", flag.ContinueOnError)

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

	var bufMB int
	var brokers, broker string
	str(&brokers, "brokers", "", "broker gRPC addresses host:port, comma-separated; one consumer joins each broker's group")
	str(&broker, "broker", "broker:7070", "a single broker address, used when -brokers is empty")
	str(&c.Group, "group", "collectors", "consumer group")
	str(&c.ClientID, "client_id", os.Getenv("HOSTNAME"), "client id shown in broker logs (default: HOSTNAME)")
	str(&c.CHAddr, "ch_addr", "clickhouse-telemetry-store:9000", "ClickHouse native protocol address")
	str(&c.CHDatabase, "ch_database", "default", "ClickHouse database")
	str(&c.CHUser, "ch_user", "collector", "ClickHouse user (insert-only)")
	str(&c.CHPassword, "ch_password", "", "ClickHouse password (prefer the COLLECTOR_CH_PASSWORD env var)")
	str(&c.CHTable, "ch_table", "telemetry", "telemetry table")
	str(&c.CHGPUsTable, "ch_gpus_table", "gpus", "GPU registry table")
	num(&c.BatchRows, "batch_rows", 5000, "insert a block at this many rows")
	dur(&c.BatchWait, "batch_wait", time.Second, "insert a block this long after its first row")
	num(&bufMB, "buffer_mb", 8, "insert a block when its rows take this much memory; fetching pauses meanwhile")
	dur(&c.InsertTimeout, "insert_timeout", 10*time.Second, "timeout of one insert attempt")
	str(&c.PoisonPolicy, "poison_policy", "skip", "undecodable batch: skip (count, log, commit past it) | stop (exit)")
	dur(&c.RegistryRefresh, "registry_refresh", time.Minute, "how often a GPU's last_seen is rewritten")
	str(&c.HTTPAddr, "http_addr", ":9090", "HTTP address for /healthz /readyz /metrics (empty disables)")
	dur(&c.ShutdownGrace, "shutdown_grace", 25*time.Second, "SIGTERM: flush, insert, commit and leave within this")
	str(&c.LogLevel, "log_level", "info", "debug | info | warn | error")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.BufferBytes = bufMB << 20
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.Brokers = append(c.Brokers, b)
		}
	}
	if len(c.Brokers) == 0 && broker != "" {
		c.Brokers = []string{broker}
	}

	switch {
	case len(c.Brokers) == 0 || c.CHAddr == "":
		return c, fmt.Errorf("brokers (or broker) and ch_addr are required")
	case c.BatchRows < 1:
		return c, fmt.Errorf("batch_rows must be at least 1")
	case c.BatchWait < 10*time.Millisecond:
		return c, fmt.Errorf("batch_wait must be at least 10ms")
	case bufMB < 1:
		return c, fmt.Errorf("buffer_mb must be at least 1")
	case c.PoisonPolicy != "skip" && c.PoisonPolicy != "stop":
		return c, fmt.Errorf("poison_policy must be skip or stop")
	case c.ShutdownGrace <= c.InsertTimeout:
		return c, fmt.Errorf("shutdown_grace must exceed insert_timeout so the final block can be written")
	}
	return c, nil
}
