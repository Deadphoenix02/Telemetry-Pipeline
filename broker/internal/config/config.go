// Package config parses flags; every flag can also come from a BROKER_<NAME>
// environment variable (e.g. BROKER_QUOTA_MB), which maps cleanly to Helm
// values. Flags win over env.
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
	Listen   string // gRPC: Ingest, Broker, Coordinator
	HTTPAddr string // /healthz /readyz /metrics /lag
	DataDir  string
	LogLevel string

	Partitions       int
	SegmentBytes     int
	FlushInterval    time.Duration
	TruncateInterval time.Duration
	QuotaBytes       int64

	Linger        time.Duration
	MaxFrameBytes int
	MaxProducers  int

	FetchMaxBytes int
	FetchMaxWait  time.Duration

	Group             string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	JoinWindow        time.Duration
	RebalanceTimeout  time.Duration

	DrainTimeout time.Duration
}

func env(name string) (string, bool) {
	return os.LookupEnv("BROKER_" + strings.ToUpper(name))
}

func Parse(args []string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)

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

	var segKB, quotaMB, frameKB, fetchKB int
	str(&c.Listen, "listen", ":7070", "gRPC listen address (Ingest, Broker, Coordinator)")
	str(&c.HTTPAddr, "http_addr", ":8080", "HTTP listen address for /healthz /readyz /metrics /lag")
	str(&c.DataDir, "data_dir", "/data", "data directory (WAL, offsets, topic.json)")
	str(&c.LogLevel, "log_level", "info", "debug | info | warn | error")
	num(&c.Partitions, "partitions", 10, "partition count; fixed once the data directory exists")
	num(&segKB, "segment_kb", 1024, "WAL segment size; the active segment of each partition is held in memory")
	dur(&c.FlushInterval, "flush_interval", 20*time.Millisecond, "group-commit fsync interval; producer acks wait for it")
	dur(&c.TruncateInterval, "truncate_interval", time.Second, "how often committed (consumed) entries are deleted")
	num(&quotaMB, "quota_mb", 256, "WAL disk quota; producers pause when it is reached")
	dur(&c.Linger, "linger", 10*time.Millisecond, "how long received records wait for more before their frames are written")
	num(&frameKB, "max_frame_kb", 64, "a partition's frame is written as soon as it reaches this size")
	num(&c.MaxProducers, "max_producers", 1024, "producer dedupe entries kept")
	num(&fetchKB, "fetch_max_kb", 256, "default bytes per Fetch response")
	dur(&c.FetchMaxWait, "fetch_max_wait", 500*time.Millisecond, "default long-poll wait of an idle Fetch")
	str(&c.Group, "group", "collectors", "the consumer group's name")
	dur(&c.SessionTimeout, "session_timeout", 10*time.Second, "a consumer without a heartbeat for this long is dropped")
	dur(&c.HeartbeatInterval, "heartbeat_interval", 3*time.Second, "heartbeat interval told to consumers")
	dur(&c.JoinWindow, "join_window", time.Second, "a rebalance waits this long after the last join")
	dur(&c.RebalanceTimeout, "rebalance_timeout", 15*time.Second, "consumers that have not rejoined by then are dropped")
	dur(&c.DrainTimeout, "drain_timeout", 10*time.Second, "at shutdown, how long producer streams may wait for final acks")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.SegmentBytes = segKB << 10
	c.QuotaBytes = int64(quotaMB) << 20
	c.MaxFrameBytes = frameKB << 10
	c.FetchMaxBytes = fetchKB << 10

	switch {
	case c.Partitions < 1 || c.Partitions > 1024:
		return c, fmt.Errorf("partitions must be in 1..1024")
	case segKB < 64:
		return c, fmt.Errorf("segment_kb must be at least 64")
	case quotaMB < 16:
		return c, fmt.Errorf("quota_mb must be at least 16")
	case c.FlushInterval < time.Millisecond:
		return c, fmt.Errorf("flush_interval must be at least 1ms")
	case frameKB < 1 || frameKB > 1024:
		return c, fmt.Errorf("max_frame_kb must be in 1..1024")
	case fetchKB < 1 || fetchKB > 2048:
		return c, fmt.Errorf("fetch_max_kb must be in 1..2048 (gRPC messages are capped at 4 MiB)")
	case c.RebalanceTimeout <= c.SessionTimeout:
		return c, fmt.Errorf("rebalance_timeout must exceed session_timeout so busy members can rejoin")
	case c.HeartbeatInterval*3 > c.SessionTimeout:
		return c, fmt.Errorf("session_timeout must be at least 3 heartbeat intervals")
	}
	return c, nil
}
