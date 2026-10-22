// Package config parses flags; every flag can also come from a STREAMER_<NAME>
// environment variable (e.g. STREAMER_RATE_ROWS_PER_SEC), which maps cleanly to
// Helm values later. Flags win over env.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const MaxRate = 100000

type Config struct {
	CSVPath        string
	Rate           int
	Tick           time.Duration
	Seed           int64 // 0 = random (the seed used is logged)
	Mode           string
	MaxMemoryBytes int64
	Grace          time.Duration
	MetricsAddr    string
	StatsInterval  time.Duration
	Targets        []string // broker gRPC addresses; empty = discard records
	StreamerID     int      // producer identity sent to the broker; -1 = unset

	// Failover between brokers (see internal/streamer/worker.go).
	Window           time.Duration // unacked data held for resend, as time at the rate
	WindowMaxRecords int
	ConnectTimeout   time.Duration
	AckTimeout       time.Duration
	HomeProbe        time.Duration
	Verbose          bool
	LogLevel         string
}

func env(name string) (string, bool) {
	return os.LookupEnv("STREAMER_" + strings.ToUpper(name))
}

func Parse(args []string) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("streamer", flag.ContinueOnError)

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
	boolean := func(p *bool, name string, def bool, usage string) {
		if v, ok := env(name); ok {
			if b, err := strconv.ParseBool(v); err == nil {
				def = b
			}
		}
		fs.BoolVar(p, name, def, usage)
	}

	var seed, mem int
	var targets, target string
	str(&c.CSVPath, "csv_path", "", "path to the telemetry CSV (required)")
	num(&c.Rate, "rate_rows_per_sec", 1000, "rows per second for this instance (1..100000)")
	dur(&c.Tick, "tick", 10*time.Millisecond, "pacing micro-tick; each tick emits the rows accrued since the last")
	num(&seed, "seed", 0, "seed for the random start offset (0 = random)")
	str(&c.Mode, "mode", "auto", "auto | memory | stream (auto = memory unless over budget)")
	num(&mem, "max_memory_mb", 16, "memory budget for parsed rows (16 bytes/row + series table)")
	dur(&c.Grace, "grace", 30*time.Second, "shutdown flush/ack deadline")
	str(&c.MetricsAddr, "metrics_addr", ":9100", "metrics/health listen address (empty disables)")
	dur(&c.StatsInterval, "stats_interval", 5*time.Second, "how often to print a stats line to stdout (0 disables)")
	str(&targets, "targets", "", "broker gRPC addresses host:port, comma-separated; the home broker is streamer_id mod their count (empty = discard records)")
	str(&target, "target", "", "a single broker address; same as -targets with one entry")
	dur(&c.Window, "window", 3*time.Second, "unacked data held for resend, as time at the rate; must outlast a failover (ack_timeout + connect_timeout)")
	num(&c.WindowMaxRecords, "window_max_records", 60000, "cap on the window in records (about 250 bytes each)")
	dur(&c.ConnectTimeout, "connect_timeout", time.Second, "one attempt to open a stream to a broker")
	dur(&c.AckTimeout, "ack_timeout", 2*time.Second, "fail over when the oldest sent record is unacked this long")
	dur(&c.HomeProbe, "home_probe", 10*time.Second, "while on another broker, try to return to the home broker this often")
	num(&c.StreamerID, "streamer_id", -1, "producer id 0..65535 sent to the broker (default: the pod ordinal from HOSTNAME, e.g. telemetry-streamer-3 -> 3)")
	boolean(&c.Verbose, "verbose", false, "print one line per record (discard mode only; slow at high rates)")
	str(&c.LogLevel, "log_level", "info", "debug | info | warn | error")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	for _, t := range strings.Split(targets, ",") {
		if t = strings.TrimSpace(t); t != "" {
			c.Targets = append(c.Targets, t)
		}
	}
	if len(c.Targets) == 0 && target != "" {
		c.Targets = []string{target}
	}
	c.Seed = int64(seed)
	c.MaxMemoryBytes = int64(mem) << 20
	if c.StreamerID < 0 {
		if id, ok := ordinal(os.Getenv("HOSTNAME")); ok {
			c.StreamerID = id
		}
	}

	switch {
	case c.CSVPath == "":
		return c, fmt.Errorf("csv_path is required")
	case c.Rate < 1 || c.Rate > MaxRate:
		return c, fmt.Errorf("rate_rows_per_sec must be in 1..%d", MaxRate)
	case c.Tick < time.Millisecond || c.Tick > time.Second:
		return c, fmt.Errorf("tick must be in 1ms..1s")
	case c.Mode != "auto" && c.Mode != "memory" && c.Mode != "stream":
		return c, fmt.Errorf("mode must be auto, memory or stream")
	case c.StreamerID > 65535:
		return c, fmt.Errorf("streamer_id must be in 0..65535")
	case len(c.Targets) > 0 && c.StreamerID < 0:
		return c, fmt.Errorf("streamer_id is required with -targets (or run as a StatefulSet pod named <name>-<ordinal>)")
	case c.ConnectTimeout < 10*time.Millisecond:
		return c, fmt.Errorf("connect_timeout must be at least 10ms")
	case c.AckTimeout < 100*time.Millisecond:
		return c, fmt.Errorf("ack_timeout must be at least 100ms")
	case c.HomeProbe < 100*time.Millisecond:
		return c, fmt.Errorf("home_probe must be at least 100ms")
	case c.Window < c.AckTimeout+c.ConnectTimeout:
		return c, fmt.Errorf("window must be at least ack_timeout + connect_timeout, or a failover would pause the streamer")
	case c.WindowMaxRecords < 100:
		return c, fmt.Errorf("window_max_records must be at least 100")
	}
	return c, nil
}

// ordinal returns N from a StatefulSet pod name "<name>-N".
func ordinal(hostname string) (int, bool) {
	i := strings.LastIndexByte(hostname, '-')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(hostname[i+1:])
	return n, err == nil && n >= 0
}
