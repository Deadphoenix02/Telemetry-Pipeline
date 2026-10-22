package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseDefaultsAndOverrides(t *testing.T) {
	t.Setenv("STREAMER_CSV_PATH", "from-env.csv")
	t.Setenv("STREAMER_RATE_ROWS_PER_SEC", "250")
	t.Setenv("STREAMER_TICK", "20ms")
	t.Setenv("STREAMER_SEED", "17")
	t.Setenv("STREAMER_MAX_MEMORY_MB", "8")
	t.Setenv("STREAMER_GRACE", "2s")
	t.Setenv("STREAMER_METRICS_ADDR", "")
	t.Setenv("STREAMER_STATS_INTERVAL", "0")
	t.Setenv("STREAMER_TARGET", "localhost:50051")
	t.Setenv("STREAMER_STREAMER_ID", "3")
	t.Setenv("STREAMER_VERBOSE", "true")
	t.Setenv("STREAMER_LOG_LEVEL", "debug")

	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.CSVPath != "from-env.csv" || c.Rate != 250 || c.Tick != 20*time.Millisecond {
		t.Fatalf("unexpected parsed basics: %+v", c)
	}
	if c.Seed != 17 || c.MaxMemoryBytes != 8<<20 || c.Grace != 2*time.Second {
		t.Fatalf("unexpected parsed memory or timing settings: %+v", c)
	}
	if c.MetricsAddr != "" || c.StatsInterval != 0 || len(c.Targets) != 1 || c.Targets[0] != "localhost:50051" || !c.Verbose || c.LogLevel != "debug" {
		t.Fatalf("unexpected parsed optional settings: %+v", c)
	}

	c, err = Parse([]string{"-csv_path", "from-flag.csv", "-rate_rows_per_sec", "500", "-verbose=false"})
	if err != nil {
		t.Fatal(err)
	}
	if c.CSVPath != "from-flag.csv" || c.Rate != 500 || c.Verbose {
		t.Fatalf("flags did not override environment: %+v", c)
	}
}

func TestParseInvalidEnvironmentUsesDefaults(t *testing.T) {
	t.Setenv("STREAMER_CSV_PATH", "valid.csv")
	t.Setenv("STREAMER_RATE_ROWS_PER_SEC", "not-an-int")
	t.Setenv("STREAMER_TICK", "not-a-duration")
	t.Setenv("STREAMER_SEED", "bad")
	t.Setenv("STREAMER_MAX_MEMORY_MB", "bad")
	t.Setenv("STREAMER_VERBOSE", "bad")

	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Rate != 1000 || c.Tick != 10*time.Millisecond || c.Seed != 0 || c.MaxMemoryBytes != 16<<20 || c.Verbose {
		t.Fatalf("invalid environment values should retain defaults: %+v", c)
	}
}

func TestParseValidationAndFlagErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing path", want: "csv_path is required"},
		{name: "rate too low", args: []string{"-csv_path", "x", "-rate_rows_per_sec", "0"}, want: "rate_rows_per_sec"},
		{name: "rate too high", args: []string{"-csv_path", "x", "-rate_rows_per_sec", "100001"}, want: "rate_rows_per_sec"},
		{name: "tick too low", args: []string{"-csv_path", "x", "-tick", "999us"}, want: "tick must"},
		{name: "tick too high", args: []string{"-csv_path", "x", "-tick", "2s"}, want: "tick must"},
		{name: "invalid mode", args: []string{"-csv_path", "x", "-mode", "disk"}, want: "mode must"},
		{name: "malformed flag", args: []string{"-unknown"}, want: "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name != "missing path" && tt.name != "malformed flag" {
				t.Setenv("STREAMER_CSV_PATH", "")
			}
			_, err := Parse(tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse(%v) error = %v, want substring %q", tt.args, err, tt.want)
			}
		})
	}
}

func TestStreamerID(t *testing.T) {
	t.Setenv("HOSTNAME", "telemetry-streamer-4")
	c, err := Parse([]string{"-csv_path", "x.csv", "-target", "broker:7070"})
	if err != nil || c.StreamerID != 4 {
		t.Fatalf("from HOSTNAME: id=%d err=%v", c.StreamerID, err)
	}
	c, err = Parse([]string{"-csv_path", "x.csv", "-target", "broker:7070", "-streamer_id", "9"})
	if err != nil || c.StreamerID != 9 {
		t.Fatalf("flag wins: id=%d err=%v", c.StreamerID, err)
	}
	t.Setenv("HOSTNAME", "laptop")
	if _, err := Parse([]string{"-csv_path", "x.csv", "-target", "broker:7070"}); err == nil {
		t.Fatal("target without a streamer id was accepted")
	}
	if _, err := Parse([]string{"-csv_path", "x.csv"}); err != nil {
		t.Fatalf("discard mode needs no id: %v", err)
	}
	if _, err := Parse([]string{"-csv_path", "x.csv", "-streamer_id", "70000"}); err == nil {
		t.Fatal("streamer_id 70000 accepted")
	}
}

func TestTargetsAndFailoverFlags(t *testing.T) {
	c, err := Parse([]string{"-csv_path", "x.csv", "-streamer_id", "4",
		"-targets", " broker-0.h:7070, broker-1.h:7070 ,,broker-2.h:7070", "-target", "ignored:1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Targets) != 3 || c.Targets[0] != "broker-0.h:7070" || c.Targets[2] != "broker-2.h:7070" {
		t.Fatalf("targets %q", c.Targets)
	}
	if c.Window != 3*time.Second || c.WindowMaxRecords != 60000 || c.ConnectTimeout != time.Second ||
		c.AckTimeout != 2*time.Second || c.HomeProbe != 10*time.Second {
		t.Fatalf("failover defaults: %+v", c)
	}
	// -target alone is a one-broker list.
	if c, err = Parse([]string{"-csv_path", "x.csv", "-streamer_id", "0", "-target", "broker:7070"}); err != nil ||
		len(c.Targets) != 1 || c.Targets[0] != "broker:7070" {
		t.Fatalf("single target: %q %v", c.Targets, err)
	}
}

func TestFailoverValidation(t *testing.T) {
	base := []string{"-csv_path", "x.csv", "-streamer_id", "0", "-targets", "a:1,b:1"}
	for _, extra := range [][]string{
		{"-window", "2s"}, // shorter than ack_timeout + connect_timeout: a failover would pause
		{"-ack_timeout", "10ms"},
		{"-connect_timeout", "1ms"},
		{"-home_probe", "1ms"},
		{"-window_max_records", "10"},
	} {
		if _, err := Parse(append(append([]string{}, base...), extra...)); err == nil {
			t.Errorf("%v accepted", extra)
		}
	}
	t.Setenv("HOSTNAME", "laptop") // no pod ordinal to derive a streamer_id from
	if _, err := Parse([]string{"-csv_path", "x.csv", "-targets", "a:1"}); err == nil {
		t.Error("targets without a streamer_id accepted")
	}
}
