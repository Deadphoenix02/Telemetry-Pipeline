package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Partitions != 10 || c.SegmentBytes != 1<<20 || c.QuotaBytes != 256<<20 || c.FlushInterval != 20*time.Millisecond ||
		c.FetchMaxBytes != 256<<10 || c.Group != "collectors" || c.RebalanceTimeout != 15*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestEnvAndFlagPrecedence(t *testing.T) {
	t.Setenv("BROKER_QUOTA_MB", "64")
	t.Setenv("BROKER_PARTITIONS", "4")
	c, err := Parse([]string{"-partitions", "6"})
	if err != nil {
		t.Fatal(err)
	}
	if c.QuotaBytes != 64<<20 || c.Partitions != 6 {
		t.Fatalf("quota=%d partitions=%d", c.QuotaBytes, c.Partitions)
	}
}

func TestValidation(t *testing.T) {
	for _, args := range [][]string{
		{"-partitions", "0"},
		{"-segment_kb", "8"},
		{"-quota_mb", "1"},
		{"-flush_interval", "0s"},
		{"-max_frame_kb", "0"},
		{"-fetch_max_kb", "4096"},
		{"-rebalance_timeout", "5s"},
		{"-heartbeat_interval", "5s"},
	} {
		if _, err := Parse(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
