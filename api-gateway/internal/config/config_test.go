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
	if c.HTTPAddr != ":8080" || c.AdminAddr != ":9090" || c.CHUser != "api" || c.MaxInFlight != 4 ||
		c.ChunkRows != 10000 || c.GPUCacheTTL != 5*time.Second || c.QueryTimeout != 15*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestEnvAndFlagPrecedence(t *testing.T) {
	t.Setenv("GATEWAY_CH_PASSWORD", "s3cret")
	t.Setenv("GATEWAY_CHUNK_ROWS", "500")
	t.Setenv("GATEWAY_MAX_INFLIGHT", "2")
	c, err := Parse([]string{"-chunk_rows", "700"})
	if err != nil {
		t.Fatal(err)
	}
	if c.CHPassword != "s3cret" || c.ChunkRows != 700 || c.MaxInFlight != 2 {
		t.Fatalf("password=%q chunk_rows=%d max_inflight=%d", c.CHPassword, c.ChunkRows, c.MaxInFlight)
	}
}

func TestValidation(t *testing.T) {
	for _, args := range [][]string{
		{"-ch_addr", ""},
		{"-http_addr", ""},
		{"-max_inflight", "0"},
		{"-query_timeout", "1ms"},
		{"-chunk_rows", "100001"}, // over the api user's result-row cap
		{"-chunk_rows", "0"},
		{"-gpu_cache_ttl", "-1s"},
		{"-shutdown_grace", "10ms"},
	} {
		if _, err := Parse(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
