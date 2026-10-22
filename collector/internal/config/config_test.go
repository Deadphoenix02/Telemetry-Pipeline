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
	if len(c.Brokers) != 1 || c.Brokers[0] != "broker:7070" || c.BatchRows != 5000 || c.BatchWait != time.Second || c.BufferBytes != 8<<20 ||
		c.CHUser != "collector" || c.PoisonPolicy != "skip" || c.ShutdownGrace != 25*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestEnvAndFlagPrecedence(t *testing.T) {
	t.Setenv("COLLECTOR_CH_PASSWORD", "s3cret")
	t.Setenv("COLLECTOR_BATCH_ROWS", "100")
	c, err := Parse([]string{"-batch_rows", "200"})
	if err != nil {
		t.Fatal(err)
	}
	if c.CHPassword != "s3cret" || c.BatchRows != 200 {
		t.Fatalf("password=%q batch_rows=%d", c.CHPassword, c.BatchRows)
	}
}

func TestValidation(t *testing.T) {
	for _, args := range [][]string{
		{"-broker", ""},
		{"-batch_rows", "0"},
		{"-batch_wait", "1ms"},
		{"-buffer_mb", "0"},
		{"-poison_policy", "halt"},
		{"-shutdown_grace", "5s"},
	} {
		if _, err := Parse(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
