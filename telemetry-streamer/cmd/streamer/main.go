package main

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"telemetry-streamer/internal/config"
	"telemetry-streamer/internal/ingest"
	"telemetry-streamer/internal/metrics"
	"telemetry-streamer/internal/source"
	"telemetry-streamer/internal/streamer"
	"telemetry-streamer/internal/wire"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	err = run(ctx, cfg)
	stop()
	if err != nil {
		slog.Error("streamer failed", "err", err)
		os.Exit(1)
	}
}

// run streams until ctx ends (SIGTERM in main), then flushes and waits for acks.
func run(ctx context.Context, cfg config.Config) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	log.Info("telemetry streamer", "version", version)

	m := metrics.New()
	mal := source.NewMalformed(log, time.Second)
	m.MalformedFn = mal.Count
	go func() { // readiness drops the moment SIGTERM arrives
		<-ctx.Done()
		m.SetShuttingDown()
	}()

	// HTTP comes up FIRST so liveness answers while a big CSV is still loading;
	// /readyz stays 503 until the CSV is loaded and a stream is connected.
	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		mux.Handle("/readyz", m.ReadyHandler())
		srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		defer srv.Close() // closed only after the shutdown flush, so /readyz is served throughout
	}

	seed := cfg.Seed
	if seed == 0 {
		var b [8]byte
		_, _ = crand.Read(b[:])
		seed = int64(binary.LittleEndian.Uint64(b[:]) >> 1)
	}
	log.Info("start offset seed", "seed", seed)

	src, err := source.Open(source.Options{
		Path:           cfg.CSVPath,
		Mode:           cfg.Mode,
		MaxMemoryBytes: cfg.MaxMemoryBytes,
	}, mal, rand.New(rand.NewSource(seed)), log)
	if err != nil {
		return fmt.Errorf("load csv: %w", err)
	}
	defer src.Close()
	m.SetCSVLoaded()
	log.Info("csv ready", "source", src.Describe(), "malformed_rows", mal.Count())

	id := ingest.Identity{StreamerID: uint16(max(cfg.StreamerID, 0)), Epoch: newEpoch()}
	var targets []streamer.Target
	for _, addr := range cfg.Targets {
		c, err := ingest.Dial(addr, id)
		if err != nil {
			return fmt.Errorf("grpc dial %s: %w", addr, err)
		}
		defer c.Close()
		targets = append(targets, streamer.Target{Name: addr, Tr: c})
	}
	home := 0
	if len(targets) > 0 {
		home = int(id.StreamerID) % len(targets)
		log.Info("sending to gRPC ingest", "brokers", cfg.Targets, "home", cfg.Targets[home],
			"streamer_id", id.StreamerID, "epoch", id.Epoch)
	} else {
		targets = []streamer.Target{{Name: "discard", Tr: &wire.Discard{Verbose: cfg.Verbose, Log: log}}}
		log.Info("no -targets set: records are discarded")
	}

	if cfg.StatsInterval > 0 {
		go printStats(ctx, log, m, mal, cfg.StatsInterval)
	}

	s := streamer.New(streamer.Options{
		ProducerID: fmt.Sprintf("streamer-%d-%08x", id.StreamerID, id.Epoch),
		Rate:       cfg.Rate,
		Tick:       cfg.Tick,
		Grace:      cfg.Grace,

		Home:             home,
		Window:           cfg.Window,
		WindowMaxRecords: cfg.WindowMaxRecords,
		ConnectTimeout:   cfg.ConnectTimeout,
		AckTimeout:       cfg.AckTimeout,
		HomeProbe:        cfg.HomeProbe,
	}, src, targets, m, log)
	return s.Run(ctx)
}

func printStats(ctx context.Context, log *slog.Logger, m *metrics.Metrics, mal *source.Malformed, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rows := m.RowsEmitted.Load()
			log.Info("stats",
				"rows_emitted", rows,
				"rows_per_sec", float64(rows-last)/every.Seconds(),
				"records_sent", m.RecordsSent.Load(),
				"records_acked", m.RecordsAcked.Load(),
				"window_used", m.Fullness(),
				"retries", m.Retries.Load(),
				"blocked_seconds", m.Blocked().Seconds(),
				"malformed_rows", mal.Count())
			last = rows
		}
	}
}

// newEpoch picks a random epoch per process start; with the streamer ID it
// identifies this producer to the broker.
func newEpoch() uint32 {
	var b [4]byte
	_, _ = crand.Read(b[:])
	return binary.LittleEndian.Uint32(b[:])
}
