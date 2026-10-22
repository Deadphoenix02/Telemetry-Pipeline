package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"broker/internal/config"
	"broker/internal/coordinator"
	"broker/internal/fetch"
	"broker/internal/ingest"
	"broker/internal/ingestpb"
	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/queuepb"
)

var version = "dev" // set with -ldflags "-X main.version=..."

const maxMsgBytes = 4 << 20

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
		slog.Error("broker failed", "err", err)
		os.Exit(1)
	}
}

// run serves until ctx ends (SIGTERM in main), then drains and stops.
func run(ctx context.Context, cfg config.Config) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	log.Info("broker", "version", version, "partitions", cfg.Partitions, "data_dir", cfg.DataDir)

	m := metrics.New()
	var ready atomic.Bool

	// HTTP comes up first so liveness answers during recovery; /readyz turns
	// 200 only once the log is recovered and gRPC is serving.
	var coord atomic.Pointer[coordinator.Coordinator]
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/lag", func(w http.ResponseWriter, _ *http.Request) {
		c := coord.Load()
		if c == nil {
			http.Error(w, "recovering", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.Lag())
	})
	hs := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
		}
	}()
	defer hs.Close()

	start := time.Now()
	store, err := plog.Open(plog.Options{
		Dir: cfg.DataDir, Partitions: cfg.Partitions, SegmentSize: cfg.SegmentBytes,
		FlushInterval: cfg.FlushInterval, TruncateInterval: cfg.TruncateInterval, QuotaBytes: cfg.QuotaBytes,
	}, m, log)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	store.Start()
	log.Info("log recovered", "took", time.Since(start).Round(time.Millisecond), "disk_bytes", store.DiskUsed())

	c, err := coordinator.New(coordinator.Options{
		Group: cfg.Group, OffsetsPath: filepath.Join(cfg.DataDir, "offsets.json"),
		SessionTimeout: cfg.SessionTimeout, HeartbeatInterval: cfg.HeartbeatInterval,
		JoinWindow: cfg.JoinWindow, RebalanceTimeout: cfg.RebalanceTimeout,
		CommitInterval: 50 * time.Millisecond, Tick: 100 * time.Millisecond,
	}, store, m, log)
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("load offsets: %w", err)
	}
	c.Start()
	coord.Store(c)

	ing := ingest.New(ingest.Options{
		Linger: cfg.Linger, MaxFrameBytes: cfg.MaxFrameBytes, MaxProducers: cfg.MaxProducers,
		ProducerIdleTTL: time.Hour, RebuildTail: 1024, DrainTimeout: cfg.DrainTimeout,
	}, store, m, log)
	stopping := make(chan struct{})
	fs := fetch.New(fetch.Options{DefaultMaxBytes: cfg.FetchMaxBytes, MaxMaxBytes: 2 << 20, DefaultMaxWait: cfg.FetchMaxWait},
		store, m, stopping)
	m.Gauges = func(w io.Writer) {
		store.WriteGauges(w)
		c.WriteGauges(w)
		metrics.Gauge(w, "broker_producers", "Producers in the dedupe table.", "", float64(ing.Producers()))
		r := 0.0
		if ready.Load() {
			r = 1
		}
		metrics.Gauge(w, "broker_ready", "1 once recovery is done and gRPC is serving.", "", r)
	}

	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgBytes), grpc.MaxSendMsgSize(maxMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	ingestpb.RegisterIngestServer(gs, ing)
	queuepb.RegisterBrokerServer(gs, fs)
	queuepb.RegisterCoordinatorServer(gs, c)
	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		_ = c.Close()
		_ = store.Close()
		return err
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	ready.Store(true)
	log.Info("serving", "grpc", cfg.Listen, "http", cfg.HTTPAddr)

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-store.Fatal():
		runErr = fmt.Errorf("fsync failed, acknowledged writes can no longer be guaranteed: %w", err)
	case err := <-serveErr:
		runErr = fmt.Errorf("grpc server: %w", err)
	}

	// Shutdown order: stop being ready, end fetches, let producer streams write
	// and ack what they hold (the flusher is still running), stop gRPC, then
	// persist offsets and run the final fsync.
	ready.Store(false)
	close(stopping)
	dctx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	ing.Drain(dctx)
	cancel()
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second): // e.g. members blocked in JoinGroup
		gs.Stop()
	}
	if err := c.Close(); err != nil {
		log.Error("persisting offsets at shutdown", "err", err)
	}
	if err := store.Close(); err != nil {
		log.Error("closing log", "err", err)
	}
	log.Info("stopped")
	return runErr
}
