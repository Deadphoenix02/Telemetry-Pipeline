package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"broker/pkg/consumer"

	"collector/internal/collector"
	"collector/internal/config"
	"collector/internal/metrics"
	"collector/internal/sink"
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
	err = run(ctx, cfg, func(o sink.Options) (sink.Sink, error) { return sink.Open(o) })
	stop()
	if err != nil {
		slog.Error("collector failed", "err", err)
		os.Exit(1)
	}
}

// run consumes until ctx ends (SIGTERM in main), then drains: insert, commit,
// leave. openSink connects to ClickHouse; tests pass a fake.
func run(ctx context.Context, cfg config.Config, openSink func(sink.Options) (sink.Sink, error)) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	log.Info("telemetry collector", "version", version, "brokers", cfg.Brokers, "clickhouse", cfg.CHAddr, "client_id", cfg.ClientID)

	m := metrics.New()

	ch, err := openSink(sink.Options{Addr: cfg.CHAddr, Database: cfg.CHDatabase, User: cfg.CHUser, Password: cfg.CHPassword,
		Table: cfg.CHTable, GPUsTable: cfg.CHGPUsTable, DialTimeout: 5 * time.Second,
		MaxConns: len(cfg.Brokers) + 1}) // one insert per broker's session at a time, plus the registry
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	defer ch.Close()

	// Each broker is a whole, independent queue with its own consumer group, so
	// the collector joins every one of them; a broker that is down only stops
	// its own consumer, which keeps retrying.
	consumers := make([]*consumer.Consumer, len(cfg.Brokers))
	for i, addr := range cfg.Brokers {
		cc, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
		if err != nil {
			return fmt.Errorf("broker %s: %w", addr, err)
		}
		defer cc.Close()
		consumers[i] = consumer.New(cc, consumer.Config{Group: cfg.Group, ClientID: cfg.ClientID,
			FetchMaxWait: 500 * time.Millisecond, Log: log.With("broker", addr)})
	}
	joined := func() (n uint64) { // generations joined, over every broker
		for _, c := range consumers {
			n += c.Stats.Rebalances.Load()
		}
		return n
	}

	// Readiness: joined the group, ClickHouse answering, not shutting down.
	// Liveness never depends on ClickHouse: restarting a collector cannot fix it.
	var chUp, shuttingDown atomic.Bool
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := ch.Ping(pctx)
			cancel()
			if up := err == nil; up != chUp.Swap(up) && !up {
				log.Warn("clickhouse unreachable", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	ready := func() (bool, string) {
		switch {
		case shuttingDown.Load():
			return false, "shutting down"
		case joined() == 0:
			return false, "not in any broker's consumer group yet"
		case !chUp.Load():
			return false, "clickhouse unreachable"
		}
		return true, ""
	}
	m.Gauges = func(w io.Writer) {
		var assigned int64
		for _, c := range consumers {
			assigned += c.Stats.Assigned.Load()
		}
		metrics.Gauge(w, "collector_assigned_partitions", "Partitions this member owns, over every broker.", "", float64(assigned))
		metrics.Gauge(w, "collector_rebalances", "Generations this member has joined, over every broker.", "", float64(joined()))
		help := "1 while this member is in the broker's consumer group." // printed with the first series only
		for i, c := range consumers {
			in := 0.0
			if c.Stats.InGroup.Load() {
				in = 1
			}
			metrics.Gauge(w, "collector_broker_in_group", help, fmt.Sprintf("broker=%q", cfg.Brokers[i]), in)
			help = ""
		}
		up := 0.0
		if chUp.Load() {
			up = 1
		}
		metrics.Gauge(w, "collector_clickhouse_up", "1 when the last ClickHouse ping succeeded.", "", up)
		r := 0.0
		if ok, _ := ready(); ok {
			r = 1
		}
		metrics.Gauge(w, "collector_ready", "1 when joined, ClickHouse is up and not shutting down.", "", r)
	}
	if cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			if ok, why := ready(); !ok {
				http.Error(w, why, http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("ready\n"))
		})
		srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server", "err", err)
			}
		}()
		defer srv.Close()
	}

	regCtx, stopReg := context.WithCancel(context.Background())
	reg := collector.NewRegistry(ch, cfg.RegistryRefresh, time.Second, m, log)
	var regDone sync.WaitGroup
	regDone.Add(1)
	go func() { defer regDone.Done(); reg.Run(regCtx) }()

	col := collector.New(collector.Options{
		BatchRows: cfg.BatchRows, BatchWait: cfg.BatchWait, BufferBytes: cfg.BufferBytes,
		InsertTimeout: cfg.InsertTimeout, PoisonPolicy: cfg.PoisonPolicy,
	}, ch, reg, m, log)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var fatal error
	go func() {
		select {
		case fatal = <-col.Fatal():
			cancelRun()
		case <-runCtx.Done():
		}
	}()
	// On SIGTERM: stop fetching, insert and commit what is buffered, leave the
	// group so the others rebalance at once, then exit; a watchdog bounds it.
	drained := make(chan struct{})
	defer close(drained)
	go func() {
		<-runCtx.Done()
		shuttingDown.Store(true)
		log.Info("draining: insert, commit, leave")
		select {
		case <-drained:
		case <-time.After(cfg.ShutdownGrace):
			log.Error("drain exceeded the shutdown grace; exiting")
			os.Exit(1)
		}
	}()

	var consuming sync.WaitGroup
	for _, c := range consumers {
		consuming.Add(1)
		go func() { defer consuming.Done(); _ = c.Run(runCtx, col.Handle) }()
	}
	consuming.Wait()
	stopReg()
	regDone.Wait()
	log.Info("stopped")
	return fatal
}
