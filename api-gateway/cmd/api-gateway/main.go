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
	"sync/atomic"
	"syscall"
	"time"

	"api-gateway/internal/api"
	"api-gateway/internal/config"
	"api-gateway/internal/metrics"
	"api-gateway/internal/store"
)

var version = "dev" // set with -ldflags "-X main.version=..."

// The general API information for the Swagger document (make swagger).
//
//	@title			GPU Telemetry API
//	@version		0.1.0
//	@description	Read-only API over the GPU telemetry store (ClickHouse). Lists the GPUs that have reported telemetry and serves one GPU's telemetry, ordered by time.
//	@description
//	@description	Everything by default: both endpoints return all matching items unless the request sets the optional limit. truncated is true only when a limit left items out. Telemetry is streamed, so a large history arrives progressively.
//	@description
//	@description	GPU ids are the DCGM form GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx; the bare UUID is accepted too. Times are RFC 3339 with optional fractional seconds, returned in UTC with nanosecond precision.
//	@description
//	@description	Local dev: http://localhost:8080 with make api-port (a background port-forward; make api-port-stop ends it), or http://<minikube ip>:30080 through the dev NodePort.
//	@description
//	@description	No authentication: the API is read-only and reachable inside the cluster, or through a port-forward or an Ingress put in front of it. Health and metrics are on the separate admin port (9090).
//	@BasePath		/
//	@tag.name			gpus
//	@tag.description	GPUs and their telemetry
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
	err = run(ctx, cfg, func(o store.Options) (store.Store, error) { return store.Open(o) })
	stop()
	if err != nil {
		slog.Error("api-gateway failed", "err", err)
		os.Exit(1)
	}
}

// run serves until ctx ends (SIGTERM in main), then drains. openStore connects
// to ClickHouse; tests pass an in-memory store.
func run(ctx context.Context, cfg config.Config, openStore func(store.Options) (store.Store, error)) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	log.Info("telemetry api-gateway", "version", version, "addr", cfg.HTTPAddr, "clickhouse", cfg.CHAddr, "user", cfg.CHUser)

	m := metrics.New()

	ch, err := openStore(store.Options{Addr: cfg.CHAddr, Database: cfg.CHDatabase, User: cfg.CHUser, Password: cfg.CHPassword,
		Table: cfg.CHTable, GPUsTable: cfg.CHGPUsTable, MaxConns: cfg.MaxInFlight + 1, DialTimeout: 5 * time.Second,
		QueryTimeout: cfg.QueryTimeout, ChunkRows: cfg.ChunkRows})
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	defer ch.Close()

	// Readiness: ClickHouse answering and not shutting down.
	// Liveness never depends on ClickHouse: restarting the gateway cannot fix it.
	var chUp, shuttingDown atomic.Bool
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := ch.Ping(pctx)
			cancel()
			if up := err == nil; up != chUp.Swap(up) {
				if up {
					log.Info("clickhouse reachable")
				} else {
					log.Warn("clickhouse unreachable", "err", err)
				}
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
		case !chUp.Load():
			return false, "clickhouse unreachable"
		}
		return true, ""
	}
	m.Gauges = func(w io.Writer) {
		up := 0.0
		if chUp.Load() {
			up = 1
		}
		metrics.Gauge(w, "gateway_clickhouse_up", "1 when the last ClickHouse ping succeeded.", "", up)
		r := 0.0
		if ok, _ := ready(); ok {
			r = 1
		}
		metrics.Gauge(w, "gateway_ready", "1 when ClickHouse is up and not shutting down.", "", r)
	}

	if cfg.AdminAddr != "" {
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
		admin := &http.Server{Addr: cfg.AdminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("admin server", "err", err)
			}
		}()
		defer admin.Close()
	}

	gw := api.New(ch, api.Options{
		MaxInFlight: cfg.MaxInFlight, QueueWait: cfg.QueueWait, QueryTimeout: cfg.QueryTimeout, GPUCacheTTL: cfg.GPUCacheTTL,
	}, m, log)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.QueueWait + 2*cfg.QueryTimeout + 5*time.Second, // until the first chunk; a stream then moves its deadline along
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	// On SIGTERM: report unready and keep serving while the endpoints update,
	// then stop accepting and let in-flight requests finish.
	shuttingDown.Store(true)
	log.Info("draining", "delay", cfg.DrainDelay)
	time.Sleep(cfg.DrainDelay)
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Error("shutdown did not finish in time", "err", err)
		return nil
	}
	log.Info("stopped")
	return nil
}
