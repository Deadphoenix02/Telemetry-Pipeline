// fakeserver is a stand-in for the real ingest service, for testing the streamer.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"telemetry-streamer/internal/ingest"
)

func main() {
	listen := flag.String("listen", ":50051", "listen address")
	ackInterval := flag.Duration("ack_interval", 5*time.Millisecond, "cumulative ack cadence")
	killEvery := flag.Uint64("kill_every", 0, "fail the stream after every Nth record (0 = never)")
	printEvery := flag.Uint64("print_every", 0, "log every Nth record (0 = never)")
	statsEvery := flag.Duration("stats_interval", 5*time.Second, "stats line interval")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	srv := ingest.NewServer(ingest.ServerConfig{AckInterval: *ackInterval, KillEvery: *killEvery, PrintEvery: *printEvery, Log: log})
	gs := grpc.NewServer(grpc.ForceServerCodec(ingest.Codec{}))
	ingest.RegisterIngestServer(gs, srv)

	go func() {
		t := time.NewTicker(*statsEvery)
		defer t.Stop()
		var last uint64
		for range t.C {
			acc := srv.Accepted.Load()
			log.Info("stats", "accepted", acc, "accepted_per_sec", float64(acc-last)/statsEvery.Seconds(),
				"received", srv.Received.Load(), "dups", srv.Dups.Load(), "gaps", srv.Gaps.Load(), "kills", srv.Kills.Load())
			last = acc
		}
	}()
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, os.Interrupt)
		<-c
		gs.GracefulStop()
	}()

	log.Info("fake ingest server listening", "addr", *listen, "ack_interval", *ackInterval, "kill_every", *killEvery)
	if err := gs.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
