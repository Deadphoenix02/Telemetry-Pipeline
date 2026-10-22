package ingest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"telemetry-streamer/internal/wire"
)

func TestClientStreamRoundTripOverBufconn(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ForceServerCodec(Codec{}))
	service := NewServer(ServerConfig{
		AckInterval: 2 * time.Millisecond,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	RegisterIngestServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{cc: cc}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.OpenStream(ctx, "buf-producer")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(wire.Record{Seq: 0, Metric: "power", Value: 7}); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || ack.Seq != 0 {
		t.Fatalf("Recv() = %+v, %v; want ack 0", ack, err)
	}
	if service.Accepted.Load() != 1 {
		t.Fatalf("server accepted %d records, want 1", service.Accepted.Load())
	}
	if _, ok := service.prod["buf-producer"]; !ok {
		t.Fatal("producer metadata was not propagated")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientOpenStreamWithCancelledContext(t *testing.T) {
	client, err := Dial("passthrough:///unreachable", Identity{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream, err := client.OpenStream(ctx, "producer")
	if err != nil {
		return
	}
	defer stream.Close()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("Recv on a stream opened with a cancelled context succeeded")
	}
}

func TestClientSendsIdentityHeaders(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	got := make(chan metadata.MD, 1)
	server := grpc.NewServer(grpc.StreamInterceptor(func(_ any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		got <- md
		return nil
	}))
	RegisterIngestServer(server, NewServer(ServerConfig{AckInterval: time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{cc: cc, id: Identity{StreamerID: 7, Epoch: 4000000000}}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := client.OpenStream(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_ = stream.Send(wire.Record{})
	select {
	case md := <-got:
		if md.Get(streamerIDHeader)[0] != "7" || md.Get(epochHeader)[0] != "4000000000" || md.Get(producerIDHeader)[0] != "p" {
			t.Fatalf("metadata = %v", md)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream never reached the server")
	}
}
