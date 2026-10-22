package ingest

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"telemetry-streamer/internal/wire"
)

const (
	streamMethod     = "/telemetry.v1.Ingest/Stream"
	producerIDHeader = "x-producer-id"
	streamerIDHeader = "x-streamer-id"
	epochHeader      = "x-epoch"
)

// Identity is how the broker recognises this producer: the streamer's pod
// ordinal and a random epoch picked once per process start. The broker drops
// resent records by (streamer_id, epoch, seq), so a restarted process (new
// epoch, seq from 0) is a new producer.
type Identity struct {
	StreamerID uint16
	Epoch      uint32
}

var streamDesc = &grpc.StreamDesc{StreamName: "Stream", ClientStreams: true, ServerStreams: true}

// Client is a wire.Transport backed by a gRPC connection.
type Client struct {
	cc *grpc.ClientConn
	id Identity
}

// Dial prepares a client for target (host:port). The connection is lazy: it is
// established on the first stream, and OpenStream fails fast while it is down.
// TLS is not wired in yet (plaintext only).
//
// Keepalive pings notice a hung connection (a lost node rather than a closed
// socket); the broker allows one every 10 s. Reconnect backoff is capped at
// 2 s instead of gRPC's 120 s, so a broker that comes back is usable again
// within seconds, which the return to the home broker relies on.
func Dial(target string, id Identity) (*Client, error) {
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
			MinConnectTimeout: time.Second,
		}))
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc, id: id}, nil
}

func (c *Client) Close() error { return c.cc.Close() }

func (c *Client) OpenStream(ctx context.Context, producerID string) (wire.Stream, error) {
	sctx, cancel := context.WithCancel(ctx)
	sctx = metadata.AppendToOutgoingContext(sctx, producerIDHeader, producerID,
		streamerIDHeader, strconv.FormatUint(uint64(c.id.StreamerID), 10),
		epochHeader, strconv.FormatUint(uint64(c.id.Epoch), 10))
	cs, err := c.cc.NewStream(sctx, streamDesc, streamMethod, grpc.ForceCodec(Codec{}))
	if err != nil {
		cancel()
		return nil, err
	}
	return &clientStream{cs: cs, cancel: cancel}, nil
}

type clientStream struct {
	cs     grpc.ClientStream
	cancel context.CancelFunc
}

func (s *clientStream) Send(r wire.Record) error { return s.cs.SendMsg(&r) }

func (s *clientStream) Recv() (wire.Ack, error) {
	var a wire.Ack
	err := s.cs.RecvMsg(&a)
	return a, err
}

// Close cancels the stream, which unblocks any Send/Recv in other goroutines.
func (s *clientStream) Close() error {
	s.cancel()
	return nil
}
