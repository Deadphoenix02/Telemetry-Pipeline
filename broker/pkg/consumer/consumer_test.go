package consumer

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"broker/internal/coordinator"
	"broker/internal/fetch"
	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
	"broker/pkg/queuepb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const parts = 4

// broker is an in-process broker (log, coordinator, fetch) on an in-memory listener.
type broker struct {
	store *plog.Store
	coord *coordinator.Coordinator
	cc    *grpc.ClientConn
}

func startBroker(t *testing.T) *broker {
	t.Helper()
	dir := t.TempDir()
	m := metrics.New()
	store, err := plog.Open(plog.Options{Dir: dir, Partitions: parts, SegmentSize: 1 << 16, FlushInterval: 2 * time.Millisecond,
		TruncateInterval: time.Hour, QuotaBytes: 1 << 30}, m, quiet)
	if err != nil {
		t.Fatal(err)
	}
	store.Start()
	coord, err := coordinator.New(coordinator.Options{Group: "g", OffsetsPath: filepath.Join(dir, "offsets.json"),
		SessionTimeout: 600 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, JoinWindow: 30 * time.Millisecond,
		RebalanceTimeout: time.Second, CommitInterval: 2 * time.Millisecond, Tick: 5 * time.Millisecond}, store, m, quiet)
	if err != nil {
		t.Fatal(err)
	}
	coord.Start()
	stopping := make(chan struct{})
	gs := grpc.NewServer()
	queuepb.RegisterCoordinatorServer(gs, coord)
	queuepb.RegisterBrokerServer(gs, fetch.New(fetch.Options{DefaultMaxBytes: 1 << 16, MaxMaxBytes: 1 << 20, DefaultMaxWait: 50 * time.Millisecond}, store, m, stopping))
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(stopping); cc.Close(); gs.Stop(); coord.Close(); store.Close() })
	return &broker{store: store, coord: coord, cc: cc}
}

// produce appends n one-record frames to every partition.
func (b *broker) produce(t *testing.T, n int) {
	for p := 0; p < parts; p++ {
		for i := 0; i < n; i++ {
			var fb frame.Builder
			fb.Reset(uint16(p), 0, 0)
			_ = fb.Add(uint64(i), &frame.Record{Metric: "m"})
			if _, err := b.store.Append(context.Background(), p, fb.Bytes()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// seen records which (partition, offset) pairs a set of consumers received.
type seen struct {
	mu  sync.Mutex
	got map[[2]uint64]int
	by  map[string]map[uint32]bool // client -> partitions it consumed from
}

func newSeen() *seen { return &seen{got: map[[2]uint64]int{}, by: map[string]map[uint32]bool{}} }

func (s *seen) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

// handler consumes batches, checks each frame and commits after every batch.
func (s *seen) handler(t *testing.T, client string) Handler {
	return func(ctx context.Context, sess *Session) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case b, ok := <-sess.Batches:
				if !ok {
					return nil
				}
				if _, err := frame.Verify(b.Frame); err != nil {
					t.Errorf("partition %d offset %d: %v", b.Partition, b.Offset, err)
				}
				s.mu.Lock()
				s.got[[2]uint64{uint64(b.Partition), b.Offset}]++
				if s.by[client] == nil {
					s.by[client] = map[uint32]bool{}
				}
				s.by[client][b.Partition] = true
				s.mu.Unlock()
				if err := sess.Commit(context.Background(), map[uint32]uint64{b.Partition: b.Next}); err != nil && err != ErrFenced {
					t.Errorf("commit: %v", err)
				}
			}
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConsumesEverythingCommitsAndLogIsDeleted(t *testing.T) {
	b := startBroker(t)
	b.produce(t, 20)
	s := newSeen()
	ctx, cancel := context.WithCancel(context.Background())
	c := New(b.cc, Config{Group: "g", ClientID: "a", FetchMaxWait: 50 * time.Millisecond, Log: quiet})
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, s.handler(t, "a")) }()
	t.Cleanup(func() { cancel(); <-done }) // runs before the broker's cleanup closes the connection
	eventually(t, "all 80 frames", func() bool { return s.count() == 80 })
	eventually(t, "zero lag", func() bool { return b.coord.Lag().Lag == 0 })
	// The last commit's reply (and its truncation request) follows the
	// offsets fsync, which can trail the in-memory lag by a group commit.
	eventually(t, "every partition deleted up to its commit", func() bool {
		b.store.TruncateNow()
		for p := 0; p < parts; p++ {
			if b.store.Partition(p).First() != 21 {
				return false
			}
		}
		return true
	})
}

func TestRebalanceHandsPartitionsOverWithoutLoss(t *testing.T) {
	b := startBroker(t)
	s := newSeen()
	b.produce(t, 10)
	actx, stopA := context.WithCancel(context.Background())
	a := New(b.cc, Config{Group: "g", ClientID: "a", FetchMaxWait: 50 * time.Millisecond, Log: quiet})
	aDone := make(chan error, 1)
	go func() { aDone <- a.Run(actx, s.handler(t, "a")) }()
	eventually(t, "a consumes the first batch", func() bool { return s.count() == 40 })

	bctx, stopB := context.WithCancel(context.Background())
	bc := New(b.cc, Config{Group: "g", ClientID: "b", FetchMaxWait: 50 * time.Millisecond, Log: quiet})
	bDone := make(chan error, 1)
	go func() { bDone <- bc.Run(bctx, s.handler(t, "b")) }()
	defer func() { stopB(); <-bDone }() // before the connection closes in cleanup
	eventually(t, "two members of two partitions each", func() bool {
		return a.Stats.Assigned.Load() == 2 && bc.Stats.Assigned.Load() == 2
	})
	b.produce(t, 10)
	eventually(t, "second batch", func() bool { return s.count() == 80 })

	stopA() // a leaves: b takes every partition
	<-aDone
	eventually(t, "b owns all partitions", func() bool { return bc.Stats.Assigned.Load() == parts })
	b.produce(t, 10)
	eventually(t, "third batch", func() bool { return s.count() == 120 })
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.by["b"]) != parts {
		t.Fatalf("b consumed from %v, want all partitions", s.by["b"])
	}
	for k, n := range s.got {
		if n > 2 {
			t.Fatalf("partition %d offset %d delivered %d times", k[0], k[1], n)
		}
	}
}

func TestCommitAfterGenerationEndsIsFenced(t *testing.T) {
	b := startBroker(t)
	b.produce(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessions := make(chan *Session, 4)
	c := New(b.cc, Config{Group: "g", ClientID: "a", Log: quiet})
	go func() {
		_ = c.Run(ctx, func(sctx context.Context, s *Session) error {
			sessions <- s
			<-sctx.Done()
			return nil
		})
	}()
	first := <-sessions
	other := New(b.cc, Config{Group: "g", ClientID: "z", Log: quiet})
	go func() { _ = other.Run(ctx, func(sctx context.Context, s *Session) error { <-sctx.Done(); return nil }) }()
	second := <-sessions // a rejoined in the next generation
	if second.Generation <= first.Generation {
		t.Fatalf("generations %d then %d", first.Generation, second.Generation)
	}
	if err := first.Commit(context.Background(), map[uint32]uint64{first.Partitions[0]: 2}); err != ErrFenced {
		t.Fatalf("commit with the old generation: %v, want ErrFenced", err)
	}
}
