package coordinator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/frame"
	"broker/pkg/queuepb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const group = "collectors"

func opts(dir string) Options {
	return Options{
		Group: group, OffsetsPath: filepath.Join(dir, "offsets.json"),
		SessionTimeout: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		JoinWindow: 30 * time.Millisecond, RebalanceTimeout: 600 * time.Millisecond,
		CommitInterval: 5 * time.Millisecond, Tick: 5 * time.Millisecond,
	}
}

type env struct {
	t     *testing.T
	dir   string
	store *plog.Store
	c     *Coordinator
}

func newEnv(t *testing.T, dir string) *env {
	t.Helper()
	m := metrics.New()
	store, err := plog.Open(plog.Options{Dir: dir, Partitions: 4, SegmentSize: 1 << 16, FlushInterval: time.Hour,
		TruncateInterval: time.Hour, QuotaBytes: 1 << 30}, m, quiet)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(opts(dir), store, m, quiet)
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	return &env{t: t, dir: dir, store: store, c: c}
}

func (e *env) close() {
	_ = e.c.Close()
	_ = e.store.Close()
}

// fill appends n single-record frames to every partition and fsyncs them.
func (e *env) fill(n int) {
	for p := 0; p < e.store.Partitions(); p++ {
		for i := 0; i < n; i++ {
			var b frame.Builder
			b.Reset(uint16(p), 0, 0)
			_ = b.Add(uint64(i), &frame.Record{Metric: "m"})
			if _, err := e.store.Append(context.Background(), p, b.Bytes()); err != nil {
				e.t.Fatal(err)
			}
		}
	}
	e.store.Flush()
}

func (e *env) join(id, client string) <-chan *queuepb.JoinGroupResponse {
	ch := make(chan *queuepb.JoinGroupResponse, 1)
	go func() {
		r, err := e.c.JoinGroup(context.Background(), &queuepb.JoinGroupRequest{Group: group, MemberId: id, ClientId: client})
		if err != nil {
			r = &queuepb.JoinGroupResponse{Error: queuepb.ErrorCode(-1)}
		}
		ch <- r
	}()
	return ch
}

func wait(t *testing.T, ch <-chan *queuepb.JoinGroupResponse) *queuepb.JoinGroupResponse {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("JoinGroup did not return")
		return nil
	}
}

func (e *env) hb(id string, gen uint64) queuepb.ErrorCode {
	r, _ := e.c.Heartbeat(context.Background(), &queuepb.HeartbeatRequest{Group: group, MemberId: id, Generation: gen})
	return r.Error
}

func (e *env) commit(id string, gen uint64, offs map[uint32]uint64) queuepb.ErrorCode {
	req := &queuepb.CommitOffsetRequest{Group: group, MemberId: id, Generation: gen}
	for p, o := range offs {
		req.Offsets = append(req.Offsets, &queuepb.PartitionOffset{Partition: p, Offset: o})
	}
	r, err := e.c.CommitOffset(context.Background(), req)
	if err != nil {
		e.t.Fatal(err)
	}
	return r.Error
}

func TestSingleMemberGetsEverything(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	r := wait(t, e.join("", "pod-a"))
	if r.Error != 0 || r.Generation != 1 || len(r.Partitions) != 4 || r.MemberId == "" {
		t.Fatalf("join: %+v", r)
	}
	if code := e.hb(r.MemberId, 1); code != 0 {
		t.Fatalf("heartbeat: %v", code)
	}
}

func TestSecondMemberRebalancesAndOldGenerationCanStillCommit(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	e.fill(5)
	a := wait(t, e.join("", "a"))
	bCh := e.join("", "b")
	time.Sleep(10 * time.Millisecond)
	if code := e.hb(a.MemberId, a.Generation); code != queuepb.ErrorCode_REBALANCE_IN_PROGRESS {
		t.Fatalf("heartbeat during rebalance: %v", code)
	}
	// a finishes its in-flight insert and commits with the generation it has
	if code := e.commit(a.MemberId, a.Generation, map[uint32]uint64{0: 3, 3: 2}); code != 0 {
		t.Fatalf("commit during PreparingRebalance: %v", code)
	}
	a2 := wait(t, e.join(a.MemberId, "a"))
	b := wait(t, bCh)
	if a2.Generation != 2 || b.Generation != 2 {
		t.Fatalf("generations a=%d b=%d", a2.Generation, b.Generation)
	}
	got := append(append([]uint32{}, a2.Partitions...), b.Partitions...)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(a2.Partitions) != 2 || len(b.Partitions) != 2 || len(got) != 4 || got[0] != 0 || got[3] != 3 {
		t.Fatalf("assignment a=%v b=%v", a2.Partitions, b.Partitions)
	}
	// the generation moved on: the old one is fenced
	if code := e.commit(a.MemberId, 1, map[uint32]uint64{a2.Partitions[0]: 4}); code != queuepb.ErrorCode_ILLEGAL_GENERATION {
		t.Fatalf("stale-generation commit: %v", code)
	}
	if code := e.commit(a.MemberId, 2, map[uint32]uint64{b.Partitions[0]: 4}); code != queuepb.ErrorCode_NOT_ASSIGNED {
		t.Fatalf("commit for another member's partition: %v", code)
	}
	if code := e.commit(a.MemberId, 2, map[uint32]uint64{a2.Partitions[0]: 7}); code != queuepb.ErrorCode_OFFSET_OUT_OF_RANGE {
		t.Fatalf("commit past the high watermark: %v", code)
	}
	if code := e.commit(a.MemberId, 2, map[uint32]uint64{a2.Partitions[0]: 6}); code != 0 {
		t.Fatalf("commit at hw+1: %v", code)
	}
}

func TestExpiredSessionHandsPartitionsOver(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	aCh, bCh := e.join("", "a"), e.join("", "b")
	a, b := wait(t, aCh), wait(t, bCh)
	if a.Generation != b.Generation || len(a.Partitions)+len(b.Partitions) != 4 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	// b keeps heartbeating; a goes silent
	deadline := time.Now().Add(2 * time.Second)
	for e.hb(b.MemberId, b.Generation) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a's session never expired")
		}
		time.Sleep(20 * time.Millisecond)
	}
	b2 := wait(t, e.join(b.MemberId, "b"))
	if len(b2.Partitions) != 4 || b2.Generation != b.Generation+1 {
		t.Fatalf("after expiry: %+v", b2)
	}
	if code := e.hb(a.MemberId, a.Generation); code != queuepb.ErrorCode_UNKNOWN_MEMBER {
		t.Fatalf("expired member heartbeat: %v", code)
	}
}

func TestLeaveTriggersRebalance(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	aCh, bCh := e.join("", "a"), e.join("", "b")
	a, b := wait(t, aCh), wait(t, bCh)
	start := time.Now()
	if r, _ := e.c.LeaveGroup(context.Background(), &queuepb.LeaveGroupRequest{Group: group, MemberId: a.MemberId}); r.Error != 0 {
		t.Fatalf("leave: %v", r.Error)
	}
	if code := e.hb(b.MemberId, b.Generation); code != queuepb.ErrorCode_REBALANCE_IN_PROGRESS {
		t.Fatalf("heartbeat after leave: %v", code)
	}
	b2 := wait(t, e.join(b.MemberId, "b"))
	if len(b2.Partitions) != 4 || time.Since(start) > 300*time.Millisecond {
		t.Fatalf("after leave: %+v in %v", b2, time.Since(start))
	}
}

func TestMemberThatDoesNotRejoinIsDroppedAtTimeout(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	aCh, bCh := e.join("", "a"), e.join("", "b")
	a, b := wait(t, aCh), wait(t, bCh)
	cCh := e.join("", "c")
	stop := make(chan struct{})
	go func() { // b heartbeats but never rejoins (e.g. stuck in a long insert)
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				e.hb(b.MemberId, b.Generation)
			}
		}
	}()
	defer close(stop)
	a2 := wait(t, e.join(a.MemberId, "a"))
	c := wait(t, cCh)
	if len(a2.Partitions)+len(c.Partitions) != 4 {
		t.Fatalf("a=%v c=%v", a2.Partitions, c.Partitions)
	}
	if code := e.hb(b.MemberId, b.Generation); code != queuepb.ErrorCode_UNKNOWN_MEMBER {
		t.Fatalf("dropped member heartbeat: %v", code)
	}
}

func TestUnknownMemberMustRejoinFresh(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	if r := wait(t, e.join("ghost-123", "x")); r.Error != queuepb.ErrorCode_UNKNOWN_MEMBER {
		t.Fatalf("join with unknown id: %+v", r)
	}
	if r, _ := e.c.JoinGroup(context.Background(), &queuepb.JoinGroupRequest{Group: "other"}); r.Error != queuepb.ErrorCode_UNKNOWN_GROUP {
		t.Fatalf("join other group: %+v", r)
	}
}

func TestCommitIsDurableAndDeletesConsumedEntries(t *testing.T) {
	dir := t.TempDir()
	e := newEnv(t, dir)
	e.fill(10)
	a := wait(t, e.join("", "a"))
	if code := e.commit(a.MemberId, a.Generation, map[uint32]uint64{1: 8}); code != 0 {
		t.Fatal(code)
	}
	e.store.TruncateNow()
	if first := e.store.Partition(1).First(); first != 8 {
		t.Fatalf("first offset after commit = %d, want 8", first)
	}
	e.close()

	e = newEnv(t, dir) // restart: membership is gone, offsets are not
	defer e.close()
	r, _ := e.c.FetchOffsets(context.Background(), &queuepb.FetchOffsetsRequest{Group: group})
	got := map[uint32]uint64{}
	for _, po := range r.Offsets {
		got[po.Partition] = po.Offset
	}
	if got[1] != 8 || got[0] != 1 || len(got) != 4 {
		t.Fatalf("offsets after restart: %v", got)
	}
	if code := e.hb(a.MemberId, a.Generation); code != queuepb.ErrorCode_UNKNOWN_MEMBER {
		t.Fatalf("pre-restart member heartbeat: %v", code)
	}
}

func TestLagCountsUncommittedRecords(t *testing.T) {
	e := newEnv(t, t.TempDir())
	defer e.close()
	e.fill(10) // 10 single-record frames per partition
	if l := e.c.Lag(); l.Lag != 40 {
		t.Fatalf("lag before any commit = %d, want 40", l.Lag)
	}
	a := wait(t, e.join("", "a"))
	_ = e.commit(a.MemberId, a.Generation, map[uint32]uint64{0: 11, 1: 6})
	l := e.c.Lag()
	if l.Lag != 25 || l.Partitions[0].Lag != 0 || l.Partitions[1].Lag != 5 {
		t.Fatalf("lag after commits: %+v", l)
	}
}
