// Package coordinator runs the broker's single consumer group: membership,
// eager rebalancing with generation fencing, and durable committed offsets.
// A commit is the consumer's ack; once it is on disk the log may delete
// everything below it.
//
// States: Empty -> PreparingRebalance -> Stable. A join, leave or expired
// session starts a rebalance. The generation only advances when the new
// assignment is computed, so members of the current generation can still
// commit what they finished while the rebalance is being prepared.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"broker/internal/metrics"
	"broker/internal/plog"
	"broker/pkg/queuepb"
)

type Options struct {
	Group             string
	OffsetsPath       string
	SessionTimeout    time.Duration // a member without a heartbeat for this long is dropped
	HeartbeatInterval time.Duration // told to members
	JoinWindow        time.Duration // a rebalance completes no sooner than this after the last join
	RebalanceTimeout  time.Duration // members that have not rejoined by then are dropped
	CommitInterval    time.Duration // group commit of the offsets file
	Tick              time.Duration // resolution of the session and rebalance timers
}

type state int

const (
	empty state = iota
	preparing
	stable
)

func (s state) String() string { return [...]string{"Empty", "PreparingRebalance", "Stable"}[s] }

type member struct {
	id, clientID  string
	lastHeartbeat time.Time
	joined        bool // rejoined during the current rebalance
	waiter        chan *queuepb.JoinGroupResponse
}

type Coordinator struct {
	queuepb.UnimplementedCoordinatorServer
	opts  Options
	store *plog.Store
	m     *metrics.Metrics
	log   *slog.Logger

	mu          sync.Mutex
	state       state
	generation  uint64
	members     map[string]*member
	owner       []string // partition -> member id in the current generation
	lastJoin    time.Time
	deadline    time.Time // rebalance timeout
	offsets     []uint64  // committed next offset per partition, 0 = never committed
	dirty       bool
	persistWait []chan error

	stop chan struct{}
	wg   sync.WaitGroup
}

// New loads the committed offsets and asks the log to delete anything they
// already cover (a crash may have come between a commit and its truncation).
func New(opts Options, store *plog.Store, m *metrics.Metrics, log *slog.Logger) (*Coordinator, error) {
	c := &Coordinator{
		opts: opts, store: store, m: m, log: log,
		members: map[string]*member{},
		owner:   make([]string, store.Partitions()),
		offsets: make([]uint64, store.Partitions()),
		stop:    make(chan struct{}),
	}
	if err := c.load(); err != nil {
		return nil, err
	}
	for p, off := range c.offsets {
		if off > 0 {
			store.RequestTruncate(p, off)
		}
	}
	return c, nil
}

type offsetsFile struct {
	Group   string            `json:"group"`
	Offsets map[string]uint64 `json:"offsets"`
}

func (c *Coordinator) load() error {
	b, err := os.ReadFile(c.opts.OffsetsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f offsetsFile
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("%s: %w", c.opts.OffsetsPath, err)
	}
	for k, v := range f.Offsets {
		p, err := strconv.Atoi(k)
		if err != nil || p < 0 || p >= len(c.offsets) {
			return fmt.Errorf("%s: unknown partition %q", c.opts.OffsetsPath, k)
		}
		c.offsets[p] = v
	}
	return nil
}

// Start runs the session/rebalance timer and the offsets group commit.
func (c *Coordinator) Start() {
	c.wg.Add(2)
	go c.tickLoop()
	go c.persistLoop()
}

// Close stops the loops after writing any pending offsets.
func (c *Coordinator) Close() error {
	close(c.stop)
	c.wg.Wait()
	return c.persist()
}

func (c *Coordinator) tickLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.opts.Tick)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.mu.Lock()
			c.expireLocked(now)
			c.maybeCompleteLocked(now)
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) persistLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.opts.CommitInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			_ = c.persist()
		}
	}
}

// persist writes the offsets file if anything changed and releases the
// commits waiting on it.
func (c *Coordinator) persist() error {
	c.mu.Lock()
	if !c.dirty {
		c.mu.Unlock()
		return nil
	}
	f := offsetsFile{Group: c.opts.Group, Offsets: map[string]uint64{}}
	for p, off := range c.offsets {
		if off > 0 {
			f.Offsets[strconv.Itoa(p)] = off
		}
	}
	waiters := c.persistWait
	c.persistWait, c.dirty = nil, false
	c.mu.Unlock()

	b, _ := json.Marshal(f)
	err := plog.WriteFileAtomic(c.opts.OffsetsPath, b)
	if err != nil {
		c.log.Error("writing offsets failed", "err", err)
		c.mu.Lock()
		c.dirty = true
		c.mu.Unlock()
	}
	for _, w := range waiters {
		w <- err
	}
	return err
}

func newMemberID(clientID string) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	if clientID == "" {
		clientID = "member"
	}
	return clientID + "-" + hex.EncodeToString(b[:])
}

// startRebalanceLocked moves to PreparingRebalance (or extends the join
// window if already there). Every member except joiner must rejoin.
func (c *Coordinator) startRebalanceLocked(now time.Time, joiner string, reason string) {
	c.lastJoin = now
	if c.state == preparing {
		return
	}
	c.state = preparing
	c.deadline = now.Add(c.opts.RebalanceTimeout)
	for id, mb := range c.members {
		mb.joined = id == joiner
	}
	c.log.Info("rebalance started", "reason", reason, "generation", c.generation, "members", len(c.members))
}

// maybeCompleteLocked forms the next generation once the join window has
// passed and every member has rejoined, or the rebalance timeout has passed.
func (c *Coordinator) maybeCompleteLocked(now time.Time) {
	if c.state != preparing || now.Sub(c.lastJoin) < c.opts.JoinWindow {
		return
	}
	all := true
	for _, mb := range c.members {
		all = all && mb.joined
	}
	if !all && now.Before(c.deadline) {
		return
	}
	var ids []string
	for id, mb := range c.members {
		if !mb.joined {
			c.log.Info("member dropped: did not rejoin before the rebalance timeout", "member", id)
			delete(c.members, id)
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	c.generation++
	c.m.Rebalances.Add(1)
	clear(c.owner)
	assigned := map[string][]uint32{}
	if len(ids) > 0 {
		for p := range c.owner {
			id := ids[p%len(ids)]
			c.owner[p] = id
			assigned[id] = append(assigned[id], uint32(p))
		}
		c.state = stable
	} else {
		c.state = empty
	}
	for _, id := range ids {
		mb := c.members[id]
		mb.lastHeartbeat = now // the session restarts with the new generation
		if mb.waiter != nil {
			mb.waiter <- &queuepb.JoinGroupResponse{
				MemberId: id, Generation: c.generation, Partitions: assigned[id],
				HeartbeatIntervalMs: uint32(c.opts.HeartbeatInterval.Milliseconds()),
				SessionTimeoutMs:    uint32(c.opts.SessionTimeout.Milliseconds()),
			}
			mb.waiter = nil
		}
	}
	c.log.Info("generation formed", "generation", c.generation, "members", len(ids), "assignment", assigned)
}

// expireLocked drops members whose session ran out. Members blocked in
// JoinGroup are exempt: they are waiting on the coordinator, not idle.
func (c *Coordinator) expireLocked(now time.Time) {
	for id, mb := range c.members {
		if mb.waiter != nil || now.Sub(mb.lastHeartbeat) <= c.opts.SessionTimeout {
			continue
		}
		c.log.Info("member session expired", "member", id)
		c.m.ConsumerSessions.Add(1)
		c.removeLocked(now, id, "session expired")
	}
}

func (c *Coordinator) removeLocked(now time.Time, id, reason string) {
	delete(c.members, id)
	if len(c.members) == 0 && c.state != preparing {
		c.state = empty
		clear(c.owner)
		c.log.Info("group empty", "reason", reason)
		return
	}
	c.startRebalanceLocked(now, "", reason)
}

func (c *Coordinator) checkGroup(g string) bool { return g == c.opts.Group }

func (c *Coordinator) JoinGroup(ctx context.Context, req *queuepb.JoinGroupRequest) (*queuepb.JoinGroupResponse, error) {
	if !c.checkGroup(req.Group) {
		return &queuepb.JoinGroupResponse{Error: queuepb.ErrorCode_UNKNOWN_GROUP}, nil
	}
	now := time.Now()
	c.mu.Lock()
	id := req.MemberId
	mb := c.members[id]
	switch {
	case id == "":
		id = newMemberID(req.ClientId)
		mb = &member{id: id, clientID: req.ClientId}
		c.members[id] = mb
	case mb == nil:
		c.mu.Unlock()
		return &queuepb.JoinGroupResponse{Error: queuepb.ErrorCode_UNKNOWN_MEMBER}, nil
	}
	mb.lastHeartbeat = now
	c.startRebalanceLocked(now, id, "member joined: "+id)
	mb.joined = true
	if mb.waiter != nil { // a duplicate join from the same member: the newest wins
		mb.waiter <- &queuepb.JoinGroupResponse{Error: queuepb.ErrorCode_REBALANCE_IN_PROGRESS}
	}
	w := make(chan *queuepb.JoinGroupResponse, 1)
	mb.waiter = w
	c.mu.Unlock()

	select {
	case resp := <-w:
		return resp, nil
	case <-ctx.Done():
		// The member gave up waiting; drop it so the generation can form without it.
		c.mu.Lock()
		if cur := c.members[id]; cur != nil && cur.waiter == w {
			c.removeLocked(time.Now(), id, "join abandoned")
		}
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.stop:
		return nil, errors.New("coordinator stopped")
	}
}

func (c *Coordinator) Heartbeat(_ context.Context, req *queuepb.HeartbeatRequest) (*queuepb.HeartbeatResponse, error) {
	if !c.checkGroup(req.Group) {
		return &queuepb.HeartbeatResponse{Error: queuepb.ErrorCode_UNKNOWN_GROUP}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	mb := c.members[req.MemberId]
	if mb == nil {
		return &queuepb.HeartbeatResponse{Error: queuepb.ErrorCode_UNKNOWN_MEMBER}, nil
	}
	mb.lastHeartbeat = time.Now()
	switch {
	case c.state == preparing:
		return &queuepb.HeartbeatResponse{Error: queuepb.ErrorCode_REBALANCE_IN_PROGRESS}, nil
	case req.Generation != c.generation:
		return &queuepb.HeartbeatResponse{Error: queuepb.ErrorCode_ILLEGAL_GENERATION}, nil
	}
	return &queuepb.HeartbeatResponse{}, nil
}

func (c *Coordinator) LeaveGroup(_ context.Context, req *queuepb.LeaveGroupRequest) (*queuepb.LeaveGroupResponse, error) {
	if !c.checkGroup(req.Group) {
		return &queuepb.LeaveGroupResponse{Error: queuepb.ErrorCode_UNKNOWN_GROUP}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[req.MemberId] == nil {
		return &queuepb.LeaveGroupResponse{Error: queuepb.ErrorCode_UNKNOWN_MEMBER}, nil
	}
	c.log.Info("member left", "member", req.MemberId)
	c.removeLocked(time.Now(), req.MemberId, "member left: "+req.MemberId)
	return &queuepb.LeaveGroupResponse{}, nil
}

// CommitOffset is fenced: only the current owner of every partition in the
// request, in the current generation, may commit, and never past what is
// fsynced. The reply is sent once the offsets file is on disk, and the log
// then deletes the consumed entries.
func (c *Coordinator) CommitOffset(ctx context.Context, req *queuepb.CommitOffsetRequest) (*queuepb.CommitOffsetResponse, error) {
	reject := func(code queuepb.ErrorCode) (*queuepb.CommitOffsetResponse, error) {
		c.m.CommitsRejected.Add(1)
		return &queuepb.CommitOffsetResponse{Error: code}, nil
	}
	if !c.checkGroup(req.Group) {
		return reject(queuepb.ErrorCode_UNKNOWN_GROUP)
	}
	c.mu.Lock()
	switch {
	case c.members[req.MemberId] == nil:
		c.mu.Unlock()
		return reject(queuepb.ErrorCode_UNKNOWN_MEMBER)
	case req.Generation != c.generation:
		c.mu.Unlock()
		return reject(queuepb.ErrorCode_ILLEGAL_GENERATION)
	}
	for _, po := range req.Offsets {
		if int(po.Partition) >= len(c.owner) || c.owner[po.Partition] != req.MemberId {
			c.mu.Unlock()
			return reject(queuepb.ErrorCode_NOT_ASSIGNED)
		}
		if po.Offset > c.store.Partition(int(po.Partition)).HW()+1 {
			c.mu.Unlock()
			return reject(queuepb.ErrorCode_OFFSET_OUT_OF_RANGE)
		}
	}
	for _, po := range req.Offsets {
		if po.Offset < c.offsets[po.Partition] {
			c.log.Warn("offset commit moved backwards", "partition", po.Partition,
				"from", c.offsets[po.Partition], "to", po.Offset, "member", req.MemberId)
		}
		c.offsets[po.Partition] = po.Offset
	}
	c.dirty = true
	w := make(chan error, 1)
	c.persistWait = append(c.persistWait, w)
	c.mu.Unlock()

	select {
	case err := <-w:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.m.CommitsAccepted.Add(1)
	for _, po := range req.Offsets {
		c.store.RequestTruncate(int(po.Partition), po.Offset)
	}
	return &queuepb.CommitOffsetResponse{}, nil
}

// committedOffset is where a consumer of p starts: the committed offset, or
// the first entry on disk if nothing was committed (or it was deleted).
func (c *Coordinator) committedOffset(p int) uint64 {
	first := c.store.Partition(p).First()
	if off := c.offsets[p]; off > first {
		return off
	}
	return first
}

func (c *Coordinator) FetchOffsets(_ context.Context, req *queuepb.FetchOffsetsRequest) (*queuepb.FetchOffsetsResponse, error) {
	if !c.checkGroup(req.Group) {
		return &queuepb.FetchOffsetsResponse{Error: queuepb.ErrorCode_UNKNOWN_GROUP}, nil
	}
	parts := req.Partitions
	if len(parts) == 0 {
		for p := range c.owner {
			parts = append(parts, uint32(p))
		}
	}
	resp := &queuepb.FetchOffsetsResponse{}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range parts {
		if int(p) >= len(c.offsets) {
			continue
		}
		resp.Offsets = append(resp.Offsets, &queuepb.PartitionOffset{Partition: p, Offset: c.committedOffset(int(p))})
	}
	return resp, nil
}

// PartitionLag is one partition's backlog for /lag.
type PartitionLag struct {
	Partition     int    `json:"partition"`
	Lag           uint64 `json:"lag"`
	Committed     uint64 `json:"committed_offset"`
	HighWatermark uint64 `json:"high_watermark"`
}

// Lag is the group's backlog in records: per partition, the records appended
// minus the record index at the committed offset.
type Lag struct {
	Group      string         `json:"group"`
	Lag        uint64         `json:"lag"`
	Partitions []PartitionLag `json:"partitions"`
}

func (c *Coordinator) Lag() Lag {
	c.mu.Lock()
	committed := make([]uint64, len(c.offsets))
	for p := range committed {
		committed[p] = c.committedOffset(p)
	}
	c.mu.Unlock()
	out := Lag{Group: c.opts.Group}
	for p, off := range committed {
		part := c.store.Partition(p)
		end, at := part.RecordEnd(), part.RecordBaseAt(off)
		var lag uint64
		if end > at {
			lag = end - at
		}
		out.Lag += lag
		out.Partitions = append(out.Partitions, PartitionLag{Partition: p, Lag: lag, Committed: off, HighWatermark: part.HW()})
	}
	return out
}

// WriteGauges writes group gauges for /metrics.
func (c *Coordinator) WriteGauges(w io.Writer) {
	c.mu.Lock()
	members, gen, st := len(c.members), c.generation, c.state
	c.mu.Unlock()
	metrics.Gauge(w, "broker_group_members", "Members of the consumer group.", "", float64(members))
	metrics.Gauge(w, "broker_group_generation", "Current consumer group generation.", "", float64(gen))
	metrics.Gauge(w, "broker_group_stable", "1 when the group is Stable (not rebalancing).", "", map[bool]float64{true: 1}[st == stable])
	lag := c.Lag()
	help := "Records the group has not committed, per partition."
	for _, pl := range lag.Partitions {
		metrics.Gauge(w, "broker_group_lag_records", help, fmt.Sprintf(`partition="%d"`, pl.Partition), float64(pl.Lag))
		help = ""
	}
}
