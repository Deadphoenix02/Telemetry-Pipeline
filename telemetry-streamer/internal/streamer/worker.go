package streamer

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"telemetry-streamer/internal/metrics"
	"telemetry-streamer/internal/wire"
)

// Target is one broker the worker can stream to.
type Target struct {
	Name string
	Tr   wire.Transport
}

// WorkerOptions tune failover. Window must cover the slowest failover
// (AckTimeout plus one connect) so the pacer never blocks while a broker is up.
type WorkerOptions struct {
	Window         int           // max unacked records held for resend
	QueueCap       int           // pacer -> worker queue
	Home           int           // index of the preferred broker
	ConnectTimeout time.Duration // bound on one stream-open attempt
	AckTimeout     time.Duration // oldest sent record unacked this long: the broker is gone
	HomeProbe      time.Duration // while away from home, try to return this often
}

type entry struct {
	r      wire.Record
	sentAt time.Time
}

// worker owns the unacked window and the stream to one broker at a time.
//
// It never blocks: connecting, sending and receiving run in goroutines and
// report back on channels, so rows keep flowing into the window while a
// broker is replaced. Records are resent on a new stream only after the old
// one is closed, never on two streams at once: a broker drops a record whose
// sequence is below one it already holds from this producer, so overlapping
// streams could lose data.
type worker struct {
	producer string
	targets  []Target
	opt      WorkerOptions
	in       chan wire.Record
	m        *metrics.Metrics
	log      *slog.Logger
	rng      *rand.Rand
}

func newWorker(producer string, targets []Target, opt WorkerOptions, m *metrics.Metrics, log *slog.Logger) *worker {
	return &worker{
		producer: producer, targets: targets, opt: opt, m: m, log: log,
		in:  make(chan wire.Record, opt.QueueCap),
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// backoff is the pause after a round in which every broker failed.
func (w *worker) backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(min(attempt-1, 6))
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return time.Duration(float64(d) * (0.8 + 0.4*w.rng.Float64()))
}

// session is one open stream: a sender goroutine fed through sendQ, and a
// receiver goroutine delivering acks. The first error of either ends it.
type session struct {
	idx    int
	st     wire.Stream
	sendQ  chan wire.Record
	acks   chan wire.Ack
	errc   chan error
	done   chan struct{}
	cancel context.CancelFunc
}

func startSession(idx int, st wire.Stream, cancel context.CancelFunc) *session {
	s := &session{idx: idx, st: st, sendQ: make(chan wire.Record, 256), acks: make(chan wire.Ack, 64),
		errc: make(chan error, 1), done: make(chan struct{}), cancel: cancel}
	go s.sendLoop()
	go s.recvLoop()
	return s
}

func (s *session) fail(err error) {
	select {
	case s.errc <- err:
	default:
	}
}

func (s *session) sendLoop() {
	for {
		select {
		case <-s.done:
			return
		case r := <-s.sendQ:
			if err := s.st.Send(r); err != nil {
				s.fail(err)
				return
			}
		}
	}
}

func (s *session) recvLoop() {
	for {
		a, err := s.st.Recv()
		if err != nil {
			s.fail(err)
			return
		}
		select {
		case s.acks <- a:
		case <-s.done:
			return
		}
	}
}

func (s *session) close() {
	close(s.done)
	_ = s.st.Close()
	s.cancel()
}

type connResult struct {
	gen    uint64
	idx    int
	st     wire.Stream
	cancel context.CancelFunc
	err    error
}

const (
	reasonError      = "error"
	reasonAckTimeout = "ack_timeout"
	reasonHome       = "home"
)

var errConnectTimeout = errors.New("connect timeout")

// run returns the number of records still unacked (in flight or queued) when it
// exits. It exits cleanly (0) once w.in is closed and everything is acked, or
// early if ctx is cancelled (grace period over).
func (w *worker) run(ctx context.Context) int {
	n := len(w.targets)
	var (
		buf      []entry // buf[head:] are the unacked records, oldest first
		head     int
		handed   int // buf[head:head+handed] were handed to the current session
		inClosed bool

		sess *session // the one active stream
		home *session // a stream to the home broker, waiting for sess to drain (planned switch)

		connecting  bool
		connGen     uint64
		connIdx     int
		connProbe   bool // the attempt is a return-home probe, not a failover
		connCancel  context.CancelFunc
		connExpires time.Time
		connCh      = make(chan connResult, 8)

		failed       int // consecutive failed attempts in this round
		rounds       int // consecutive rounds in which every broker failed
		backoffUntil time.Time
		lastProbe    time.Time
	)
	pending := func() int { return len(buf) - head }

	connect := func(idx int, probe bool) {
		connGen++
		gen := connGen
		sctx, cancel := context.WithCancel(ctx)
		connecting, connIdx, connProbe, connCancel = true, idx, probe, cancel
		connExpires = time.Now().Add(w.opt.ConnectTimeout)
		go func() {
			st, err := w.targets[idx].Tr.OpenStream(sctx, w.producer)
			select {
			case connCh <- connResult{gen: gen, idx: idx, st: st, cancel: cancel, err: err}:
			case <-ctx.Done(): // the worker is gone
				if st != nil {
					_ = st.Close()
				}
				cancel()
			}
		}()
	}
	abandon := func() { // the in-flight attempt no longer matters
		if connecting {
			connCancel()
			connecting = false
			connGen++
		}
	}
	adopt := func(idx int, st wire.Stream, cancel context.CancelFunc) {
		sess = startSession(idx, st, cancel)
		handed = 0
		failed, rounds = 0, 0
		w.m.SetStreamConnected(true)
		w.m.SetBroker(idx)
		w.m.SetAllBrokersDown(false)
		if idx != w.opt.Home {
			lastProbe = time.Now() // the first return attempt waits HomeProbe
		}
		if pending() > 0 {
			w.m.Retries.Add(uint64(pending()))
			w.log.Info("stream established; resending unacked records in order",
				"broker", w.targets[idx].Name, "count", pending())
		}
	}
	// next tries the broker after idx, or backs off when a full round has failed.
	next := func(idx int) {
		if failed >= n {
			rounds++
			failed = 0
			backoffUntil = time.Now().Add(w.backoff(rounds))
			w.m.SetAllBrokersDown(true)
			w.log.Warn("every broker is unavailable; backing off", "round", rounds, "unacked", pending())
			return
		}
		connect((idx+1)%n, false)
	}
	// failover ends the active session; its unacked records go to the next stream.
	failover := func(reason string, err error) {
		from := sess.idx
		sess.close()
		sess = nil
		handed = 0
		w.m.SetStreamConnected(false)
		w.m.SetBroker(-1)
		w.m.Failover(reason)
		w.m.StreamErrors.Add(1)
		w.log.Warn("broker stream lost; failing over", "broker", w.targets[from].Name, "reason", reason,
			"err", err, "unacked", pending())
		if home != nil { // a ready stream to the home broker: use it now
			h := home
			home = nil
			adopt(h.idx, h.st, h.cancel)
			return
		}
		abandon()
		failed = 1
		next(from)
	}
	teardown := func() {
		abandon()
		if sess != nil {
			sess.close()
			w.m.SetStreamConnected(false)
			w.m.SetBroker(-1)
		}
		if home != nil {
			home.close()
		}
	}
	defer teardown()

	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	connect(w.opt.Home, false)

	for {
		// Intake first: take every row already waiting while the window has
		// room, so a burst of resends after a failover never delays it.
	intake:
		for !inClosed && pending() < w.opt.Window {
			select {
			case r, ok := <-w.in:
				if !ok {
					inClosed = true
					break intake
				}
				buf = append(buf, entry{r: r})
			default:
				break intake
			}
		}
		if inClosed && pending() == 0 {
			return 0
		}

		// A planned switch home happens only once everything sent on the old
		// stream is acked, so the two streams never overlap.
		if home != nil && sess != nil && handed == 0 {
			from := sess.idx
			sess.close()
			sess = nil
			h := home
			home = nil
			w.m.Failover(reasonHome)
			w.log.Info("returning to the home broker", "from", w.targets[from].Name, "to", w.targets[h.idx].Name)
			adopt(h.idx, h.st, h.cancel)
		}

		var sendQ chan wire.Record
		var nextRec wire.Record
		if sess != nil && home == nil && handed < pending() {
			sendQ, nextRec = sess.sendQ, buf[head+handed].r
		}
		inCh := w.in
		if inClosed || pending() >= w.opt.Window {
			inCh = nil // window full (or draining): stop pulling new records
		}
		var acks <-chan wire.Ack
		var errc <-chan error
		if sess != nil {
			acks, errc = sess.acks, sess.errc
		}
		var homeErr <-chan error
		if home != nil {
			homeErr = home.errc
		}

		select {
		case <-ctx.Done():
			return pending() + len(w.in)

		case r, ok := <-inCh:
			if !ok {
				inClosed = true
			} else {
				buf = append(buf, entry{r: r})
			}

		case sendQ <- nextRec:
			buf[head+handed].sentAt = time.Now()
			handed++
			w.m.RecordsSent.Add(1)

		case a := <-acks:
			k := 0
			for k < handed && buf[head+k].r.Seq <= a.Seq {
				w.m.Latency.Observe(time.Since(buf[head+k].sentAt))
				k++
			}
			if k > 0 {
				head += k
				handed -= k
				if head == len(buf) {
					buf, head = buf[:0], 0
				} else if head >= 4096 && head*2 >= len(buf) {
					buf = append(buf[:0], buf[head:]...)
					head = 0
				}
				w.m.RecordsAcked.Add(uint64(k))
			}

		case err := <-errc:
			failover(reasonError, err)

		case err := <-homeErr: // the waiting home stream died; stay where we are
			w.log.Info("home stream lost before the switch", "err", err)
			home.close()
			home = nil

		case res := <-connCh:
			if res.gen != connGen { // abandoned or timed out: discard
				if res.st != nil {
					_ = res.st.Close()
				}
				res.cancel()
				continue
			}
			connecting = false
			switch {
			case res.err != nil && connProbe:
				res.cancel() // home still down; try again after HomeProbe
			case res.err != nil:
				res.cancel()
				w.m.StreamErrors.Add(1)
				w.log.Warn("broker unavailable", "broker", w.targets[res.idx].Name, "err", res.err)
				failed++
				next(res.idx)
			case connProbe && sess != nil:
				home = startSession(res.idx, res.st, res.cancel) // drain the current stream, then switch
			default:
				adopt(res.idx, res.st, res.cancel)
			}

		case now := <-tick.C:
			switch {
			case connecting && now.After(connExpires):
				idx, probe := connIdx, connProbe
				abandon()
				if !probe {
					w.m.StreamErrors.Add(1)
					w.log.Warn("broker unavailable", "broker", w.targets[idx].Name, "err", errConnectTimeout)
					failed++
					next(idx)
				}
			case sess != nil && handed > 0 && now.Sub(buf[head].sentAt) > w.opt.AckTimeout:
				failover(reasonAckTimeout, errors.New("no ack within the ack timeout"))
			case sess == nil && !connecting && !backoffUntil.IsZero() && now.After(backoffUntil):
				backoffUntil = time.Time{}
				connect(w.opt.Home, false) // a new round, starting at home
			case sess != nil && sess.idx != w.opt.Home && home == nil && !connecting &&
				now.Sub(lastProbe) >= w.opt.HomeProbe:
				lastProbe = now
				connect(w.opt.Home, true)
			}
		}
		w.m.SetFullness(pending()+len(w.in), w.opt.Window+cap(w.in))
	}
}
