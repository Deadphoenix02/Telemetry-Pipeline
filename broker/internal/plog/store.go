// Package plog is the broker's write-ahead log: one tidwall/wal log per
// partition, a group-commit flusher that publishes the high watermark, a disk
// quota that pauses producers, and a truncator that deletes consumed entries.
package plog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"broker/internal/metrics"
	"broker/pkg/frame"
	"broker/pkg/partitioner"
)

type Options struct {
	Dir              string // data directory; the WAL lives in Dir/wal/<partition>
	Partitions       int
	SegmentSize      int           // bytes per segment file (tidwall keeps the tail segment in memory)
	FlushInterval    time.Duration // group-commit fsync interval
	TruncateInterval time.Duration // how often committed prefixes are deleted
	QuotaBytes       int64         // producers pause at this many bytes on disk
	ResumeRatio      float64       // and resume below QuotaBytes*ResumeRatio
}

// topicMeta is persisted once and checked on every start: the partition count
// and the hash decide where every GPU's data lives.
type topicMeta struct {
	Partitions int    `json:"partitions"`
	Format     int    `json:"format"`
	Hash       string `json:"hash"`
}

type Store struct {
	opts  Options
	parts []*Partition
	m     *metrics.Metrics
	log   *slog.Logger

	qmu       sync.Mutex
	used      int64
	blocked   bool
	unblocked chan struct{} // closed when the quota clears

	hwMu sync.Mutex
	hwCh chan struct{} // closed and replaced whenever a high watermark advances

	fatal chan error // fsync failures: durability is gone, the process must stop
	stop  chan struct{}
	wg    sync.WaitGroup
}

// Open checks topic.json and recovers every partition.
func Open(opts Options, m *metrics.Metrics, log *slog.Logger) (*Store, error) {
	if opts.ResumeRatio <= 0 || opts.ResumeRatio > 1 {
		opts.ResumeRatio = 0.9
	}
	if err := os.MkdirAll(filepath.Join(opts.Dir, "wal"), 0o750); err != nil {
		return nil, err
	}
	if err := checkTopic(opts); err != nil {
		return nil, err
	}
	s := &Store{opts: opts, m: m, log: log, hwCh: make(chan struct{}), fatal: make(chan error, 1), stop: make(chan struct{})}
	for i := 0; i < opts.Partitions; i++ {
		p, err := openPartition(i, filepath.Join(opts.Dir, "wal", strconv.Itoa(i)), opts.SegmentSize, m, log)
		if err != nil {
			for _, q := range s.parts {
				_ = q.close()
			}
			return nil, err
		}
		s.parts = append(s.parts, p)
		s.used += p.Bytes()
	}
	s.updateQuotaLocked()
	return s, nil
}

func checkTopic(opts Options) error {
	want := topicMeta{Partitions: opts.Partitions, Format: frame.Version, Hash: partitioner.Name}
	path := filepath.Join(opts.Dir, "topic.json")
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		out, _ := json.Marshal(want)
		return writeFileAtomic(path, out)
	}
	if err != nil {
		return err
	}
	var got topicMeta
	if err := json.Unmarshal(b, &got); err != nil {
		return fmt.Errorf("topic.json: %w", err)
	}
	if got != want {
		return fmt.Errorf("topic.json has %+v but the broker is configured for %+v; "+
			"changing the partition count or hash remaps GPUs and breaks per-GPU ordering", got, want)
	}
	return nil
}

// writeFileAtomic writes path via a temp file, fsync and rename, then fsyncs
// the directory so the rename survives power loss.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// WriteFileAtomic is exported for the coordinator's offsets file.
func WriteFileAtomic(path string, data []byte) error { return writeFileAtomic(path, data) }

// Start runs the flusher and the truncator until Close.
func (s *Store) Start() {
	s.wg.Add(2)
	go s.flushLoop()
	go s.truncateLoop()
}

// Fatal delivers an fsync failure; after one, acknowledged durability can no
// longer be promised and the broker should exit.
func (s *Store) Fatal() <-chan error { return s.fatal }

// Close stops the loops, runs a final fsync and closes every partition.
func (s *Store) Close() error {
	close(s.stop)
	s.wg.Wait()
	s.Flush()
	var errs []error
	for _, p := range s.parts {
		errs = append(errs, p.close())
	}
	return errors.Join(errs...)
}

func (s *Store) Partitions() int            { return len(s.parts) }
func (s *Store) Partition(i int) *Partition { return s.parts[i] }

// Append waits for the disk quota, then writes frame f (from frame.Builder)
// to partition p. It blocks while the quota is exceeded, which is how the
// broker pushes back on producers.
func (s *Store) Append(ctx context.Context, p int, f []byte) (uint64, error) {
	if err := s.waitQuota(ctx); err != nil {
		return 0, err
	}
	idx, size, err := s.parts[p].append(f)
	if err != nil {
		return 0, err
	}
	s.addUsage(int64(size))
	return idx, nil
}

func (s *Store) waitQuota(ctx context.Context) error {
	var start time.Time
	for {
		s.qmu.Lock()
		if !s.blocked {
			s.qmu.Unlock()
			if !start.IsZero() {
				s.m.QuotaBlockedNs.Add(int64(time.Since(start)))
			}
			return nil
		}
		ch := s.unblocked
		s.qmu.Unlock()
		if start.IsZero() {
			start = time.Now()
		}
		select {
		case <-ch:
		case <-ctx.Done():
			s.m.QuotaBlockedNs.Add(int64(time.Since(start)))
			return ctx.Err()
		}
	}
}

func (s *Store) addUsage(delta int64) {
	s.qmu.Lock()
	s.used += delta
	s.updateQuotaLocked()
	s.qmu.Unlock()
}

func (s *Store) updateQuotaLocked() {
	switch {
	case !s.blocked && s.used >= s.opts.QuotaBytes:
		s.blocked = true
		s.unblocked = make(chan struct{})
		s.log.Warn("disk quota reached; pausing producers", "used", s.used, "quota", s.opts.QuotaBytes)
	case s.blocked && float64(s.used) <= float64(s.opts.QuotaBytes)*s.opts.ResumeRatio:
		s.blocked = false
		close(s.unblocked)
		s.log.Info("disk quota cleared; resuming producers", "used", s.used)
	}
}

// DiskUsed is the WAL's size on disk; QuotaBlocked reports whether producers are paused.
func (s *Store) DiskUsed() int64 {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.used
}

func (s *Store) QuotaBlocked() bool {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.blocked
}

// HWChanged returns a channel that is closed the next time any partition's
// high watermark advances.
func (s *Store) HWChanged() <-chan struct{} {
	s.hwMu.Lock()
	defer s.hwMu.Unlock()
	return s.hwCh
}

func (s *Store) notifyHW() {
	s.hwMu.Lock()
	close(s.hwCh)
	s.hwCh = make(chan struct{})
	s.hwMu.Unlock()
}

// Flush fsyncs every dirty partition now and publishes the new watermarks.
func (s *Store) Flush() {
	advanced := false
	for _, p := range s.parts {
		ok, err := p.sync()
		if err != nil {
			s.log.Error("fsync failed", "err", err)
			select {
			case s.fatal <- err:
			default:
			}
			continue
		}
		advanced = advanced || ok
	}
	if advanced {
		s.notifyHW()
	}
}

func (s *Store) flushLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.Flush()
		}
	}
}

// RequestTruncate records that every entry below offset has been consumed by
// all groups. The truncator deletes them on its next pass.
func (s *Store) RequestTruncate(p int, offset uint64) {
	w := &s.parts[p].wantTrunc
	for {
		cur := w.Load()
		if offset <= cur || w.CompareAndSwap(cur, offset) {
			return
		}
	}
}

func (s *Store) truncateLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.TruncateInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.TruncateNow()
		}
	}
}

// TruncateNow runs one truncation pass over every partition.
func (s *Store) TruncateNow() {
	for _, p := range s.parts {
		delta, err := p.truncate()
		if err != nil {
			s.m.TruncateErrors.Add(1)
			s.log.Error("truncate failed", "err", err)
			continue
		}
		if delta != 0 {
			s.m.Truncations.Add(1)
			s.addUsage(delta)
		}
	}
}

// WriteGauges writes per-partition and disk gauges for /metrics.
func (s *Store) WriteGauges(w io.Writer) {
	metrics.Gauge(w, "broker_disk_used_bytes", "WAL bytes on disk.", "", float64(s.DiskUsed()))
	metrics.Gauge(w, "broker_disk_quota_bytes", "Producers pause at this many WAL bytes.", "", float64(s.opts.QuotaBytes))
	blocked := 0.0
	if s.QuotaBlocked() {
		blocked = 1
	}
	metrics.Gauge(w, "broker_quota_blocked", "1 while producers are paused at the disk quota.", "", blocked)
	series := []struct {
		name, help string
		val        func(*Partition) float64
	}{
		{"broker_partition_high_watermark", "Last fsynced offset.", func(p *Partition) float64 { return float64(p.HW()) }},
		{"broker_partition_first_offset", "First offset still on disk.", func(p *Partition) float64 { return float64(p.First()) }},
		{"broker_partition_records_end", "Records ever appended to the partition (since its log was last empty).", func(p *Partition) float64 { return float64(p.RecordEnd()) }},
		{"broker_partition_bytes", "Partition bytes on disk.", func(p *Partition) float64 { return float64(p.Bytes()) }},
	}
	for _, sr := range series {
		help := sr.help
		for _, p := range s.parts {
			metrics.Gauge(w, sr.name, help, fmt.Sprintf(`partition="%d"`, p.ID), sr.val(p))
			help = ""
		}
	}
}
