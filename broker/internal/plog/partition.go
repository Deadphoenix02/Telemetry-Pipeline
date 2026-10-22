package plog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/wal"

	"broker/internal/metrics"
	"broker/pkg/frame"
)

// ErrOutOfRange is returned by Read for an offset below the first entry still
// on disk or more than one past the high watermark.
var ErrOutOfRange = errors.New("plog: offset out of range")

// Entry is one frame and its offset.
type Entry struct {
	Offset uint64
	Frame  []byte
}

// Partition is one tidwall/wal log. tidwall's Read mutates its segment cache
// under a read lock, so every call into the log goes through mu.
type Partition struct {
	ID  int
	dir string
	m   *metrics.Metrics
	log *slog.Logger

	mu        sync.Mutex
	w         *wal.Log
	last      uint64 // last written index
	recordEnd uint64 // record_base of the next frame
	dirty     bool   // written since the last fsync
	dirMod    time.Time

	hw        atomic.Uint64 // last fsynced index: the only data consumers may see
	first     atomic.Uint64 // first index still on disk
	lastPub   atomic.Uint64 // copy of last for lock-free readers
	recEndPub atomic.Uint64 // copy of recordEnd for lock-free readers
	bytes     atomic.Int64  // on-disk size of the partition directory
	wantTrunc atomic.Uint64 // highest committed offset; entries below it may go
}

func walOptions(segmentSize int) *wal.Options {
	return &wal.Options{
		NoSync:           true, // the flusher fsyncs: group commit
		SegmentSize:      segmentSize,
		SegmentCacheSize: 1,
		AllowEmpty:       true, // consumed entries can all be deleted
		DirPerms:         0o750,
		FilePerms:        0o640,
	}
}

// openPartition opens (or creates) the log in dir and recovers it: a torn
// final entry is cut, then trailing entries that are not valid frames (a
// zero-filled tail after power loss parses as empty entries) are dropped.
// Everything left is treated as durable: hw = last.
func openPartition(id int, dir string, segmentSize int, m *metrics.Metrics, log *slog.Logger) (*Partition, error) {
	opts := walOptions(segmentSize)
	w, err := wal.Open(dir, opts)
	if errors.Is(err, wal.ErrCorrupt) {
		cut, rerr := repairTail(dir)
		if rerr != nil {
			return nil, fmt.Errorf("partition %d: repair torn tail: %w", id, rerr)
		}
		m.RecoveryTruncBs.Add(uint64(cut))
		log.Warn("cut torn tail", "partition", id, "bytes", cut)
		w, err = wal.Open(dir, opts)
	}
	if err != nil {
		return nil, fmt.Errorf("partition %d: open: %w", id, err)
	}
	p := &Partition{ID: id, dir: dir, m: m, log: log, w: w}
	first, _ := w.FirstIndex()
	last, _ := w.LastIndex()

	good := last
	for good >= first && good > 0 {
		if b, err := w.Read(good); err == nil {
			if _, err := frame.Verify(b); err == nil {
				break
			}
		}
		good--
	}
	if good < last {
		if err := w.TruncateBack(good); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("partition %d: drop %d invalid tail entries: %w", id, last-good, err)
		}
		log.Warn("dropped invalid tail entries", "partition", id, "entries", last-good)
	}
	if good >= first && good > 0 {
		b, err := w.Read(good)
		if err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("partition %d: read last entry: %w", id, err)
		}
		h, _ := frame.ParseHeader(b)
		p.recordEnd = h.RecordBase + uint64(h.RecordCount)
	}
	p.last = good
	p.first.Store(first)
	p.hw.Store(good)
	p.lastPub.Store(good)
	p.recEndPub.Store(p.recordEnd)
	p.bytes.Store(dirSize(dir))
	if fi, err := os.Stat(dir); err == nil {
		p.dirMod = fi.ModTime()
	}
	return p, nil
}

// repairTail cuts the last segment file after its last complete entry
// (tidwall's binary format: uvarint length, then data).
func repairTail(dir string) (int64, error) {
	segs, err := segments(dir)
	if err != nil || len(segs) == 0 {
		return 0, err
	}
	path := filepath.Join(dir, segmentName(segs[len(segs)-1]))
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pos := 0
	for pos < len(data) {
		size, n := binary.Uvarint(data[pos:])
		if n <= 0 || uint64(len(data)-pos-n) < size {
			break
		}
		pos += n + int(size)
	}
	cut := int64(len(data) - pos)
	if cut == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := f.Truncate(int64(pos)); err != nil {
		return 0, err
	}
	return cut, f.Sync()
}

// segments lists the start indexes of the plain segment files in dir, sorted.
func segments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, e := range ents {
		if len(e.Name()) != 20 {
			continue
		}
		if i, err := strconv.ParseUint(e.Name(), 10, 64); err == nil {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out, nil
}

func segmentName(index uint64) string { return fmt.Sprintf("%020d", index) }

func dirSize(dir string) int64 {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var n int64
	for _, e := range ents {
		if fi, err := e.Info(); err == nil && !fi.IsDir() {
			n += fi.Size()
		}
	}
	return n
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// append seals f (stamping record_base and the CRC) and writes it as the next
// entry. It returns the entry's offset and its size on disk.
func (p *Partition) append(f []byte) (uint64, int, error) {
	h, err := frame.ParseHeader(f)
	if err != nil {
		return 0, 0, err
	}
	p.mu.Lock()
	frame.Seal(f, p.recordEnd)
	idx := p.last + 1
	if err := p.w.Write(idx, f); err != nil {
		p.mu.Unlock()
		return 0, 0, fmt.Errorf("partition %d: write %d: %w", p.ID, idx, err)
	}
	p.last = idx
	p.recordEnd += uint64(h.RecordCount)
	p.dirty = true
	p.lastPub.Store(idx)
	p.recEndPub.Store(p.recordEnd)
	p.mu.Unlock()

	size := len(f) + uvarintLen(uint64(len(f)))
	p.bytes.Add(int64(size))
	p.m.FramesAppended.Add(1)
	p.m.BytesAppended.Add(uint64(len(f)))
	return idx, size, nil
}

// sync fsyncs the active segment (tidwall syncs a segment when it rolls over)
// and, if a segment file was created since, the directory. It then advances
// the high watermark to everything written before the fsync.
func (p *Partition) sync() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.dirty {
		return false, nil
	}
	start := time.Now()
	if err := p.w.Sync(); err != nil {
		return false, fmt.Errorf("partition %d: fsync: %w", p.ID, err)
	}
	if fi, err := os.Stat(p.dir); err == nil && !fi.ModTime().Equal(p.dirMod) {
		if err := syncDir(p.dir); err != nil {
			return false, fmt.Errorf("partition %d: fsync dir: %w", p.ID, err)
		}
		p.dirMod = fi.ModTime()
	}
	p.m.Fsyncs.Add(1)
	p.m.FsyncLatency.Observe(time.Since(start))
	p.dirty = false
	p.hw.Store(p.last)
	return true, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Read returns entries from offset up to the high watermark, stopping once
// maxBytes is reached (at least one entry is returned when one is available).
// When the entry at offset cannot be read from disk, Read returns no entries
// and skipTo, the first offset of the next readable segment.
func (p *Partition) Read(offset uint64, maxBytes int) (ents []Entry, skipTo uint64, err error) {
	hw := p.hw.Load()
	if offset < p.first.Load() || offset > hw+1 {
		return nil, 0, ErrOutOfRange
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	size := 0
	for i := offset; i <= hw && size < maxBytes; i++ {
		b, err := p.w.Read(i)
		if err != nil {
			if len(ents) > 0 {
				break // serve what we have; the next call hits the error first
			}
			if errors.Is(err, wal.ErrNotFound) {
				return nil, 0, ErrOutOfRange // truncated since the check above
			}
			return nil, p.nextSegment(i), nil
		}
		ents = append(ents, Entry{Offset: i, Frame: b})
		size += len(b)
	}
	return ents, 0, nil
}

// nextSegment is the first segment start after i, or one past the last entry.
func (p *Partition) nextSegment(i uint64) uint64 {
	p.m.CorruptRanges.Add(1)
	segs, _ := segments(p.dir)
	for _, s := range segs {
		if s > i {
			p.log.Error("unreadable log range skipped", "partition", p.ID, "from", i, "to", s)
			return s
		}
	}
	p.log.Error("unreadable log range skipped", "partition", p.ID, "from", i, "to", p.last+1)
	return p.last + 1
}

// truncate deletes entries below the requested commit offset, never past the
// high watermark. It reports the change in bytes on disk.
func (p *Partition) truncate() (int64, error) {
	want := p.wantTrunc.Load()
	p.mu.Lock()
	defer p.mu.Unlock()
	if hw := p.hw.Load(); want > hw+1 {
		want = hw + 1
	}
	if want <= p.first.Load() {
		return 0, nil
	}
	if err := p.w.TruncateFront(want); err != nil {
		return 0, fmt.Errorf("partition %d: truncate front to %d: %w", p.ID, want, err)
	}
	p.first.Store(want)
	before := p.bytes.Load()
	after := dirSize(p.dir)
	p.bytes.Store(after)
	return after - before, nil
}

// HeaderAt returns the header of the entry at offset, which must be in
// [First, Last]. Used for lag (record_base) and producer-table rebuilds.
func (p *Partition) HeaderAt(offset uint64) (frame.Header, error) {
	p.mu.Lock()
	b, err := p.w.Read(offset)
	p.mu.Unlock()
	if err != nil {
		return frame.Header{}, err
	}
	return frame.ParseHeader(b)
}

// RecordBaseAt is the record index of the first record at or after offset:
// the record_base of that entry, or RecordEnd when offset is past the log.
func (p *Partition) RecordBaseAt(offset uint64) uint64 {
	if offset > p.lastPub.Load() || offset < p.first.Load() {
		return p.recEndPub.Load()
	}
	h, err := p.HeaderAt(offset)
	if err != nil {
		return p.recEndPub.Load()
	}
	return h.RecordBase
}

func (p *Partition) HW() uint64        { return p.hw.Load() }
func (p *Partition) First() uint64     { return p.first.Load() }
func (p *Partition) Last() uint64      { return p.lastPub.Load() }
func (p *Partition) RecordEnd() uint64 { return p.recEndPub.Load() }
func (p *Partition) Bytes() int64      { return p.bytes.Load() }

func (p *Partition) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.w.Close()
}
