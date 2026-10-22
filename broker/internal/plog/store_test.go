package plog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"broker/internal/metrics"
	"broker/pkg/frame"
)

func testOpts(dir string) Options {
	return Options{Dir: dir, Partitions: 2, SegmentSize: 512, FlushInterval: time.Hour, TruncateInterval: time.Hour, QuotaBytes: 1 << 30}
}

func open(t *testing.T, o Options) *Store {
	t.Helper()
	s, err := Open(o, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// mkFrame builds an unsealed frame with n records for partition p.
func mkFrame(p uint16, firstSeq uint64, n int) []byte {
	var b frame.Builder
	b.Reset(p, 1, 9)
	for i := 0; i < n; i++ {
		r := frame.Record{TsNanos: int64(i), Metric: "m", Value: 1}
		_ = b.Add(firstSeq+uint64(i), &r)
	}
	return b.Bytes()
}

func appendN(t *testing.T, s *Store, p, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := s.Append(context.Background(), p, mkFrame(uint16(p), uint64(i*3), 3)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNothingVisibleBeforeFsync(t *testing.T) {
	s := open(t, testOpts(t.TempDir()))
	defer s.Close()
	p := s.Partition(0)
	ch := s.HWChanged()
	appendN(t, s, 0, 3)
	if p.HW() != 0 || p.Last() != 3 {
		t.Fatalf("before flush: hw=%d last=%d", p.HW(), p.Last())
	}
	ents, _, err := p.Read(1, 1<<20)
	if err != nil || len(ents) != 0 {
		t.Fatalf("read before flush: %d entries, err %v", len(ents), err)
	}
	s.Flush()
	select {
	case <-ch:
	default:
		t.Fatal("HWChanged not signalled by the flush")
	}
	ents, _, err = p.Read(1, 1<<20)
	if err != nil || len(ents) != 3 || p.HW() != 3 {
		t.Fatalf("after flush: %d entries, hw=%d, err %v", len(ents), p.HW(), err)
	}
	for i, e := range ents {
		h, err := frame.Verify(e.Frame)
		if err != nil || e.Offset != uint64(i+1) || h.RecordBase != uint64(i*3) {
			t.Fatalf("entry %d: offset=%d header=%+v err=%v", i, e.Offset, h, err)
		}
	}
	if _, _, err := p.Read(5, 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("read past hw+1: %v", err)
	}
	if ents, _, err := p.Read(4, 1); err != nil || len(ents) != 0 {
		t.Fatalf("read at hw+1 should be empty, got %d err %v", len(ents), err)
	}
}

func TestReadRespectsMaxBytes(t *testing.T) {
	s := open(t, testOpts(t.TempDir()))
	defer s.Close()
	appendN(t, s, 1, 10)
	s.Flush()
	ents, _, _ := s.Partition(1).Read(1, 1)
	if len(ents) != 1 {
		t.Fatalf("maxBytes=1 returned %d entries, want 1", len(ents))
	}
}

func TestReopenContinuesOffsetsAndRecordBase(t *testing.T) {
	dir := t.TempDir()
	s := open(t, testOpts(dir))
	appendN(t, s, 0, 5)
	_ = s.Close()
	s = open(t, testOpts(dir))
	defer s.Close()
	p := s.Partition(0)
	if p.HW() != 5 || p.RecordEnd() != 15 {
		t.Fatalf("after reopen: hw=%d recordEnd=%d", p.HW(), p.RecordEnd())
	}
	idx, err := s.Append(context.Background(), 0, mkFrame(0, 99, 2))
	if err != nil || idx != 6 {
		t.Fatalf("append after reopen: idx=%d err=%v", idx, err)
	}
	if p.RecordBaseAt(6) != 15 || p.RecordBaseAt(7) != 17 {
		t.Fatalf("record base at 6 = %d, past end = %d", p.RecordBaseAt(6), p.RecordBaseAt(7))
	}
}

func lastSegPath(t *testing.T, dir string) string {
	segs, err := segments(dir)
	if err != nil || len(segs) == 0 {
		t.Fatalf("segments: %v %v", segs, err)
	}
	return filepath.Join(dir, segmentName(segs[len(segs)-1]))
}

// Cutting the last segment anywhere inside its final entry loses exactly that
// entry; everything before it survives and the log accepts new writes.
func TestRecoveryCutAtEveryByteOfLastEntry(t *testing.T) {
	base := t.TempDir()
	o := testOpts(base)
	o.SegmentSize = 1 << 20
	s := open(t, o)
	appendN(t, s, 0, 4)
	_ = s.Close()
	pdir := filepath.Join(base, "wal", "0")
	seg := lastSegPath(t, pdir)
	full, _ := os.ReadFile(seg)
	entry := len(mkFrame(0, 0, 3)) + 1 // frame + 1-byte uvarint length
	for cut := 1; cut <= entry; cut++ {
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, "wal", "0"), 0o750)
		_ = os.MkdirAll(filepath.Join(dir, "wal", "1"), 0o750)
		b, _ := os.ReadFile(filepath.Join(base, "topic.json"))
		_ = os.WriteFile(filepath.Join(dir, "topic.json"), b, 0o640)
		_ = os.WriteFile(filepath.Join(dir, "wal", "0", filepath.Base(seg)), full[:len(full)-cut], 0o640)
		s := open(t, testOpts(dir))
		p := s.Partition(0)
		if p.Last() != 3 || p.HW() != 3 || p.RecordEnd() != 9 {
			t.Fatalf("cut %d bytes: last=%d hw=%d recordEnd=%d", cut, p.Last(), p.HW(), p.RecordEnd())
		}
		if idx, err := s.Append(context.Background(), 0, mkFrame(0, 9, 1)); err != nil || idx != 4 {
			t.Fatalf("cut %d bytes: append idx=%d err=%v", cut, idx, err)
		}
		_ = s.Close()
	}
}

func TestRecoveryDropsZeroFilledTail(t *testing.T) {
	dir := t.TempDir()
	s := open(t, testOpts(dir))
	appendN(t, s, 0, 2)
	_ = s.Close()
	seg := lastSegPath(t, filepath.Join(dir, "wal", "0"))
	f, _ := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write(make([]byte, 100))
	_ = f.Close()
	s = open(t, testOpts(dir))
	defer s.Close()
	if p := s.Partition(0); p.Last() != 2 || p.HW() != 2 {
		t.Fatalf("zero tail: last=%d hw=%d", p.Last(), p.HW())
	}
}

func TestTruncateDeletesConsumedEntries(t *testing.T) {
	dir := t.TempDir()
	s := open(t, testOpts(dir))
	appendN(t, s, 0, 40) // many 512-byte segments
	s.Flush()
	p := s.Partition(0)
	before := s.DiskUsed()
	s.RequestTruncate(0, 30)
	s.RequestTruncate(0, 20) // an older commit never moves it back
	s.TruncateNow()
	if p.First() != 30 || s.DiskUsed() >= before {
		t.Fatalf("first=%d used before=%d after=%d", p.First(), before, s.DiskUsed())
	}
	if _, _, err := p.Read(29, 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("read below first: %v", err)
	}
	// commit past the high watermark is clamped to it
	appendN(t, s, 0, 1) // index 41, not fsynced
	s.RequestTruncate(0, 1000)
	s.TruncateNow()
	if p.First() != 41 {
		t.Fatalf("truncate went past hw: first=%d hw=%d", p.First(), p.HW())
	}
	s.Flush()
	s.RequestTruncate(0, 42)
	s.TruncateNow()
	if p.First() != 42 || p.Last() != 41 {
		t.Fatalf("truncate all: first=%d last=%d", p.First(), p.Last())
	}
	_ = s.Close()
	s = open(t, testOpts(dir))
	defer s.Close()
	if idx, err := s.Append(context.Background(), 0, mkFrame(0, 0, 1)); err != nil || idx != 42 {
		t.Fatalf("append after emptying and reopening: idx=%d err=%v", idx, err)
	}
}

func TestQuotaBlocksAndResumes(t *testing.T) {
	o := testOpts(t.TempDir())
	o.QuotaBytes = 2000
	s := open(t, o)
	defer s.Close()
	for !s.QuotaBlocked() {
		appendN(t, s, 0, 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Append(ctx, 1, mkFrame(1, 0, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("append at quota: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Append(context.Background(), 1, mkFrame(1, 0, 1))
		done <- err
	}()
	s.Flush()
	s.RequestTruncate(0, s.Partition(0).HW()+1)
	s.TruncateNow()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("append still blocked after the quota cleared")
	}
}

func TestTopicMismatchRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	_ = open(t, testOpts(dir)).Close()
	o := testOpts(dir)
	o.Partitions = 3
	if _, err := Open(o, metrics.New(), slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("opened with a different partition count")
	}
}

func TestCorruptSealedSegmentIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s := open(t, testOpts(dir))
	appendN(t, s, 0, 30)
	_ = s.Close()
	pdir := filepath.Join(dir, "wal", "0")
	segs, _ := segments(pdir)
	if len(segs) < 3 {
		t.Fatalf("want several segments, got %v", segs)
	}
	// break the length prefix of the first entry in the second segment
	path := filepath.Join(pdir, segmentName(segs[1]))
	b, _ := os.ReadFile(path)
	b[0] = 0xFF
	b[1] = 0xFF
	_ = os.WriteFile(path, b, 0o640)
	s = open(t, testOpts(dir))
	defer s.Close()
	ents, skip, err := s.Partition(0).Read(segs[1], 1<<20)
	if err != nil || len(ents) != 0 || skip != segs[2] {
		t.Fatalf("ents=%d skip=%d err=%v, want skip to %d", len(ents), skip, err, segs[2])
	}
	if ents, _, err := s.Partition(0).Read(skip, 1<<20); err != nil || len(ents) == 0 {
		t.Fatalf("read after skip: %d entries, err %v", len(ents), err)
	}
}
