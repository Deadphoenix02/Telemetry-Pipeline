package plog

// These tests pin the tidwall/wal behaviour the partition log relies on, so a
// library upgrade that changes it fails here first.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tidwall/wal"
)

func spikeOpts() *wal.Options {
	return &wal.Options{NoSync: true, SegmentSize: 256, SegmentCacheSize: 1, AllowEmpty: true}
}

func mustOpen(t *testing.T, dir string) *wal.Log {
	t.Helper()
	l, err := wal.Open(dir, spikeOpts())
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func writeN(t *testing.T, l *wal.Log, from, n uint64, size int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		if err := l.Write(i, make([]byte, size)); err != nil {
			t.Fatal(err)
		}
	}
}

func lastSegment(t *testing.T, dir string) string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, ents[len(ents)-1].Name())
}

// C1: a torn entry at the tail makes Open fail, so the log needs a repair step.
func TestSpikeTornTailFailsOpen(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	writeN(t, l, 1, 3, 40)
	_ = l.Close()
	f, _ := os.OpenFile(lastSegment(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write([]byte{50, 1, 2}) // length prefix 50 but only 2 data bytes
	_ = f.Close()
	if _, err := wal.Open(dir, spikeOpts()); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Open with torn tail: err = %v, want ErrCorrupt", err)
	}
}

// Zero bytes at the tail (an extended but unwritten file after power loss)
// parse as valid empty entries, so repair must also check frame contents.
func TestSpikeZeroTailParsesAsEntries(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	writeN(t, l, 1, 2, 40)
	_ = l.Close()
	f, _ := os.OpenFile(lastSegment(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write(make([]byte, 3))
	_ = f.Close()
	l = mustOpen(t, dir)
	defer l.Close()
	last, _ := l.LastIndex()
	if last != 5 {
		t.Fatalf("last index = %d, want 5 (2 real + 3 phantom empty entries)", last)
	}
	if b, _ := l.Read(5); len(b) != 0 {
		t.Fatalf("phantom entry has %d bytes", len(b))
	}
}

// C4 with AllowEmpty: every consumed entry can be deleted, and the next index
// survives a restart.
func TestSpikeTruncateAllAndReopen(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	writeN(t, l, 1, 20, 40) // several 256-byte segments
	if err := l.TruncateFront(21); err != nil {
		t.Fatal(err)
	}
	first, _ := l.FirstIndex()
	last, _ := l.LastIndex()
	if first != 21 || last != 20 {
		t.Fatalf("after truncating all: first=%d last=%d", first, last)
	}
	_ = l.Close()
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("%d files left after truncating everything, want 1 empty segment", len(ents))
	}
	l = mustOpen(t, dir)
	defer l.Close()
	if err := l.Write(21, []byte("x")); err != nil {
		t.Fatalf("write after reopen: %v", err)
	}
}

// C3: TruncateFront deletes whole older segments and keeps the rest readable.
func TestSpikeTruncateFrontDeletesSegments(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	defer l.Close()
	writeN(t, l, 1, 30, 40)
	before, _ := os.ReadDir(dir)
	if err := l.TruncateFront(25); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadDir(dir)
	if len(after) >= len(before) {
		t.Fatalf("segments before=%d after=%d", len(before), len(after))
	}
	if _, err := l.Read(24); !errors.Is(err, wal.ErrNotFound) {
		t.Fatalf("read below first: %v", err)
	}
	if _, err := l.Read(25); err != nil {
		t.Fatalf("read first: %v", err)
	}
}

// C6: without Close (a killed process), written entries survive in the files,
// including entries in segments that rolled over.
func TestSpikeUnclosedLogKeepsWrites(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	writeN(t, l, 1, 30, 40)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	// no Close: reopen the directory as a restarted process would
	l2 := mustOpen(t, dir)
	defer l2.Close()
	last, _ := l2.LastIndex()
	if last != 30 {
		t.Fatalf("last index after reopen = %d, want 30", last)
	}
}
