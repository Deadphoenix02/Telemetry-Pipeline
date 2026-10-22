package frame

import (
	"errors"
	"testing"
)

func sample(n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{TsNanos: int64(1_700_000_000_000_000_000 + i), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: float64(i) / 3}
		recs[i].UUID[0], recs[i].UUID[15] = byte(i), 0xAB
	}
	return recs
}

func build(t testing.TB, recs []Record, base uint64) []byte {
	var b Builder
	b.Reset(3, 7, 0xCAFE)
	for i := range recs {
		if err := b.Add(uint64(100+i), &recs[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := b.Bytes()
	Seal(f, base)
	return f
}

func TestRoundTrip(t *testing.T) {
	recs := sample(5)
	f := build(t, recs, 42)
	var got []Record
	h, err := Decode(f, nil, func(r *Record) error { got = append(got, *r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := Header{RecordBase: 42, RecordCount: 5, Partition: 3, StreamerID: 7, Epoch: 0xCAFE, FirstSeq: 100, LastSeq: 104}
	if h != want {
		t.Fatalf("header = %+v, want %+v", h, want)
	}
	if len(got) != len(recs) {
		t.Fatalf("decoded %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i] != recs[i] {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], recs[i])
		}
	}
	if len(f) != HeaderSize+5*RecordSize(&recs[0]) {
		t.Fatalf("frame is %d bytes", len(f))
	}
}

func TestInternReusesStrings(t *testing.T) {
	f := build(t, sample(3), 0)
	calls := 0
	intern := func(b []byte) string { calls++; return "X" }
	_, err := Decode(f, intern, func(r *Record) error {
		if r.Metric != "X" {
			t.Fatalf("metric %q", r.Metric)
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestResetClearsState(t *testing.T) {
	var b Builder
	b.Reset(1, 1, 1)
	r := sample(1)[0]
	_ = b.Add(9, &r)
	b.Reset(2, 2, 2)
	if b.Count() != 0 || b.Len() != HeaderSize {
		t.Fatalf("count=%d len=%d after reset", b.Count(), b.Len())
	}
	_ = b.Add(10, &r)
	f := b.Bytes()
	Seal(f, 0)
	h, err := Verify(f)
	if err != nil || h.FirstSeq != 10 || h.LastSeq != 10 || h.Partition != 2 {
		t.Fatalf("h=%+v err=%v", h, err)
	}
}

func TestDetectsCorruption(t *testing.T) {
	f := build(t, sample(4), 1)
	for i := range f {
		c := append([]byte(nil), f...)
		c[i] ^= 0x01
		if _, err := Verify(c); err == nil {
			t.Fatalf("flipping byte %d went undetected", i)
		}
	}
	if _, err := Verify(f[:HeaderSize-1]); !errors.Is(err, ErrShort) {
		t.Fatalf("short frame: %v", err)
	}
}

func TestRejectsLongMetric(t *testing.T) {
	var b Builder
	b.Reset(0, 0, 0)
	r := Record{Metric: string(make([]byte, MaxMetricLen+1))}
	if err := b.Add(0, &r); err == nil {
		t.Fatal("expected error for a 256-byte metric name")
	}
}

func TestRecordCountMismatch(t *testing.T) {
	f := build(t, sample(2), 0)
	f = f[:len(f)-1] // drop a byte, then re-seal so only the structure is wrong
	Seal(f, 0)
	if _, err := Decode(f, nil, func(*Record) error { return nil }); !errors.Is(err, ErrRecords) {
		t.Fatalf("err = %v, want ErrRecords", err)
	}
	g := build(t, sample(2), 0)
	g = append(g, 0)
	Seal(g, 0)
	if _, err := Decode(g, nil, func(*Record) error { return nil }); !errors.Is(err, ErrRecords) {
		t.Fatalf("trailing byte: err = %v, want ErrRecords", err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(build(f, sample(3), 7))
	f.Add([]byte{Magic, Version})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data, nil, func(*Record) error { return nil })
		if len(data) >= HeaderSize {
			Seal(data, 0) // a valid CRC must still not let malformed records through
			_, _ = Decode(data, nil, func(*Record) error { return nil })
		}
	})
}
