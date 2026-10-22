// Package frame is the v1 on-disk and on-wire format of one WAL entry: a header
// followed by telemetry records, all little-endian. The broker builds frames;
// consumers decode them. The CRC covers every byte after the CRC field, so a
// frame is self-checking wherever it travels.
//
//	0  magic        u8  0xDB
//	1  version      u8  1
//	2  crc32c       u32 Castagnoli, over bytes [6:]
//	6  record_base  u64 partition-wide index of the first record
//	14 record_count u32
//	18 partition    u16
//	20 streamer_id  u16
//	22 epoch        u32
//	26 first_seq    u64 producer seq of the first record
//	34 last_seq     u64 producer seq of the last record
//	42 records:     ts_ns i64 | uuid [16]byte | name_len u8 | name | value f64
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	Magic      = 0xDB
	Version    = 1
	HeaderSize = 42

	// MaxMetricLen is the longest metric name the 1-byte length prefix can carry.
	MaxMetricLen = 255
	// fixedRecordSize is a record without its metric name.
	fixedRecordSize = 8 + 16 + 1 + 8
)

var (
	ErrShort    = errors.New("frame: truncated")
	ErrMagic    = errors.New("frame: bad magic")
	ErrVersion  = errors.New("frame: unsupported version")
	ErrChecksum = errors.New("frame: checksum mismatch")
	ErrRecords  = errors.New("frame: record section does not match record_count")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Header struct {
	RecordBase  uint64
	RecordCount uint32
	Partition   uint16
	StreamerID  uint16
	Epoch       uint32
	FirstSeq    uint64
	LastSeq     uint64
}

type Record struct {
	TsNanos int64
	UUID    [16]byte
	Metric  string
	Value   float64
}

// RecordSize is the encoded size of r.
func RecordSize(r *Record) int { return fixedRecordSize + len(r.Metric) }

// Builder accumulates records for one frame. The zero value is ready to use.
// Seal must be called on the result of Bytes before the frame is stored.
type Builder struct {
	buf   []byte
	count uint32
	first uint64
	last  uint64
}

// Reset starts a new frame for the given producer and partition.
func (b *Builder) Reset(partition, streamerID uint16, epoch uint32) {
	if cap(b.buf) < HeaderSize {
		b.buf = make([]byte, HeaderSize, 4096)
	}
	b.buf = b.buf[:HeaderSize]
	clear(b.buf)
	b.buf[0], b.buf[1] = Magic, Version
	binary.LittleEndian.PutUint16(b.buf[18:], partition)
	binary.LittleEndian.PutUint16(b.buf[20:], streamerID)
	binary.LittleEndian.PutUint32(b.buf[22:], epoch)
	b.count, b.first, b.last = 0, 0, 0
}

// Add appends one record carrying producer sequence seq. Metric names longer
// than MaxMetricLen are rejected; callers validate before adding.
func (b *Builder) Add(seq uint64, r *Record) error {
	if len(r.Metric) > MaxMetricLen {
		return fmt.Errorf("frame: metric name is %d bytes, max %d", len(r.Metric), MaxMetricLen)
	}
	if b.count == 0 {
		b.first = seq
	}
	b.last = seq
	b.count++
	b.buf = binary.LittleEndian.AppendUint64(b.buf, uint64(r.TsNanos))
	b.buf = append(b.buf, r.UUID[:]...)
	b.buf = append(b.buf, byte(len(r.Metric)))
	b.buf = append(b.buf, r.Metric...)
	b.buf = binary.LittleEndian.AppendUint64(b.buf, math.Float64bits(r.Value))
	return nil
}

func (b *Builder) Count() uint32 { return b.count }
func (b *Builder) Len() int      { return len(b.buf) }
func (b *Builder) LastSeq() uint64 {
	return b.last
}

// Bytes returns a copy of the unsealed frame (record_base and CRC still zero).
func (b *Builder) Bytes() []byte {
	binary.LittleEndian.PutUint32(b.buf[14:], b.count)
	binary.LittleEndian.PutUint64(b.buf[26:], b.first)
	binary.LittleEndian.PutUint64(b.buf[34:], b.last)
	return append([]byte(nil), b.buf...)
}

// Seal stamps record_base into a frame from Builder.Bytes and computes its CRC.
func Seal(f []byte, recordBase uint64) {
	binary.LittleEndian.PutUint64(f[6:], recordBase)
	binary.LittleEndian.PutUint32(f[2:], crc32.Checksum(f[6:], castagnoli))
}

// ParseHeader checks magic, version and length, and decodes the header. It does
// not verify the CRC; use Verify for that.
func ParseHeader(f []byte) (Header, error) {
	if len(f) < HeaderSize {
		return Header{}, ErrShort
	}
	if f[0] != Magic {
		return Header{}, ErrMagic
	}
	if f[1] != Version {
		return Header{}, ErrVersion
	}
	le := binary.LittleEndian
	return Header{
		RecordBase:  le.Uint64(f[6:]),
		RecordCount: le.Uint32(f[14:]),
		Partition:   le.Uint16(f[18:]),
		StreamerID:  le.Uint16(f[20:]),
		Epoch:       le.Uint32(f[22:]),
		FirstSeq:    le.Uint64(f[26:]),
		LastSeq:     le.Uint64(f[34:]),
	}, nil
}

// Verify checks the header and the CRC, and returns the header.
func Verify(f []byte) (Header, error) {
	h, err := ParseHeader(f)
	if err != nil {
		return h, err
	}
	if binary.LittleEndian.Uint32(f[2:]) != crc32.Checksum(f[6:], castagnoli) {
		return h, ErrChecksum
	}
	return h, nil
}

// Decode verifies f and calls fn for every record in order. The Record passed
// to fn is reused between calls; fn must copy what it keeps. Metric is a fresh
// string per record unless intern is non-nil, in which case intern maps the raw
// bytes to a string (letting callers reuse one string per metric name).
func Decode(f []byte, intern func([]byte) string, fn func(*Record) error) (Header, error) {
	h, err := Verify(f)
	if err != nil {
		return h, err
	}
	b := f[HeaderSize:]
	var r Record
	for i := uint32(0); i < h.RecordCount; i++ {
		if len(b) < fixedRecordSize {
			return h, ErrRecords
		}
		n := int(b[24])
		if len(b) < fixedRecordSize+n {
			return h, ErrRecords
		}
		r.TsNanos = int64(binary.LittleEndian.Uint64(b))
		copy(r.UUID[:], b[8:24])
		if intern != nil {
			r.Metric = intern(b[25 : 25+n])
		} else {
			r.Metric = string(b[25 : 25+n])
		}
		r.Value = math.Float64frombits(binary.LittleEndian.Uint64(b[25+n:]))
		b = b[fixedRecordSize+n:]
		if err := fn(&r); err != nil {
			return h, err
		}
	}
	if len(b) != 0 {
		return h, ErrRecords
	}
	return h, nil
}
