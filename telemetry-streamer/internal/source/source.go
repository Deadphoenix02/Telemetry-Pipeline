// Package source loads the CSV and serves an endless, looping sequence of
// rows. Per row it keeps the label columns plus the value; the CSV's own
// timestamp and labels_raw are discarded.
package source

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Series is the label set of one GPU metric (constant across its rows).
type Series struct {
	Metric, GPUID, Device, UUID, ModelName, Hostname, Container, Pod, Namespace string
}

// Row is one CSV row. S is shared and must not be modified.
type Row struct {
	S     *Series
	Value float64
}

// Source is an endless row sequence. Not safe for concurrent use: only the
// pacer loop calls Next.
type Source interface {
	Next() (Row, error)
	Close() error
	Describe() string
}

type Options struct {
	Path           string
	Mode           string // auto | memory | stream
	MaxMemoryBytes int64
}

// ErrOverBudget means the parsed rows would not fit the memory budget.
var ErrOverBudget = errors.New("parsed rows exceed memory budget")

// Open builds a Source. auto = memory unless the file is over budget, then stream.
func Open(opt Options, rep *Malformed, rng *rand.Rand, log *slog.Logger) (Source, error) {
	switch opt.Mode {
	case "stream":
		s, err := newStream(opt, rep, rng)
		if err != nil {
			return nil, err
		}
		return s, nil
	case "memory":
		s, err := loadMemory(opt, rep, rng)
		if err != nil {
			return nil, err
		}
		return s, nil
	case "auto":
		s, err := loadMemory(opt, rep, rng)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrOverBudget) {
			return nil, err
		}
		log.Warn("CSV exceeds memory budget; falling back to streaming re-read mode",
			"budget_bytes", opt.MaxMemoryBytes)
		rep.Reset()
		ss, err := newStream(opt, rep, rng)
		if err != nil {
			return nil, err
		}
		return ss, nil
	default:
		return nil, fmt.Errorf("unknown mode %q", opt.Mode)
	}
}

// ---- shared parsing --------------------------------------------------------

const (
	cMetric = iota
	cGPUID
	cDevice
	cUUID
	cModel
	cHost
	cContainer
	cPod
	cNamespace
	cValue
	numCols
)

// CSV header names (matched case-insensitively).
var colNames = [numCols]string{"metric_name", "gpu_id", "device", "uuid", "modelName", "Hostname", "container", "pod", "namespace", "value"}

type colIdx struct {
	at   [numCols]int
	need int
}

func resolveColumns(header []string) (colIdx, error) {
	var ix colIdx
	for c, name := range colNames {
		ix.at[c] = -1
		for i, h := range header {
			h = strings.TrimPrefix(strings.TrimSpace(h), "\ufeff")
			if strings.EqualFold(h, name) {
				ix.at[c] = i
				break
			}
		}
		if ix.at[c] < 0 {
			return ix, fmt.Errorf("column %q not found in CSV header %v", name, header)
		}
		if ix.at[c]+1 > ix.need {
			ix.need = ix.at[c] + 1
		}
	}
	return ix, nil
}

func newCSVReader(r io.Reader) *csv.Reader {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // we validate ourselves and count/skip bad rows
	cr.LazyQuotes = true
	cr.ReuseRecord = true
	return cr
}

// parseRecord returns a non-empty reason when the row is malformed. uuid,
// metric_name and value are mandatory; other labels may be empty.
func parseRecord(rec []string, ix colIdx) (s Series, v float64, reason string) {
	if len(rec) < ix.need {
		return s, 0, "too few fields"
	}
	f := func(c int) string { return strings.TrimSpace(rec[ix.at[c]]) }
	s = Series{
		Metric: f(cMetric), GPUID: f(cGPUID), Device: f(cDevice), UUID: f(cUUID),
		ModelName: f(cModel), Hostname: f(cHost), Container: f(cContainer),
		Pod: f(cPod), Namespace: f(cNamespace),
	}
	if s.UUID == "" {
		return s, 0, "empty uuid"
	}
	if s.Metric == "" {
		return s, 0, "empty metric_name"
	}
	fv, err := strconv.ParseFloat(f(cValue), 64)
	if err != nil {
		return s, 0, "unparseable value"
	}
	if math.IsNaN(fv) || math.IsInf(fv, 0) {
		return s, 0, "non-finite value"
	}
	return s, fv, ""
}

func (s Series) appendKey(b []byte) []byte {
	for _, f := range [...]string{s.Metric, s.GPUID, s.Device, s.UUID, s.ModelName, s.Hostname, s.Container, s.Pod, s.Namespace} {
		b = append(b, f...)
		b = append(b, 0)
	}
	return b
}

// clone copies the strings so they don't pin the whole CSV line they came from.
func (s Series) clone() Series {
	return Series{
		Metric: strings.Clone(s.Metric), GPUID: strings.Clone(s.GPUID), Device: strings.Clone(s.Device),
		UUID: strings.Clone(s.UUID), ModelName: strings.Clone(s.ModelName), Hostname: strings.Clone(s.Hostname),
		Container: strings.Clone(s.Container), Pod: strings.Clone(s.Pod), Namespace: strings.Clone(s.Namespace),
	}
}

// ---- malformed-row accounting ---------------------------------------------

// Malformed counts bad rows and logs them at most once per interval.
type Malformed struct {
	count    atomic.Uint64
	log      *slog.Logger
	interval time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed uint64
}

func NewMalformed(log *slog.Logger, interval time.Duration) *Malformed {
	return &Malformed{log: log, interval: interval}
}

func (m *Malformed) Report(where, reason string) {
	m.count.Add(1)
	m.mu.Lock()
	now := time.Now()
	if now.Sub(m.last) < m.interval {
		m.suppressed++
		m.mu.Unlock()
		return
	}
	sup := m.suppressed
	m.suppressed, m.last = 0, now
	m.mu.Unlock()
	m.log.Warn("skipping malformed row", "at", where, "reason", reason,
		"suppressed_since_last_log", sup, "malformed_total", m.count.Load())
}

func (m *Malformed) Count() uint64 { return m.count.Load() }
func (m *Malformed) Reset()        { m.count.Store(0) }

// ---- in-memory mode --------------------------------------------------------

// packed is 16 bytes: the label set is interned into a series table.
type packed struct {
	s uint32
	v float64
}

const rowBytes = 16

type memSource struct {
	series []Series
	rows   []packed
	pos    int
	start  int
}

func loadMemory(opt Options, rep *Malformed, rng *rand.Rand) (*memSource, error) {
	f, err := os.Open(opt.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := newCSVReader(bufio.NewReaderSize(f, 1<<16))
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	ix, err := resolveColumns(header)
	if err != nil {
		return nil, err
	}
	maxRows := int(opt.MaxMemoryBytes / rowBytes)
	ms := &memSource{}
	idx := map[string]uint32{}
	var key []byte
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			var pe *csv.ParseError
			if errors.As(err, &pe) {
				rep.Report(fmt.Sprintf("line %d", pe.StartLine), pe.Err.Error())
				continue
			}
			return nil, err
		}
		ln, _ := r.FieldPos(0)
		s, v, reason := parseRecord(rec, ix)
		if reason != "" {
			rep.Report(fmt.Sprintf("line %d", ln), reason)
			continue
		}
		if len(ms.rows) >= maxRows {
			return nil, ErrOverBudget
		}
		key = s.appendKey(key[:0])
		id, ok := idx[string(key)]
		if !ok {
			id = uint32(len(ms.series))
			idx[string(key)] = id
			ms.series = append(ms.series, s.clone())
		}
		ms.rows = append(ms.rows, packed{s: id, v: v})
	}
	if len(ms.rows) == 0 {
		return nil, errors.New("CSV contains no valid rows")
	}
	exact := make([]packed, len(ms.rows)) // drop append slack
	copy(exact, ms.rows)
	ms.rows = exact
	ms.start = rng.Intn(len(ms.rows))
	ms.pos = ms.start
	return ms, nil
}

func (s *memSource) Next() (Row, error) {
	p := s.rows[s.pos]
	s.pos++
	if s.pos == len(s.rows) {
		s.pos = 0
	}
	return Row{S: &s.series[p.s], Value: p.v}, nil
}

func (s *memSource) Close() error { return nil }

func (s *memSource) Describe() string {
	return fmt.Sprintf("memory mode: %d rows, %d series, start offset row %d", len(s.rows), len(s.series), s.start)
}

// ---- streaming re-read mode --------------------------------------------------

type streamSource struct {
	opt       Options
	rep       *Malformed
	ix        colIdx
	f         *os.File
	r         *csv.Reader
	dataStart int64
	base      int64 // file offset of r's first byte
	startOff  int64
	fullPass  bool // current pass began at the first data row
	valid     int  // valid rows seen in the current pass
}

func newStream(opt Options, rep *Malformed, rng *rand.Rand) (*streamSource, error) {
	f, err := os.Open(opt.Path)
	if err != nil {
		return nil, err
	}
	hr := newCSVReader(bufio.NewReaderSize(f, 1<<16))
	header, err := hr.Read()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	ix, err := resolveColumns(header)
	if err != nil {
		f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &streamSource{opt: opt, rep: rep, ix: ix, f: f, dataStart: hr.InputOffset()}
	off := s.dataStart
	if st.Size() > s.dataStart {
		off += rng.Int63n(st.Size() - s.dataStart)
	}
	s.startOff = off
	if err := s.reset(off); err != nil {
		f.Close()
		return nil, err
	}
	s.fullPass = off == s.dataStart
	return s, nil
}

// reset seeks to off and resyncs to the next line boundary (unless at data start).
func (s *streamSource) reset(off int64) error {
	if _, err := s.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	br := bufio.NewReaderSize(s.f, 1<<16)
	s.base = off
	if off > s.dataStart {
		skipped, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}
		s.base += int64(len(skipped))
	}
	s.r = newCSVReader(br)
	return nil
}

func (s *streamSource) Next() (Row, error) {
	for {
		rec, err := s.r.Read()
		if err == io.EOF {
			if s.fullPass && s.valid == 0 {
				return Row{}, errors.New("CSV contains no valid rows")
			}
			if err := s.reset(s.dataStart); err != nil {
				return Row{}, err
			}
			s.fullPass, s.valid = true, 0 // wrap: this is the loop
			continue
		}
		if err != nil {
			var pe *csv.ParseError
			if errors.As(err, &pe) {
				s.rep.Report(fmt.Sprintf("byte ~%d", s.base+s.r.InputOffset()), pe.Err.Error())
				continue
			}
			return Row{}, err
		}
		series, v, reason := parseRecord(rec, s.ix)
		if reason != "" {
			s.rep.Report(fmt.Sprintf("byte ~%d", s.base+s.r.InputOffset()), reason)
			continue
		}
		s.valid++
		return Row{S: &series, Value: v}, nil
	}
}

func (s *streamSource) Close() error { return s.f.Close() }

func (s *streamSource) Describe() string {
	return fmt.Sprintf("stream mode: re-reading %s, start byte offset %d", s.opt.Path, s.startOff)
}
