package source

import (
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testHeader = "metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value\n"

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func writeCSV(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "telemetry.csv")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testOptions(path, mode string, budget int64) Options {
	return Options{Path: path, Mode: mode, MaxMemoryBytes: budget}
}

func TestResolveColumnsAndParseRecord(t *testing.T) {
	header := []string{"\ufeffUUID ", "VALUE", "metric_name", "gpu_id", "device", "modelname", "hostname", "container", "pod", "namespace"}
	ix, err := resolveColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	row, value, reason := parseRecord([]string{"id-1", "1.25", "power", "0", "gpu", "A100", "host", "c", "p", "ns"}, ix)
	if reason != "" || value != 1.25 || row.UUID != "id-1" || row.Metric != "power" || row.ModelName != "A100" {
		t.Fatalf("parseRecord = %+v, %v, %q", row, value, reason)
	}

	if _, err := resolveColumns([]string{"metric_name"}); err == nil || !strings.Contains(err.Error(), "gpu_id") {
		t.Fatalf("missing column error = %v", err)
	}
	standard, err := resolveColumns(strings.Split(strings.TrimSpace(testHeader), ","))
	if err != nil {
		t.Fatal(err)
	}
	malformed := []struct {
		name string
		rec  []string
		want string
	}{
		{name: "short row", rec: []string{"power"}, want: "too few fields"},
		{name: "empty uuid", rec: []string{"power", "0", "gpu", "", "A100", "host", "c", "p", "ns", "1"}, want: "empty uuid"},
		{name: "empty metric", rec: []string{"", "0", "gpu", "id", "A100", "host", "c", "p", "ns", "1"}, want: "empty metric_name"},
		{name: "bad number", rec: []string{"power", "0", "gpu", "id", "A100", "host", "c", "p", "ns", "bad"}, want: "unparseable value"},
		{name: "nan", rec: []string{"power", "0", "gpu", "id", "A100", "host", "c", "p", "ns", "NaN"}, want: "non-finite value"},
		{name: "infinity", rec: []string{"power", "0", "gpu", "id", "A100", "host", "c", "p", "ns", "Inf"}, want: "non-finite value"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			_, _, got := parseRecord(tc.rec, standard)
			if got != tc.want {
				t.Fatalf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMalformedAccountingSuppressionAndReset(t *testing.T) {
	rep := NewMalformed(testLogger(), time.Hour)
	rep.Report("line 2", "bad value")
	rep.Report("line 3", "empty uuid")
	if got := rep.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}
	rep.Reset()
	if got := rep.Count(); got != 0 {
		t.Fatalf("Count() after Reset = %d", got)
	}
}

func TestMemorySourceInternsRowsLoopsAndCountsMalformed(t *testing.T) {
	path := writeCSV(t, testHeader+
		"power,0,gpu,id-1,A100,host,c,p,ns,1.5\n"+
		"power,0,gpu,id-1,A100,host,c,p,ns,2.5\n"+
		"power,0,gpu,,A100,host,c,p,ns,3\n"+
		"power,0,gpu,id-1,A100,host,c,p,ns,NaN\n")
	rep := NewMalformed(testLogger(), time.Hour)
	src, err := Open(testOptions(path, "memory", 1024), rep, rand.New(rand.NewSource(1)), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	mem := src.(*memSource)
	if len(mem.rows) != 2 || len(mem.series) != 1 || rep.Count() != 2 {
		t.Fatalf("memory source rows=%d series=%d malformed=%d", len(mem.rows), len(mem.series), rep.Count())
	}
	first, err := src.Next()
	if err != nil {
		t.Fatal(err)
	}
	second, err := src.Next()
	if err != nil {
		t.Fatal(err)
	}
	third, err := src.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first.S != second.S || third.S != first.S || third.Value != first.Value {
		t.Fatalf("rows did not reuse their series and wrap: %+v %+v %+v", first, second, third)
	}
	if !strings.Contains(src.Describe(), "memory mode: 2 rows, 1 series") {
		t.Fatalf("unexpected description %q", src.Describe())
	}
}

func TestOpenErrorsAndNoValidMemoryRows(t *testing.T) {
	rep := NewMalformed(testLogger(), 0)
	rng := rand.New(rand.NewSource(1))
	if _, err := Open(testOptions("missing.csv", "memory", 1024), rep, rng, testLogger()); err == nil {
		t.Fatal("Open accepted a missing file")
	}
	if _, err := Open(testOptions("", "disk", 1024), rep, rng, testLogger()); err == nil {
		t.Fatal("Open accepted an unknown mode")
	}
	path := writeCSV(t, "not_a_header\nrow\n")
	if _, err := Open(testOptions(path, "memory", 1024), rep, rng, testLogger()); err == nil {
		t.Fatal("Open accepted a missing CSV column")
	}
	path = writeCSV(t, testHeader+"power,0,gpu,id,A100,host,c,p,ns,nope\n")
	if _, err := Open(testOptions(path, "memory", 1024), rep, rng, testLogger()); err == nil || !strings.Contains(err.Error(), "no valid rows") {
		t.Fatalf("empty memory source error = %v", err)
	}
	if rep.Count() != 1 {
		t.Fatalf("malformed count = %d, want 1", rep.Count())
	}
}

func TestMemoryBudgetAndAutoStreamingFallback(t *testing.T) {
	path := writeCSV(t, testHeader+
		"power,0,gpu,,A100,host,c,p,ns,invalid\n"+
		"power,0,gpu,id-1,A100,host,c,p,ns,1\n"+
		"power,0,gpu,id-2,A100,host,c,p,ns,2\n")
	rep := NewMalformed(testLogger(), 0)
	if _, err := Open(testOptions(path, "memory", rowBytes), rep, rand.New(rand.NewSource(2)), testLogger()); err != ErrOverBudget {
		t.Fatalf("memory budget error = %v, want ErrOverBudget", err)
	}

	rep.Reset()
	src, err := Open(testOptions(path, "auto", rowBytes), rep, rand.New(rand.NewSource(2)), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if !strings.Contains(src.Describe(), "stream mode") {
		t.Fatalf("auto mode did not fall back to streaming: %q", src.Describe())
	}
	if rep.Count() != 0 {
		t.Fatalf("auto fallback did not reset first-pass malformed count: %d", rep.Count())
	}
	for i := 0; i < 3; i++ {
		if _, err := src.Next(); err != nil {
			t.Fatalf("fallback source Next: %v", err)
		}
	}
	if rep.Count() == 0 {
		t.Fatal("streaming pass did not report its malformed row")
	}
}

func TestStreamSourceReadsAndWrapsValidRows(t *testing.T) {
	path := writeCSV(t, testHeader+
		"power,0,gpu,id-1,A100,host,c,p,ns,1\n"+
		"power,0,gpu,id-2,A100,host,c,p,ns,2\n"+
		"power,0,gpu,id-3,A100,host,c,p,ns,3\n")
	rep := NewMalformed(testLogger(), 0)
	src, err := Open(testOptions(path, "stream", 0), rep, rand.New(rand.NewSource(1)), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	seen := map[float64]bool{}
	for i := 0; i < 4; i++ {
		row, err := src.Next()
		if err != nil {
			t.Fatal(err)
		}
		seen[row.Value] = true
	}
	for _, value := range []float64{1, 2, 3} {
		if !seen[value] {
			t.Errorf("stream did not produce value %v; seen %v", value, seen)
		}
	}
	if !strings.Contains(src.Describe(), "stream mode: re-reading") {
		t.Fatalf("unexpected description %q", src.Describe())
	}
}

func TestStreamSourceRejectsCSVWithoutValidRows(t *testing.T) {
	path := writeCSV(t, testHeader+"power,0,gpu,id,A100,host,c,p,ns,bad\n")
	rep := NewMalformed(testLogger(), 0)
	src, err := Open(testOptions(path, "stream", 0), rep, rand.New(rand.NewSource(3)), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Next(); err == nil || !strings.Contains(err.Error(), "no valid rows") {
		t.Fatalf("Next() error = %v, want no-valid-rows", err)
	}
	if rep.Count() == 0 {
		t.Fatal("stream source did not report malformed row")
	}
}
