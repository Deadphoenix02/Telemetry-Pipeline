package wire

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestDiscardStreamAcknowledgesAndCloses(t *testing.T) {
	d := &Discard{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	st, err := d.OpenStream(context.Background(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(Record{Seq: 12}); err != nil {
		t.Fatal(err)
	}
	ack, err := st.Recv()
	if err != nil || ack.Seq != 12 {
		t.Fatalf("Recv() = %+v, %v; want ack 12", ack, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != io.EOF {
		t.Fatalf("Recv after close error = %v, want EOF", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	blocked, err := d.OpenStream(context.Background(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(blocked.(*discardStream).acks); i++ {
		if err := blocked.Send(Record{Seq: uint64(i)}); err != nil {
			t.Fatalf("fill ack buffer at %d: %v", i, err)
		}
	}
	_ = blocked.Close()
	if err := blocked.Send(Record{}); err != io.EOF {
		t.Fatalf("Send on closed stream with full buffer = %v, want EOF", err)
	}
}

func TestDiscardRejectsCancelledOpenAndLogsVerboseRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&Discard{}).OpenStream(ctx, "producer"); err != context.Canceled {
		t.Fatalf("OpenStream error = %v, want context canceled", err)
	}

	var output bytes.Buffer
	d := &Discard{Verbose: true, Log: slog.New(slog.NewTextHandler(&output, nil))}
	st, err := d.OpenStream(context.Background(), "producer")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(Record{Seq: 3, Metric: "temperature"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "temperature") {
		t.Fatalf("verbose log did not contain record: %s", output.String())
	}
	if _, err := st.Recv(); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
}
