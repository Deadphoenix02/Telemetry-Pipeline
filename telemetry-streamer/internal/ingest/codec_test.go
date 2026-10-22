package ingest

import (
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"telemetry-streamer/internal/wire"
)

func TestCodecRecordRoundTripAndUnknownField(t *testing.T) {
	want := &wire.Record{
		Seq: 41, TimestampNanos: -123456, Metric: "power", GPUID: "2", Device: "gpu",
		UUID: "uuid", ModelName: "model", Hostname: "host", Container: "container",
		Pod: "pod", Namespace: "ns", Value: math.Pi,
	}
	codec := Codec{}
	encoded, err := codec.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	encoded = protowire.AppendTag(encoded, 100, protowire.VarintType)
	encoded = protowire.AppendVarint(encoded, 9)
	var got wire.Record
	if err := codec.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got != *want {
		t.Fatalf("round trip = %+v, want %+v", got, *want)
	}
}

func TestCodecDefaultsAckAndErrors(t *testing.T) {
	codec := Codec{}
	if codec.Name() != "proto" {
		t.Fatalf("Name() = %q", codec.Name())
	}
	zero, err := codec.Marshal(&wire.Record{})
	if err != nil || len(zero) != 0 {
		t.Fatalf("zero record marshal = %v, %v", zero, err)
	}
	var record wire.Record
	if err := codec.Unmarshal(nil, &record); err != nil || record != (wire.Record{}) {
		t.Fatalf("zero record unmarshal = %+v, %v", record, err)
	}

	encoded, err := codec.Marshal(&wire.Ack{Seq: 8})
	if err != nil {
		t.Fatal(err)
	}
	var ack wire.Ack
	if err := codec.Unmarshal(encoded, &ack); err != nil || ack.Seq != 8 {
		t.Fatalf("ack round trip = %+v, %v", ack, err)
	}
	if _, err := codec.Marshal("unsupported"); err == nil {
		t.Fatal("Marshal accepted unsupported value")
	}
	if err := codec.Unmarshal(nil, new(string)); err == nil {
		t.Fatal("Unmarshal accepted unsupported destination")
	}
	if err := codec.Unmarshal([]byte{0x08}, &record); err == nil {
		t.Fatal("Unmarshal accepted a truncated varint")
	}
	if err := codec.Unmarshal([]byte{0x0a, 0x05, 'x'}, &record); err == nil {
		t.Fatal("Unmarshal accepted truncated string field")
	}
	if err := codec.Unmarshal([]byte{0x08}, &ack); err == nil {
		t.Fatal("Ack unmarshal accepted a truncated varint")
	}
	unknown := protowire.AppendTag(nil, 8, protowire.BytesType)
	unknown = protowire.AppendString(unknown, "ignored")
	if err := codec.Unmarshal(unknown, &ack); err != nil || ack.Seq != 0 {
		t.Fatalf("unknown ack field = %+v, %v", ack, err)
	}
}
