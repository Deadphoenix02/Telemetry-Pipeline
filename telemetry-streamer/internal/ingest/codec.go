// Package ingest is the gRPC side: a client transport for the streamer and a
// small server used as a fake counterpart for testing.
package ingest

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"

	"telemetry-streamer/internal/wire"
)

// Codec encodes wire.Record / wire.Ack as the protobuf messages in
// proto/ingest.proto, without generated code. Field numbers must match.
type Codec struct{}

func (Codec) Name() string { return "proto" }

func (Codec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case *wire.Record:
		return appendRecord(make([]byte, 0, 256), m), nil
	case *wire.Ack:
		return putVarint(nil, 1, m.Seq), nil
	default:
		return nil, fmt.Errorf("ingest codec: cannot marshal %T", v)
	}
}

func (Codec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case *wire.Record:
		return parseRecord(data, m)
	case *wire.Ack:
		return parseAck(data, m)
	default:
		return fmt.Errorf("ingest codec: cannot unmarshal into %T", v)
	}
}

func putVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b // proto3 omits defaults
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func putString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendRecord(b []byte, r *wire.Record) []byte {
	b = putVarint(b, 1, r.Seq)
	b = putVarint(b, 2, uint64(r.TimestampNanos))
	b = putString(b, 3, r.Metric)
	b = putString(b, 4, r.GPUID)
	b = putString(b, 5, r.Device)
	b = putString(b, 6, r.UUID)
	b = putString(b, 7, r.ModelName)
	b = putString(b, 8, r.Hostname)
	b = putString(b, 9, r.Container)
	b = putString(b, 10, r.Pod)
	b = putString(b, 11, r.Namespace)
	if r.Value != 0 {
		b = protowire.AppendTag(b, 12, protowire.Fixed64Type)
		b = protowire.AppendFixed64(b, math.Float64bits(r.Value))
	}
	return b
}

func parseRecord(b []byte, r *wire.Record) error {
	*r = wire.Record{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case typ == protowire.VarintType && (num == 1 || num == 2):
			x, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			if num == 1 {
				r.Seq = x
			} else {
				r.TimestampNanos = int64(x)
			}
		case typ == protowire.BytesType && num >= 3 && num <= 11:
			s, n := protowire.ConsumeString(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case 3:
				r.Metric = s
			case 4:
				r.GPUID = s
			case 5:
				r.Device = s
			case 6:
				r.UUID = s
			case 7:
				r.ModelName = s
			case 8:
				r.Hostname = s
			case 9:
				r.Container = s
			case 10:
				r.Pod = s
			case 11:
				r.Namespace = s
			}
		case typ == protowire.Fixed64Type && num == 12:
			x, n := protowire.ConsumeFixed64(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			r.Value = math.Float64frombits(x)
		default: // unknown field: skip
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func parseAck(b []byte, a *wire.Ack) error {
	*a = wire.Ack{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if num == 1 && typ == protowire.VarintType {
			x, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			a.Seq = x
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
	}
	return nil
}
