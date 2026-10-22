package partitioner

import (
	"hash/fnv"
	"math/big"
	"testing"
)

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// The mapping is an on-disk contract: pin it against the stdlib FNV.
func TestMatchesStdlibFNVPlusFinalizer(t *testing.T) {
	var u [16]byte
	for i := range u {
		u[i] = byte(i * 17)
	}
	h := fnv.New32a()
	h.Write(u[:])
	if got, want := Partition(u, 10), int(fmix32(h.Sum32())%10); got != want {
		t.Fatalf("Partition = %d, want %d", got, want)
	}
}

// structured returns UUIDs that differ only in their first and eighth bytes,
// the shape that defeats both plain FNV-1a and uuid % 10.
func structured(i int) [16]byte {
	var u [16]byte
	u[0], u[7], u[15] = byte(i), byte(i*7), 0x42
	return u
}

func TestSpreadsStructuredInput(t *testing.T) {
	seen := map[int]int{}
	for i := 0; i < 250; i++ {
		seen[Partition(structured(i), 10)]++
	}
	if len(seen) != 10 {
		t.Fatalf("only %d of 10 partitions used: %v", len(seen), seen)
	}
	for p, n := range seen {
		if n < 10 || n > 45 { // mean 25
			t.Fatalf("partition %d got %d of 250: %v", p, n, seen)
		}
	}
}

// Documents why the hash exists: the same input taken as a plain 128-bit
// integer modulo 10 never reaches the odd partitions.
func TestPlainModWouldUseHalfThePartitions(t *testing.T) {
	seen := map[int64]bool{}
	for i := 0; i < 250; i++ {
		u := structured(i)
		seen[new(big.Int).Mod(new(big.Int).SetBytes(u[:]), big.NewInt(10)).Int64()] = true
	}
	if len(seen) != 5 {
		t.Fatalf("plain mod used %d partitions; this test documents that it uses only 5", len(seen))
	}
}
