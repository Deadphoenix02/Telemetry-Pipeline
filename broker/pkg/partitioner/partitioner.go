// Package partitioner maps a GPU UUID to a partition. The function is part of
// the on-disk contract (recorded in topic.json): changing it breaks per-GPU
// ordering for data already in the log.
package partitioner

// Name identifies the hash in topic.json.
const Name = "fnv1a32-fmix32"

// Partition returns FNV-1a 32 of the 16 raw UUID bytes, passed through the
// murmur3 finalizer, modulo n. A hash is needed because the UUID itself is a
// poor number to take modulo 10: 256^k ≡ 6 (mod 10) for k ≥ 1, so uuid % 10
// reduces to (byte sum mod 5, parity of the last byte), and any UUIDs sharing
// a last byte's parity fill only half the partitions. Plain FNV-1a has a
// similar flaw (its low bit is the XOR of the input bytes' low bits), which the
// finalizer removes by mixing every input bit into every output bit.
func Partition(uuid [16]byte, n int) int {
	h := uint32(2166136261)
	for _, c := range uuid {
		h ^= uint32(c)
		h *= 16777619
	}
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return int(h % uint32(n))
}
