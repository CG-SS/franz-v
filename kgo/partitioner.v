// Partitioners assign partitions to records that do not pin one.
module kgo

// Partitioner chooses a partition for a record among num_partitions.
// Implementations may keep state (e.g. round-robin cursors), hence the
// mut method.
pub interface Partitioner {
mut:
	partition(r &Record, num_partitions int) int
}

// murmur2 is Kafka's partitioning hash (seed 0x9747b28c); it must match
// the Java reference implementation exactly so keyed records land on the
// same partitions as other clients place them.
fn murmur2(b []u8) u32 {
	seed := u32(0x9747b28c)
	m := u32(0x5bd1e995)
	r := 24
	mut h := seed ^ u32(b.len)
	mut i := 0
	for b.len - i >= 4 {
		mut k := u32(b[i + 3]) << 24 | u32(b[i + 2]) << 16 | u32(b[i + 1]) << 8 | u32(b[i])
		i += 4
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	rem := b.len - i
	if rem >= 3 {
		h ^= u32(b[i + 2]) << 16
	}
	if rem >= 2 {
		h ^= u32(b[i + 1]) << 8
	}
	if rem >= 1 {
		h ^= u32(b[i])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// KafkaPartitioner matches the Java client's default placement: keyed
// records hash with murmur2, keyless records round-robin. The default.
pub struct KafkaPartitioner {
mut:
	rr u32
}

// partition implements Partitioner.
pub fn (mut p KafkaPartitioner) partition(r &Record, num_partitions int) int {
	if key := r.key {
		if key.len > 0 {
			return int((murmur2(key) & 0x7fffffff) % u32(num_partitions))
		}
	}
	p.rr++
	return int((p.rr - 1) % u32(num_partitions))
}

// RoundRobinPartitioner ignores keys and cycles through partitions.
pub struct RoundRobinPartitioner {
mut:
	rr u32
}

// partition implements Partitioner.
pub fn (mut p RoundRobinPartitioner) partition(r &Record, num_partitions int) int {
	p.rr++
	return int((p.rr - 1) % u32(num_partitions))
}
