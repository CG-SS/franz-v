module kgo

import kfake
import krec
import time

// ---------------------------------------------------------------------------
// murmur2: golden vectors from Kafka's UtilsTest.testMurmur2 — placement
// must match the Java reference exactly.
// ---------------------------------------------------------------------------

fn test_murmur2_kafka_vectors() {
	vectors := {
		'21':                                               i32(-973932308)
		'foobar':                                           i32(-790332482)
		'a-little-bit-long-string':                         i32(-985981536)
		'a-little-bit-longer-string':                       i32(-1486304829)
		'lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8': i32(-58897971)
		'abc':                                              i32(479470107)
	}
	for input, want in vectors {
		assert i32(murmur2(input.bytes())) == want, 'murmur2(${input})'
	}
}

fn test_partitioners() {
	mut kp := krec.KafkaPartitioner{}
	keyed := krec.Record{
		key: 'foobar'.bytes()
	}
	// deterministic: (murmur2 & 0x7fffffff) % 16
	want := int((u32(u32(-790332482)) & 0x7fffffff) % 16)
	assert kp.partition(&keyed, 16) == want
	assert kp.partition(&keyed, 16) == want // stable

	// keyless round-robins
	keyless := krec.Record{}
	first := kp.partition(&keyless, 4)
	second := kp.partition(&keyless, 4)
	assert second == (first + 1) % 4

	mut rr := krec.RoundRobinPartitioner{}
	assert rr.partition(&keyed, 3) == 0
	assert rr.partition(&keyed, 3) == 1
	assert rr.partition(&keyed, 3) == 2
	assert rr.partition(&keyed, 3) == 0
}

// ---------------------------------------------------------------------------
// Batch build/parse
// ---------------------------------------------------------------------------

fn sample_records() []Record {
	return [
		krec.Record{
			key:       'k1'.bytes()
			value:     'first value'.bytes()
			timestamp: 1700000000000
			headers:   [
				krec.RecordHeader{
					key:   'trace'
					value: 'abc123'.bytes()
				},
			]
		},
		krec.Record{
			value:     'keyless'.bytes()
			timestamp: 1700000000005
		},
		krec.Record{
			key:       'k3'.bytes()
			timestamp: 1700000000009
		},
	]
}

fn test_batch_roundtrip_all_supported_codecs() {
	for codec in [krec.Codec.uncompressed, .gzip, .snappy, .zstd] {
		batch := krec.build_record_batch(sample_records(), krec.BatchOpts{
			codec: codec
		}) or {
			assert false, '${codec}: build failed: ${err}'
			return
		}
		meta, recs := krec.parse_record_batch(batch) or {
			assert false, '${codec}: parse failed: ${err}'
			return
		}
		assert meta.num_records == 3
		assert int(meta.attributes) & 0x07 == int(codec)
		assert meta.first_timestamp == 1700000000000
		assert meta.max_timestamp == 1700000000009
		assert recs.len == 3
		k0 := recs[0].key or { []u8{} }
		assert k0.bytestr() == 'k1'
		v0 := recs[0].value or { []u8{} }
		assert v0.bytestr() == 'first value'
		assert recs[0].headers.len == 1
		assert recs[0].headers[0].key == 'trace'
		assert recs[1].key == none
		assert recs[1].timestamp == 1700000000005
		assert recs[2].value == none
		assert recs[2].offset == 2
	}
}

fn test_batch_lz4_unsupported() {
	if _ := krec.build_record_batch(sample_records(), krec.BatchOpts{
		codec: .lz4
	})
	{
		assert false, 'lz4 must be rejected'
	}
}

fn test_batch_crc_tamper_detected() {
	mut batch := krec.build_record_batch(sample_records(), krec.BatchOpts{}) or {
		assert false, '${err}'
		return
	}
	batch[batch.len - 1] ^= 0xff
	_, _ := krec.parse_record_batch(batch) or {
		assert err is krec.BatchCrcError
		return
	}
	assert false, 'tampered batch must fail crc'
}

// ---------------------------------------------------------------------------
// End-to-end produce through kfake
// ---------------------------------------------------------------------------

fn test_produce_end_to_end() {
	mut cl := krec.start(1, krec.ClusterCfg{})
	mut c := new_client(krec.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}

	mut records := [
		krec.Record{
			key:     'order-1'.bytes()
			value:   'created'.bytes()
			headers: [
				krec.RecordHeader{
					key:   'source'
					value: 'franz-v'.bytes()
				},
			]
		},
		krec.Record{
			key:   'order-1'.bytes()
			value: 'paid'.bytes()
		},
		krec.Record{
			value: 'keyless event'.bytes()
		},
	]
	c.produce('orders', mut records) or {
		assert false, 'produce failed: ${err}'
		return
	}

	// every record got topic/partition/offset assigned
	for r in records {
		assert r.topic == 'orders'
		assert r.partition == 0
		assert r.offset >= 0
	}
	assert records[0].offset == 0
	assert records[1].offset == 1
	assert records[2].offset == 2

	// the fake verified crc + decoded the batch; stored content matches
	stored := cl.records('orders', 0)
	assert stored.len == 3
	sk := stored[0].key or { []u8{} }
	assert sk.bytestr() == 'order-1'
	sv := stored[0].value or { []u8{} }
	assert sv.bytestr() == 'created'
	assert stored[0].headers.len == 1
	assert stored[0].headers[0].key == 'source'
	hv := stored[0].headers[0].value or { []u8{} }
	assert hv.bytestr() == 'franz-v'
	assert stored[2].key == none
	assert stored[0].timestamp > 0

	// a second produce continues offsets
	mut more := [
		krec.Record{
			value: 'refunded'.bytes()
		},
	]
	c.produce('orders', mut more) or {
		assert false, '${err}'
		return
	}
	assert more[0].offset == 3
	assert cl.records('orders', 0).len == 4
}

fn test_produce_all_codecs_through_wire() {
	for codec in [krec.Codec.uncompressed, .gzip, .snappy, .zstd] {
		mut cl := krec.start(1, krec.ClusterCfg{})
		mut c := new_client(krec.Config{
			seed_brokers: [cl.seed_addr()]
			compression:  codec
		}) or {
			assert false, '${err}'
			return
		}
		mut records := sample_records()
		c.produce('t', mut records) or {
			assert false, '${codec} produce failed: ${err}'
			c.close()
			return
		}
		stored := cl.records('t', 0)
		assert stored.len == 3, '${codec}: stored ${stored.len}'
		v := stored[0].value or { []u8{} }
		assert v.bytestr() == 'first value', '${codec}'
		c.close()
	}
}

fn test_produce_multi_partition_multi_leader() {
	// 3 nodes, 6 partitions; leader(p) = p % 3
	mut cl := krec.start(3, krec.ClusterCfg{
		partitions_per_topic: 6
	})
	mut c := new_client(krec.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}

	mut records := []krec.Record{}
	for p in 0 .. 6 {
		records << krec.Record{
			partition: p
			value:     'to-p${p}'.bytes()
		}
		records << krec.Record{
			partition: p
			value:     'to-p${p}-again'.bytes()
		}
	}
	c.produce('spread', mut records) or {
		assert false, 'produce failed: ${err}'
		return
	}
	for p in 0 .. 6 {
		stored := cl.records('spread', p)
		assert stored.len == 2, 'partition ${p}'
		v := stored[0].value or { []u8{} }
		assert v.bytestr() == 'to-p${p}'
		assert stored[0].offset == 0
		assert stored[1].offset == 1
	}
	// keyed placement is murmur2-deterministic across the same topic
	mut keyed := [
		krec.Record{
			key:   'foobar'.bytes()
			value: 'x'.bytes()
		},
	]
	c.produce('spread', mut keyed) or {
		assert false, '${err}'
		return
	}
	assert keyed[0].partition == int((u32(u32(-790332482)) & 0x7fffffff) % 6)
}

fn test_produce_partition_error_surfaces() {
	mut cl := krec.start(1, krec.ClusterCfg{
		produce_error_code: 6 // NOT_LEADER_FOR_PARTITION
	})
	mut c := new_client(krec.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut records := [
		krec.Record{
			value: 'v'.bytes()
		},
	]
	c.produce('t', mut records) or {
		assert err is krec.ProduceError
		if err is krec.ProduceError {
			assert err.code == 6
			assert err.detail.contains('NOT_LEADER')
		}
		return
	}
	assert false, 'produce must surface the partition error'
}

fn test_produce_empty_and_timestamp_defaulting() {
	mut cl := krec.start(1, krec.ClusterCfg{})
	mut c := new_client(krec.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut empty := []krec.Record{}
	c.produce('t', mut empty) or {
		assert false, 'empty produce must be a no-op: ${err}'
		return
	}

	before := time.now().unix_milli()
	mut records := [
		krec.Record{
			value: 'v'.bytes()
		},
	]
	c.produce('t', mut records) or {
		assert false, '${err}'
		return
	}
	assert records[0].timestamp >= before
	assert records[0].timestamp <= time.now().unix_milli()
}
