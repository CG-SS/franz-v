// Real-broker smoke test for franz-v: point it at any Kafka-compatible
// broker (Redpanda, Apache Kafka, or the bundled kfake) and it will
// negotiate versions, create topics, produce records with every supported
// codec, fetch them back raw, and byte-compare the round trip.
//
//	v run examples/real_broker_smoke.v [broker:port]   (default 127.0.0.1:9092)
//
// Exit code 0 means every step verified.
module main

import kbin
import kgo
import kmsg
import krec
import os

fn fail(msg string) {
	eprintln('SMOKE FAIL: ${msg}')
	exit(1)
}

// create_topic best-effort creates a single-partition topic; brokers that
// do not support CreateTopics (e.g. kfake, which auto-creates) are fine.
fn create_topic(mut c kgo.Client, topic string) {
	mut req := kmsg.CreateTopicsRequest{
		timeout_millis: 10000
		topics:         [
			kmsg.CreateTopicsRequestTopic{
				topic:              topic
				num_partitions:     1
				replication_factor: 1
			},
		]
	}
	body := c.request(mut req) or {
		println('  create ${topic}: skipped (${err.msg()})')
		return
	}
	mut resp := kmsg.CreateTopicsResponse{
		version: req.version
	}
	mut r := kbin.Reader{
		src: body
	}
	resp.read_from(mut r) or {
		fail('CreateTopics response decode: ${err}')
		return
	}
	for t in resp.topics {
		if t.error_code != 0 && t.error_code != 36 { // 36: TOPIC_ALREADY_EXISTS
			fail('create topic ${topic}: error code ${t.error_code}')
		}
	}
	println('  create ${topic}: ok')
}

// fetch_all fetches records from offset 0 of topic/partition 0 and parses
// every returned batch.
fn fetch_all(mut c kgo.Client, topic string) []krec.Record {
	// use uuid addressing (Fetch v13+) when metadata taught us the id
	tid := c.topic_id(topic) or { [16]u8{} }
	mut req := kmsg.FetchRequest{
		replica_id:      -1
		session_epoch:   -1
		max_wait_millis: 500
		min_bytes:       1
		max_bytes:       1 << 20
		topics:          [
			kmsg.FetchRequestTopic{
				topic:      topic
				topic_id:   tid
				partitions: [
					kmsg.FetchRequestTopicPartition{
						partition:            0
						fetch_offset:         0
						current_leader_epoch: -1
						log_start_offset:     -1
						partition_max_bytes:  1 << 20
					},
				]
			},
		]
	}
	zero := [16]u8{}
	body := if tid != zero {
		c.request(mut req) or {
			fail('fetch: ${err.msg()}')
			return []
		}
	} else {
		c.request_capped(mut req, 12) or {
			fail('fetch: ${err.msg()}')
			return []
		}
	}
	mut resp := kmsg.FetchResponse{
		version: req.version
	}
	mut r := kbin.Reader{
		src: body
	}
	resp.read_from(mut r) or {
		fail('fetch decode (v${req.version}): ${err}')
		return []
	}
	if resp.topics.len == 0 || resp.topics[0].partitions.len == 0 {
		fail('fetch: empty response structure')
		return []
	}
	part := resp.topics[0].partitions[0]
	if part.error_code != 0 {
		fail('fetch: partition error code ${part.error_code}')
	}
	batches := part.record_batches or { []u8{} }
	return krec.parse_record_batches(batches) or {
		fail('parse fetched batches: ${err}')
		return []
	}
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:9092' }
	println('franz-v real-broker smoke against ${addr}')

	mut c := kgo.new_client(kgo.Config{
		seed_brokers: [addr]
	}) or {
		fail('client: ${err.msg()}')
		return
	}
	defer {
		c.close()
	}
	meta := c.metadata([]) or {
		fail('metadata: ${err.msg()}')
		return
	}
	cluster := meta.cluster_id or { '<none>' }
	println('  connected: cluster ${cluster}, ${meta.brokers.len} broker(s)')
	for key in [i16(0), 1, 3, 19] {
		v := c.negotiated_version(addr, key) or { i16(-1) }
		println('  negotiated ${kmsg.name_for_key(key)}: v${v}')
	}

	codecs := [krec.Codec.uncompressed, .gzip, .snappy, .zstd]
	for codec in codecs {
		topic := 'franzv-smoke-${codec}'
		println('- codec ${codec}:')
		create_topic(mut c, topic)

		c.cfg.compression = codec
		mut records := [
			krec.Record{
				key:     'k-${codec}'.bytes()
				value:   'value one via ${codec}'.bytes()
				headers: [
					krec.RecordHeader{
						key:   'codec'
						value: '${codec}'.bytes()
					},
				]
			},
			krec.Record{
				value: 'value two via ${codec}'.bytes()
			},
			krec.Record{
				key: 'tombstone'.bytes()
			},
		]
		c.produce(topic, mut records) or {
			fail('produce (${codec}): ${err.msg()}')
			return
		}
		println('  produced 3 records at offsets ${records.map(it.offset)}')

		fetched := fetch_all(mut c, topic)
		if fetched.len < 3 {
			fail('${codec}: fetched ${fetched.len} records, want >= 3')
		}
		base := fetched.len - 3
		fk := fetched[base].key or { []u8{} }
		fv := fetched[base].value or { []u8{} }
		if fk.bytestr() != 'k-${codec}' || fv.bytestr() != 'value one via ${codec}' {
			fail('${codec}: first record mismatch: key=${fk.bytestr()} value=${fv.bytestr()}')
		}
		if fetched[base].headers.len != 1 || fetched[base].headers[0].key != 'codec' {
			fail('${codec}: header mismatch')
		}
		if fetched[base + 1].key != none {
			fail('${codec}: record 2 should be keyless')
		}
		if fetched[base + 2].value != none {
			fail('${codec}: record 3 should be a tombstone (null value)')
		}
		if fetched[base].offset != records[0].offset {
			fail('${codec}: offset mismatch: fetched ${fetched[base].offset}, produced ${records[0].offset}')
		}
		println('  fetched back and verified: keys, values, headers, null-ness, offsets')
	}
	// ------------------------------------------------------------------
	// Consumer API: consume everything just produced across all topics
	// ------------------------------------------------------------------
	topics := codecs.map('franzv-smoke-${it}')
	mut co := c.new_consumer(topics, kgo.ConsumerOpts{}) or {
		fail('new_consumer: ${err.msg()}')
		return
	}
	mut consumed := []krec.Record{}
	// poll until a quiet round (offsets resolved via ListOffsets earliest)
	for _ in 0 .. 10 {
		recs := co.poll() or {
			fail('poll: ${err.msg()}')
			return
		}
		if recs.len == 0 && consumed.len >= 3 * codecs.len {
			break
		}
		consumed << recs
	}
	if consumed.len < 3 * codecs.len {
		fail('consumer: got ${consumed.len} records, want >= ${3 * codecs.len}')
	}
	mut per_topic := map[string]int{}
	for r in consumed {
		per_topic[r.topic]++
	}
	for topic in topics {
		if per_topic[topic] < 3 {
			fail('consumer: topic ${topic} yielded ${per_topic[topic]} records')
		}
	}
	println('- consumer: polled ${consumed.len} records across ${per_topic.len} topics (ListOffsets earliest)')

	// incremental consumption: newly produced records arrive on next poll
	mut extra := [
		krec.Record{
			value: 'post-consumer record'.bytes()
		},
	]
	c.cfg.compression = .uncompressed
	c.produce(topics[0], mut extra) or {
		fail('incremental produce: ${err.msg()}')
		return
	}
	mut got_extra := false
	for _ in 0 .. 10 {
		recs := co.poll() or {
			fail('incremental poll: ${err.msg()}')
			return
		}
		for r in recs {
			v := r.value or { []u8{} }
			if v.bytestr() == 'post-consumer record' && r.offset == extra[0].offset {
				got_extra = true
			}
		}
		if got_extra {
			break
		}
	}
	if !got_extra {
		fail('consumer: incremental record never arrived')
	}
	println('- consumer: incremental record arrived at offset ${extra[0].offset}')
	println('ALL OK: produce + raw fetch + consumer verified for ${codecs.len} codecs')
}
