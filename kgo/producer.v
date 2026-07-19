// The synchronous producer: partition records, group them per partition
// leader, build one RecordBatch per partition, and issue ProduceRequests
// to each leader. Correctness-first per the port plan; pipelined/async
// sinks come later.
module kgo

import kbin
import kmsg
import krec
import kerr
import time

// ProduceError describes one partition's produce failure.
pub struct ProduceError {
	kerr.Error
pub:
	topic     string
	partition int
	code      i16
	detail    string
}

// msg implements IError.
pub fn (e ProduceError) msg() string {
	return 'produce to ${e.topic}[${e.partition}] failed: ${e.detail}'
}

// produce synchronously produces records to topic. Records without a
// pinned partition are assigned one by cfg.partitioner; records without a
// timestamp get the current time. On success every record's topic,
// partition and offset are filled in. The first failing partition's error
// is returned (records of other partitions may still have succeeded and
// carry their offsets).
pub fn (mut c Client) produce(topic string, mut records []Record) ! {
	if records.len == 0 {
		return
	}
	// ensure we know the topic's partitions and leaders
	mut nparts := c.partition_count(topic) or { 0 }
	if nparts == 0 {
		c.metadata([topic])!
		nparts = c.partition_count(topic) or { 0 }
	}
	if nparts == 0 {
		return error('topic ${topic}: no partitions known')
	}

	now := time.now().unix_milli()
	for mut r in records {
		r.topic = topic
		if r.timestamp == 0 {
			r.timestamp = now
		}
		if r.partition < 0 {
			r.partition = c.cfg.partitioner.partition(r, nparts)
		}
		if r.partition >= nparts {
			return error('record pins partition ${r.partition}, topic has ${nparts}')
		}
	}

	// group record indices per partition, preserving order
	mut by_partition := map[int][]int{}
	for i, r in records {
		by_partition[r.partition] << i
	}
	// group partitions per leader
	mut by_leader := map[int][]int{}
	for partition, _ in by_partition {
		leader := c.partition_leader(topic, partition) or {
			return error('no leader known for ${topic}[${partition}]')
		}
		by_leader[leader] << partition
	}

	for leader, partitions in by_leader {
		c.produce_to_leader(leader, topic, partitions, by_partition, mut records)!
	}
}

fn (mut c Client) produce_to_leader(leader int, topic string, partitions []int, by_partition map[int][]int, mut records []Record) ! {
	mut req := kerr.ProduceRequest{
		acks:           c.cfg.required_acks
		timeout_millis: int(i64(c.cfg.produce_timeout) / 1000000)
	}
	mut req_topic := kerr.ProduceRequestTopic{
		topic: topic
	}
	for partition in partitions {
		idxs := by_partition[partition]
		mut batch_records := []kerr.Record{cap: idxs.len}
		for i in idxs {
			batch_records << records[i]
		}
		batch := kerr.build_record_batch(batch_records, kerr.BatchOpts{
			codec: c.cfg.compression
		})!
		req_topic.partitions << kerr.ProduceRequestTopicPartition{
			partition: partition
			records:   batch
		}
	}
	req.topics = [req_topic]

	body := c.request_broker(leader, mut req)!
	mut resp := kerr.ProduceResponse{
		version: req.version
	}
	mut r := kerr.Reader{
		src: body
	}
	resp.read_from(mut r)!

	for t in resp.topics {
		for p in t.partitions {
			idxs := by_partition[p.partition] or { continue }
			if p.error_code != 0 {
				e := kerr.error_for_code(p.error_code) or { kerr.unknown_server_error }
				return kerr.ProduceError{
					topic:     topic
					partition: p.partition
					code:      p.error_code
					detail:    e.msg()
				}
			}
			for j, i in idxs {
				records[i].offset = p.base_offset + j
			}
		}
	}
}

// partition_count returns the number of partitions cached for topic.
pub fn (mut c Client) partition_count(topic string) ?int {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	meta := c.meta or { return none }
	for t in meta.topics {
		tname := t.topic or { continue }
		if tname == topic {
			if t.partitions.len == 0 {
				return none
			}
			return t.partitions.len
		}
	}
	return none
}
