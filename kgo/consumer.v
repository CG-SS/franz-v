// The direct consumer: explicit topic/partition cursors, ListOffsets-based
// start positions, and per-leader fetch rounds. Correctness-first per the
// port plan; consumer groups build on top of this in the next stage.
module kgo

import kbin
import kerr
import kmsg
import krec

// IsolationLevel selects transactional visibility for fetches.
pub enum IsolationLevel {
	read_uncommitted = 0
	read_committed   = 1
}

// StartOffset selects where consumption begins.
pub enum StartOffset {
	earliest
	latest
}

// ConsumerOpts configures new_consumer.
pub struct ConsumerOpts {
pub mut:
	// start is where cursors begin for each partition.
	start krec.StartOffset = .earliest
}

// Consumer consumes explicit topics with per-partition cursors. Obtain one
// from Client.new_consumer.
@[heap]
pub struct Consumer {
pub mut:
	client &krec.Client
mut:
	cursors map[string]i64 // 'topic/partition' -> next offset to fetch
}

fn cursor_key(topic string, partition int) string {
	return '${topic}/${partition}'
}

// new_consumer builds a consumer over every partition of the given topics,
// with cursors resolved via ListOffsets according to opts.start.
pub fn (mut c Client) new_consumer(topics []string, opts ConsumerOpts) !&Consumer {
	c.metadata(topics)!
	mut co := &krec.Consumer{
		client: c
	}
	ts := if opts.start == .earliest { i64(-2) } else { i64(-1) }
	for topic in topics {
		nparts := c.partition_count(topic) or { return error('topic ${topic}: no partitions') }
		// group partitions per leader for ListOffsets
		mut by_leader := map[int][]int{}
		for p in 0 .. nparts {
			leader := c.partition_leader(topic, p) or {
				return error('no leader for ${topic}[${p}]')
			}
			by_leader[leader] << p
		}
		for leader, parts in by_leader {
			offsets := c.list_offsets(leader, topic, parts, ts)!
			for p, off in offsets {
				co.cursors[cursor_key(topic, p)] = off
			}
		}
	}
	return co
}

// list_offsets resolves offsets for partitions of one topic on one leader;
// ts is -2 for earliest, -1 for latest.
fn (mut c Client) list_offsets(leader int, topic string, partitions []int, ts i64) !map[int]i64 {
	mut req := krec.ListOffsetsRequest{
		replica_id: -1
		topics:     [
			krec.ListOffsetsRequestTopic{
				topic:      topic
				partitions: partitions.map(krec.ListOffsetsRequestTopicPartition{
					partition: it
					timestamp: ts
				})
			},
		]
	}
	body := c.request_broker(leader, mut req)!
	mut resp := krec.ListOffsetsResponse{
		version: req.version
	}
	mut r := krec.Reader{
		src: body
	}
	resp.read_from(mut r)!
	mut out := map[int]i64{}
	for t in resp.topics {
		for p in t.partitions {
			if p.error_code != 0 {
				e := krec.error_for_code(p.error_code) or { krec.unknown_server_error }
				return error('list offsets ${topic}[${p.partition}]: ${e.msg()}')
			}
			// v0 returns old_style_offsets; v1+ a single offset
			if req.version == 0 && p.old_style_offsets.len > 0 {
				out[p.partition] = p.old_style_offsets[0]
			} else {
				out[p.partition] = p.offset
			}
		}
	}
	return out
}

// seek positions one partition cursor at the given offset.
pub fn (mut co Consumer) seek(topic string, partition int, offset i64) {
	co.cursors[cursor_key(topic, partition)] = offset
}

// position returns the next offset that poll would fetch for a partition.
pub fn (mut co Consumer) position(topic string, partition int) ?i64 {
	return co.cursors[cursor_key(topic, partition)] or { return none }
}

struct FetchTarget {
mut:
	topic      string
	partitions []int
}

// poll performs one fetch round across all cursors, returning the records
// received (possibly none if no data arrived within fetch_max_wait).
// Cursors advance past returned records. Recoverable partition conditions
// (offset out of range, moved leadership) are handled internally: offsets
// reset per cfg.offset_reset, metadata refreshes for the next poll.
pub fn (mut co Consumer) poll() ![]Record {
	mut c := co.client
	// group cursors: leader -> topic -> partitions
	mut leader_topics := map[int]map[string]krec.FetchTarget{}
	for key, _ in co.cursors {
		idx := key.last_index('/') or { continue }
		topic := key[..idx]
		partition := key[idx + 1..].int()
		leader := c.partition_leader(topic, partition) or {
			c.metadata([topic])!
			c.partition_leader(topic, partition) or {
				return error('no leader for ${topic}[${partition}]')
			}
		}
		if topic !in leader_topics[leader] {
			leader_topics[leader][topic] = krec.FetchTarget{
				topic: topic
			}
		}
		leader_topics[leader][topic].partitions << partition
	}

	mut out := []krec.Record{}
	for leader, targets in leader_topics {
		co.fetch_from(leader, targets, mut out) or {
			if is_retriable_err(err) {
				continue // transient; next poll retries
			}
			return err
		}
	}
	return out
}

fn (mut co Consumer) fetch_from(leader int, targets map[string]FetchTarget, mut out []Record) ! {
	mut c := co.client
	mut req := krec.FetchRequest{
		replica_id:      -1
		max_wait_millis: int(i64(c.cfg.fetch_max_wait) / 1000000)
		min_bytes:       c.cfg.fetch_min_bytes
		max_bytes:       c.cfg.fetch_max_bytes
		isolation_level: i8(int(c.cfg.isolation_level))
	}
	for _, target in targets {
		mut ft := krec.FetchRequestTopic{
			topic: target.topic
		}
		for p in target.partitions {
			ft.partitions << krec.FetchRequestTopicPartition{
				partition:            p
				fetch_offset:         co.cursors[cursor_key(target.topic, p)]
				current_leader_epoch: -1
				log_start_offset:     -1
				partition_max_bytes:  c.cfg.fetch_partition_max_bytes
			}
		}
		req.topics << ft
	}

	// Fetch v13+ addresses topics by uuid (KIP-516); stay on v12 until
	// topic-id resolution is implemented.
	body := c.request_broker_capped(leader, mut req, 12)!
	mut resp := krec.FetchResponse{
		version: req.version
	}
	mut r := krec.Reader{
		src: body
	}
	resp.read_from(mut r)!

	for t in resp.topics {
		for p in t.partitions {
			key := cursor_key(t.topic, p.partition)
			if p.error_code != 0 {
				co.handle_partition_error(t.topic, p.partition, p.error_code)!
				continue
			}
			batches := p.record_batches or { []u8{} }
			parsed := krec.parse_batches_meta(batches)!
			cursor := co.cursors[key]
			mut next := cursor

			// aborted transactions of this response, activated once the
			// scan reaches their first offset (read_committed only)
			mut aborted := (p.aborted_transactions or {
				[]krec.FetchResponseTopicPartitionAbortedTransaction{}
			}).clone()

			aborted.sort(a.first_offset < b.first_offset)
			mut next_abort := 0
			mut active_aborts := map[i64]bool{}

			for pb in parsed {
				base := pb.batch.first_offset
				for next_abort < aborted.len && aborted[next_abort].first_offset <= base {
					active_aborts[aborted[next_abort].producer_id] = true
					next_abort++
				}
				batch_end := base + i64(pb.batch.last_offset_delta) + 1
				if krec.is_control(pb.batch) {
					// a marker closes its producer's transaction; control
					// records are never returned to the caller
					for rec in pb.records {
						if commit := krec.control_marker_is_commit(rec) {
							_ = commit
							active_aborts.delete(pb.batch.producer_id)
						}
					}
					if batch_end > next {
						next = batch_end
					}
					continue
				}
				if c.cfg.isolation_level == .read_committed && krec.is_transactional(pb.batch)
					&& pb.batch.producer_id in active_aborts {
					// skip the aborted transaction's data entirely
					if batch_end > next {
						next = batch_end
					}
					continue
				}
				for rec in pb.records {
					if rec.offset < cursor {
						continue
					}
					mut owned := rec
					owned.topic = t.topic
					owned.partition = p.partition
					out << owned
				}
				if batch_end > next {
					next = batch_end
				}
			}
			co.cursors[key] = next
		}
	}
}

// handle_partition_error deals with per-partition fetch errors:
// out-of-range offsets reset per policy; leadership changes refresh
// metadata for the next poll; anything else surfaces.
fn (mut co Consumer) handle_partition_error(topic string, partition int, code i16) ! {
	mut c := co.client
	match code {
		1 { // OFFSET_OUT_OF_RANGE: reset per policy
			ts := if c.cfg.offset_reset == .earliest { i64(-2) } else { i64(-1) }
			leader := c.partition_leader(topic, partition) or {
				return error('no leader for ${topic}[${partition}]')
			}
			offsets := c.list_offsets(leader, topic, [partition], ts)!
			co.cursors[cursor_key(topic, partition)] = offsets[partition]
			c.cfg.log(.info,
				'reset ${topic}[${partition}] to ${offsets[partition]} (offset out of range)')
		}
		6, 9 { // NOT_LEADER / REPLICA_NOT_AVAILABLE: refresh for next poll
			c.metadata([topic])!
		}
		else {
			e := krec.error_for_code(code) or { krec.unknown_server_error }
			return error('fetch ${topic}[${partition}]: ${e.msg()}')
		}
	}
}
