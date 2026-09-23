// Transactions: a TxnProducer wraps a Client with a transactional id,
// producer id/epoch, and per-partition sequences, implementing the classic
// transaction flow — InitProducerId, AddPartitionsToTxn before first
// produce to a partition, transactional record batches, TxnOffsetCommit
// for exactly-once consume-transform-produce, and EndTxn commit/abort.
//
// Like group members, give each TxnProducer its own Client.
module kgo

import kbin
import kerr
import kmsg
import krec
import time

const ec_producer_fenced = i16(90)
const ec_invalid_producer_epoch = i16(47)
const ec_concurrent_transactions = i16(51)

// TxnProducer produces within transactions. Obtain one from
// Client.new_txn_producer.
@[heap]
pub struct TxnProducer {
pub mut:
	client           &Client
	transactional_id string
mut:
	coordinator    int = -2147483648
	producer_id    i64 = -1
	producer_epoch i16 = -1
	in_txn         bool
	added          map[string]bool // 'topic/partition' in the current txn
	seqs           map[string]int  // 'topic/partition' -> next sequence
}

// new_txn_producer finds the transaction coordinator and initializes the
// producer id/epoch for the given transactional id. Re-initializing the
// same id bumps the epoch, fencing any previous producer (zombie fencing).
pub fn (mut c Client) new_txn_producer(transactional_id string) !&TxnProducer {
	if transactional_id == '' {
		return error('transactional id must be non-empty')
	}
	// coordinator requests are addressed by node id: make sure the
	// cluster's brokers are discovered first
	if c.known_brokers().len == 0 {
		c.metadata([])!
	}
	mut t := &TxnProducer{
		client:           c
		transactional_id: transactional_id
	}
	t.find_txn_coordinator()!
	t.init_producer_id()!
	return t
}

fn (mut t TxnProducer) find_txn_coordinator() ! {
	mut c := t.client
	mut req := kmsg.FindCoordinatorRequest{
		coordinator_key:  t.transactional_id
		coordinator_type: 1 // transaction
	}
	// the coordinator's backing topic is created lazily; retry while the
	// broker reports it not yet available
	for attempt in 0 .. 40 {
		body := c.request_capped(mut req, 3)!
		mut resp := kmsg.FindCoordinatorResponse{
			version: req.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		match resp.error_code {
			0 {
				t.coordinator = resp.node_id
				return
			}
			ec_coordinator_not_available, ec_coordinator_load {
				_ = attempt
				time.sleep(250 * time.millisecond)
			}
			else {
				e := kerr.error_for_code(resp.error_code) or { kerr.unknown_server_error }
				return error('find txn coordinator: ${e.msg()}')
			}
		}
	}
	return error('find txn coordinator: not available after retries')
}

fn (mut t TxnProducer) init_producer_id() ! {
	mut c := t.client
	mut req := kmsg.InitProducerIDRequest{
		transactional_id:           t.transactional_id
		transaction_timeout_millis: 60000
	}
	// the coordinator loads its state lazily after (re)start; retry
	for _ in 0 .. 120 {
		body := c.request_broker(t.coordinator, mut req)!
		mut resp := kmsg.InitProducerIDResponse{
			version: req.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		match resp.error_code {
			0 {
				t.producer_id = resp.producer_id
				t.producer_epoch = resp.producer_epoch
				t.seqs = map[string]int{}
				c.cfg.log(.info,
					'txn ${t.transactional_id}: producer id ${t.producer_id} epoch ${t.producer_epoch}')
				return
			}
			ec_coordinator_load, ec_coordinator_not_available {
				time.sleep(250 * time.millisecond)
			}
			ec_not_coordinator {
				t.find_txn_coordinator()!
			}
			else {
				e := kerr.error_for_code(resp.error_code) or { kerr.unknown_server_error }
				return error('init producer id: ${e.msg()}')
			}
		}
	}
	return error('init producer id: coordinator still loading after retries')
}

// producer returns the coordinator-assigned producer id and epoch.
pub fn (t &TxnProducer) producer() (i64, i16) {
	return t.producer_id, t.producer_epoch
}

// begin opens a transaction. Produces before begin are rejected.
pub fn (mut t TxnProducer) begin() ! {
	if t.in_txn {
		return error('transaction already open')
	}
	t.in_txn = true
	t.added = map[string]bool{}
}

// add_partitions registers partitions with the transaction coordinator;
// called automatically on first produce to each partition.
fn (mut t TxnProducer) add_partitions(topic string, partitions []int) ! {
	mut missing := []int{}
	for p in partitions {
		if !t.added['${topic}/${p}'] {
			missing << p
		}
	}
	if missing.len == 0 {
		return
	}
	mut c := t.client
	mut req := kmsg.AddPartitionsToTxnRequest{
		transactional_id: t.transactional_id
		producer_id:      t.producer_id
		producer_epoch:   t.producer_epoch
		topics:           [
			kmsg.AddPartitionsToTxnRequestTopic{
				topic:      topic
				partitions: missing
			},
		]
	}
	// v4+ batches transactions per request; stay on the classic shape.
	// CONCURRENT_TRANSACTIONS is returned while the previous EndTxn's
	// markers are still being written; retry like other clients do.
	for _ in 0 .. 100 {
		body := c.request_broker_capped(t.coordinator, mut req, 3)!
		mut resp := kmsg.AddPartitionsToTxnResponse{
			version: req.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		mut retry := false
		for rt in resp.topics {
			for p in rt.partitions {
				match p.error_code {
					0 {
						t.added['${rt.topic}/${p.partition}'] = true
					}
					ec_concurrent_transactions, ec_coordinator_load, ec_coordinator_not_available {
						retry = true
					}
					else {
						e := kerr.error_for_code(p.error_code) or { kerr.unknown_server_error }
						return error('add partitions to txn ${rt.topic}[${p.partition}]: ${e.msg()}')
					}
				}
			}
		}
		if !retry {
			return
		}
		time.sleep(100 * time.millisecond)
	}
	return error('add partitions to txn: coordinator busy after retries')
}

// produce produces records inside the open transaction, with idempotent
// sequencing per partition.
pub fn (mut t TxnProducer) produce(topic string, mut records []Record) ! {
	if !t.in_txn {
		return error('no open transaction; call begin first')
	}
	if records.len == 0 {
		return
	}
	mut c := t.client
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
	}
	mut by_partition := map[int][]int{}
	for i, r in records {
		by_partition[r.partition] << i
	}
	mut partitions := by_partition.keys()
	partitions.sort()
	t.add_partitions(topic, partitions)!

	mut by_leader := map[int][]int{}
	for partition, _ in by_partition {
		leader := c.leader_for(topic, partition)!
		by_leader[leader] << partition
	}

	for leader, parts in by_leader {
		mut req := kmsg.ProduceRequest{
			transaction_id: t.transactional_id
			acks:           -1 // transactions require full acks
			timeout_millis: int(i64(c.cfg.produce_timeout) / 1000000)
		}
		mut req_topic := kmsg.ProduceRequestTopic{
			topic: topic
		}
		mut cap := i16(12)
		if id := c.topic_id(topic) {
			req_topic.topic_id = id
			cap = -1
		}
		for partition in parts {
			idxs := by_partition[partition]
			mut batch_records := []krec.Record{cap: idxs.len}
			for i in idxs {
				batch_records << records[i]
			}
			seq_key := '${topic}/${partition}'
			batch := krec.build_record_batch(batch_records, krec.BatchOpts{
				codec:          c.cfg.compression
				producer_id:    t.producer_id
				producer_epoch: t.producer_epoch
				base_sequence:  t.seqs[seq_key]
				transactional:  true
			})!
			req_topic.partitions << kmsg.ProduceRequestTopicPartition{
				partition: partition
				records:   batch
			}
		}
		req.topics = [req_topic]
		body := c.request_broker_capped(leader, mut req, cap)!
		mut resp := kmsg.ProduceResponse{
			version: req.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		mut first_err := ?ProduceError(none)
		for rt in resp.topics {
			for p in rt.partitions {
				idxs := by_partition[p.partition] or { continue }
				if p.error_code != 0 {
					if first_err == none {
						e := kerr.error_for_code(p.error_code) or { kerr.unknown_server_error }
						first_err = ProduceError{
							topic:     topic
							partition: p.partition
							code:      p.error_code
							detail:    e.msg()
						}
					}
					continue
				}
				for j, i in idxs {
					records[i].offset = p.base_offset + j
				}
				seq_key := '${topic}/${p.partition}'
				t.seqs[seq_key] = krec.next_sequence(t.seqs[seq_key], idxs.len)
			}
		}
		if e := first_err {
			return e
		}
	}
}

// send_offsets commits consumer-group offsets within the transaction (the
// exactly-once consume-transform-produce pattern): the offsets become
// visible to the group if and only if the transaction commits. Pass a
// GroupConsumer's positions().
pub fn (mut t TxnProducer) send_offsets(group string, offsets map[string]i64) ! {
	if !t.in_txn {
		return error('no open transaction')
	}
	if offsets.len == 0 {
		return
	}
	mut c := t.client
	mut areq := kmsg.AddOffsetsToTxnRequest{
		transactional_id: t.transactional_id
		producer_id:      t.producer_id
		producer_epoch:   t.producer_epoch
		group:            group
	}
	for _ in 0 .. 100 {
		abody := c.request_broker(t.coordinator, mut areq)!
		mut aresp := kmsg.AddOffsetsToTxnResponse{
			version: areq.version
		}
		mut ar := kbin.Reader{
			src: abody
		}
		aresp.read_from(mut ar)!
		if aresp.error_code == 0 {
			break
		}
		if aresp.error_code in [ec_concurrent_transactions, ec_coordinator_load,
			ec_coordinator_not_available] {
			time.sleep(100 * time.millisecond)
			continue
		}
		e := kerr.error_for_code(aresp.error_code) or { kerr.unknown_server_error }
		return error('add offsets to txn: ${e.msg()}')
	}

	// the group coordinator receives the staged offsets
	mut greq := kmsg.FindCoordinatorRequest{
		coordinator_key:  group
		coordinator_type: 0
	}
	gbody := c.request_capped(mut greq, 3)!
	mut gresp := kmsg.FindCoordinatorResponse{
		version: greq.version
	}
	mut gr := kbin.Reader{
		src: gbody
	}
	gresp.read_from(mut gr)!
	if gresp.error_code != 0 {
		return error('find group coordinator: error ${gresp.error_code}')
	}

	mut per_topic := map[string][]kmsg.TxnOffsetCommitRequestTopicPartition{}
	for key, off in offsets {
		idx := key.last_index('/') or { continue }
		per_topic[key[..idx]] << kmsg.TxnOffsetCommitRequestTopicPartition{
			partition: key[idx + 1..].int()
			offset:    off
		}
	}
	mut treq := kmsg.TxnOffsetCommitRequest{
		transactional_id: t.transactional_id
		group:            group
		producer_id:      t.producer_id
		producer_epoch:   t.producer_epoch
	}
	mut topics := per_topic.keys()
	topics.sort()
	for topic in topics {
		treq.topics << kmsg.TxnOffsetCommitRequestTopic{
			topic:      topic
			partitions: per_topic[topic]
		}
	}
	// v3+ adds group generation/member fencing (KIP-447); classic shape
	tbody := c.request_broker_capped(gresp.node_id, mut treq, 2)!
	mut tresp := kmsg.TxnOffsetCommitResponse{
		version: treq.version
	}
	mut tr := kbin.Reader{
		src: tbody
	}
	tresp.read_from(mut tr)!
	for rt in tresp.topics {
		for p in rt.partitions {
			if p.error_code != 0 {
				e := kerr.error_for_code(p.error_code) or { kerr.unknown_server_error }
				return error('txn offset commit ${rt.topic}[${p.partition}]: ${e.msg()}')
			}
		}
	}
}

fn (mut t TxnProducer) end(commit bool) ! {
	if !t.in_txn {
		return error('no open transaction')
	}
	mut c := t.client
	mut req := kmsg.EndTxnRequest{
		transactional_id: t.transactional_id
		producer_id:      t.producer_id
		producer_epoch:   t.producer_epoch
		commit:           commit
	}
	// v5+ (KIP-890) changes epoch semantics; stay on the classic shape
	for _ in 0 .. 100 {
		body := c.request_broker_capped(t.coordinator, mut req, 4)!
		mut resp := kmsg.EndTxnResponse{
			version: req.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		match resp.error_code {
			0 {
				t.in_txn = false
				t.added = map[string]bool{}
				return
			}
			ec_concurrent_transactions, ec_coordinator_load, ec_coordinator_not_available {
				time.sleep(100 * time.millisecond)
			}
			else {
				e := kerr.error_for_code(resp.error_code) or { kerr.unknown_server_error }
				return error('end txn: ${e.msg()}')
			}
		}
	}
	return error('end txn: coordinator busy after retries')
}

// commit commits the open transaction: its records become visible to
// read_committed consumers and its staged offsets apply to the group.
pub fn (mut t TxnProducer) commit() ! {
	t.end(true)!
}

// abort aborts the open transaction: its records are marked aborted and
// its staged offsets are discarded.
pub fn (mut t TxnProducer) abort() ! {
	t.end(false)!
}
