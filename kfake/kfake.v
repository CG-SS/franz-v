// Module kfake is an in-process fake Kafka cluster speaking real wire
// frames over loopback TCP, for hermetic client tests — the seed of the
// port plan's Phase 5. It serves ApiVersions, Metadata and Produce;
// produced record batches are fully verified (CRC-32C, decompression,
// record decode) and stored for assertions.
module kfake

import kbin
import krec
import kmsg
import net
import sync
import time

// ClusterCfg configures a fake cluster.
pub struct ClusterCfg {
pub mut:
	// advertised maps API key -> max version advertised via ApiVersions.
	advertised map[i16]i16 = {
		i16(0):  i16(13)
		i16(1):  i16(16)
		i16(2):  i16(7)
		i16(3):  i16(13)
		i16(8):  i16(10)
		i16(9):  i16(5)
		i16(10): i16(2)
		i16(11): i16(5)
		i16(12): i16(3)
		i16(13): i16(2)
		i16(14): i16(3)
		i16(15): i16(4)
		i16(16): i16(4)
		i16(18): i16(3)
		i16(19): i16(5)
		i16(20): i16(3)
		i16(21): i16(1)
		i16(22): i16(4)
		i16(24): i16(3)
		i16(25): i16(3)
		i16(26): i16(3)
		i16(28): i16(3)
		i16(32): i16(3)
		i16(37): i16(1)
		i16(42): i16(1)
		i16(44): i16(0)
	}
	// api_versions_max caps the ApiVersions request version accepted;
	// higher requests get a v0-encoded UNSUPPORTED_VERSION reply
	// (KIP-511 old-broker behavior).
	api_versions_max i16 = 3
	// partitions_per_topic controls Metadata answers; partition p's
	// leader is node p % nodes.
	partitions_per_topic int = 1
	// fail_first_conns: this many first connections (cluster-wide) are
	// accepted and instantly dropped, to exercise retries.
	fail_first_conns int
	// produce_error_code, when non-zero, fails every produced partition
	// with this Kafka error code.
	produce_error_code i16
	// fetch_delay is served before answering any fetch, to simulate
	// long-polling.
	fetch_delay time.Duration
	// kill_after_requests, when > 0, closes each connection after
	// serving this many requests on it (mid-stream failure injection).
	kill_after_requests int
	// pending_topic_metadata answers this many first Metadata requests
	// as if every requested topic were still being created: listed with
	// LEADER_NOT_AVAILABLE and no partitions, like a broker that has not
	// yet applied a just-created topic.
	pending_topic_metadata int
}

// Cluster is a running fake cluster.
@[heap]
pub struct Cluster {
pub:
	cfg   kmsg.ClusterCfg
	nodes int
pub mut:
	ports []int
mut:
	mu          &sync.Mutex = sync.new_mutex()
	fails       int
	conns       map[int]int                   // node -> connections accepted
	stored      map[string][]kmsg.StoredBatch // 'topic/partition' -> batches
	groups      map[string]&kmsg.FakeGroup
	next_member int
	txns        map[string]&kmsg.FakeTxn       // transactional_id -> state
	pid_epoch   map[i64]i16                    // producer id -> current epoch (fencing)
	seqs        map[string]int                 // 'pid/topic/partition' -> next sequence
	aborted     map[string][]kmsg.AbortedRange // 'topic/partition' -> aborted ranges
	open_txn    map[string]i64                 // 'topic/partition/pid' -> open txn first offset
	next_pid    i64 = 1000
	topics      map[string]int               // topic -> partition count (registry)
	tconfigs    map[string]map[string]string // topic -> configs
	log_start   map[string]i64               // 'topic/partition' -> log start offset
	topic_uuids map[string][16]u8
	uuid_names  map[string]string
	// pending counts down the Metadata answers still served pending.
	pending int
}

// start launches a fake cluster with the given node count.
pub fn start(nodes int, cfg ClusterCfg) &Cluster {
	mut cl := &kmsg.Cluster{
		cfg:     cfg
		nodes:   nodes
		fails:   cfg.fail_first_conns
		pending: cfg.pending_topic_metadata
	}
	mut listeners := []&net.TcpListener{}
	for _ in 0 .. nodes {
		mut l := net.listen_tcp(.ip, '127.0.0.1:0') or { panic('kfake listen: ${err}') }
		addr := l.addr() or { panic('kfake addr: ${err}') }
		cl.ports << '${addr}'.split(':')[1].int()
		listeners << l
	}
	for i, mut l in listeners {
		spawn cl.serve_node(i, mut l)
	}
	return cl
}

// seed_addr returns node 0's address for client bootstrap.
pub fn (cl &Cluster) seed_addr() string {
	return '127.0.0.1:${cl.ports[0]}'
}

// records returns a copy of every user record produced to
// topic/partition in offset order (control markers excluded, aborted
// records included).
pub fn (mut cl Cluster) records(topic string, partition int) []Record {
	cl.mu.lock()
	defer {
		cl.mu.unlock()
	}
	mut out := []kmsg.Record{}
	for b in cl.stored['${topic}/${partition}'] {
		if b.control {
			continue
		}
		out << b.records
	}
	return out
}

// ---------------------------------------------------------------------------
// Wire serving
// ---------------------------------------------------------------------------

fn write_all(mut conn net.TcpConn, buf []u8) ! {
	mut off := 0
	for off < buf.len {
		n := conn.write(buf[off..]) or { return error('write: ${err.msg()}') }
		if n <= 0 {
			return error('write: closed')
		}
		off += n
	}
}

fn read_full(mut conn net.TcpConn, mut buf []u8) ! {
	mut off := 0
	for off < buf.len {
		n := conn.read(mut buf[off..]) or { return error('read: ${err.msg()}') }
		if n <= 0 {
			return error('read: closed')
		}
		off += n
	}
}

fn (mut cl Cluster) serve_node(node_id int, mut l net.TcpListener) {
	for {
		mut conn := l.accept() or { return }
		cl.mu.lock()
		cl.conns[node_id]++
		fail := cl.fails > 0
		if fail {
			cl.fails--
		}
		cl.mu.unlock()
		if fail {
			conn.close() or {}
			continue
		}
		spawn cl.serve_conn(node_id, mut conn)
	}
}

// connections reports how many connections a node has accepted.
pub fn (mut cl Cluster) connections(node_id int) int {
	cl.mu.lock()
	defer {
		cl.mu.unlock()
	}
	return cl.conns[node_id]
}

fn (mut cl Cluster) serve_conn(node_id int, mut conn net.TcpConn) {
	mut served := 0
	for {
		mut size_buf := []u8{len: 4}
		read_full(mut conn, mut size_buf) or {
			conn.close() or {}
			return
		}
		mut sr := kmsg.Reader{
			src: size_buf
		}
		size := sr.read_int32()
		mut payload := []u8{len: size}
		read_full(mut conn, mut payload) or {
			conn.close() or {}
			return
		}

		mut r := kmsg.Reader{
			src: payload
		}
		key := r.read_int16()
		version := r.read_int16()
		corr := r.read_int32()
		r.read_nullable_string() or { '' }
		flexible := (key == 18 && version >= 3) || (key == 3 && version >= 9)
			|| (key == 0 && version >= 9) || (key == 1 && version >= 12)
			|| (key == 2 && version >= 6) || (key == 8 && version >= 8)
			|| (key == 22 && version >= 2) || (key == 24 && version >= 3)
			|| (key == 25 && version >= 3) || (key == 26 && version >= 3)
			|| (key == 28 && version >= 3) || (key == 16 && version >= 3)
			|| (key == 19 && version >= 5)
		if flexible {
			num_tags := r.read_uvarint()
			for _ in 0 .. num_tags {
				r.read_uvarint()
				sz := int(r.read_uvarint())
				r.span(sz)
			}
		}
		body := payload[r.off..].clone()

		if key == 1 && i64(cl.cfg.fetch_delay) > 0 {
			time.sleep(cl.cfg.fetch_delay)
		}
		resp_body := match key {
			18 { cl.answer_api_versions(body, version) }
			3 { cl.answer_metadata(node_id, body, version) }
			0 { cl.answer_produce(body, version) }
			1 { cl.answer_fetch(body, version) }
			2 { cl.answer_list_offsets(body, version) }
			8 { cl.answer_offset_commit(body, version) }
			9 { cl.answer_offset_fetch(body, version) }
			10 { cl.answer_find_coordinator(node_id, body, version) }
			11 { cl.answer_join_group(body, version) }
			12 { cl.answer_heartbeat(body, version) }
			13 { cl.answer_leave_group(body, version) }
			14 { cl.answer_sync_group(body, version) }
			22 { cl.answer_init_producer_id(body, version) }
			24 { cl.answer_add_partitions_to_txn(body, version) }
			25 { cl.answer_add_offsets_to_txn(body, version) }
			26 { cl.answer_end_txn(body, version) }
			28 { cl.answer_txn_offset_commit(body, version) }
			15 { cl.answer_describe_groups(body, version) }
			16 { cl.answer_list_groups(body, version) }
			19 { cl.answer_create_topics(body, version) }
			20 { cl.answer_delete_topics(body, version) }
			21 { cl.answer_delete_records(body, version) }
			32 { cl.answer_describe_configs(body, version) }
			37 { cl.answer_create_partitions(body, version) }
			42 { cl.answer_delete_groups(body, version) }
			44 { cl.answer_incremental_alter_configs(body, version) }
			else { []u8{} }
		}

		mut hdr := kmsg.Writer{}
		hdr.write_int32(corr)
		if flexible && key != 18 {
			hdr.write_uvarint(0)
		}
		mut framed := kmsg.Writer{}
		framed.write_int32(hdr.buf.len + resp_body.len)
		framed.buf << hdr.buf
		framed.buf << resp_body
		write_all(mut conn, framed.buf) or {
			conn.close() or {}
			return
		}
		served++
		if cl.cfg.kill_after_requests > 0 && served >= cl.cfg.kill_after_requests {
			conn.close() or {}
			return
		}
	}
}

fn (cl &Cluster) api_keys() []ApiVersionsResponseApiKey {
	mut keys := []kmsg.ApiVersionsResponseApiKey{}
	for k, v in cl.cfg.advertised {
		keys << kmsg.ApiVersionsResponseApiKey{
			api_key:     k
			max_version: v
		}
	}
	keys.sort(a.api_key < b.api_key)
	return keys
}

fn (mut cl Cluster) answer_api_versions(body []u8, version i16) []u8 {
	_ = body
	if version > cl.cfg.api_versions_max {
		mut resp := kmsg.ApiVersionsResponse{
			version:    0
			error_code: 35
			api_keys:   cl.api_keys()
		}
		mut w := kmsg.Writer{}
		resp.write_to(mut w)
		return w.buf
	}
	mut resp := kmsg.ApiVersionsResponse{
		version:  version
		api_keys: cl.api_keys()
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_metadata(node_id int, body []u8, version i16) []u8 {
	mut req := kmsg.MetadataRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut req_topics := (req.topics or {
		// null topics = all known topics
		cl.mu.lock()
		mut names := cl.topics.keys()
		names.sort()
		cl.mu.unlock()
		names.map(kmsg.MetadataRequestTopic{
			topic: it
		})
	}).clone()

	mut resp := kmsg.MetadataResponse{
		version:       version
		cluster_id:    'kfake'
		controller_id: node_id
	}
	cl.mu.lock()
	pending := cl.pending > 0
	if pending {
		cl.pending--
	}
	cl.mu.unlock()
	for t in req_topics {
		tname0 := t.topic or { '' }
		cl.mu.lock()
		tid := cl.uuid_for(tname0)
		if tname0 !in cl.topics {
			cl.topics[tname0] = cl.cfg.partitions_per_topic // auto-create
		}
		nparts := cl.topics[tname0]
		cl.mu.unlock()
		if pending {
			resp.topics << kmsg.MetadataResponseTopic{
				error_code: 5 // LEADER_NOT_AVAILABLE
				topic:      t.topic
			}
			continue
		}
		mut partitions := []kmsg.MetadataResponseTopicPartition{}
		for p in 0 .. nparts {
			partitions << kmsg.MetadataResponseTopicPartition{
				partition: p
				leader:    p % cl.nodes
				replicas:  [p % cl.nodes]
				isr:       [p % cl.nodes]
			}
		}
		resp.topics << kmsg.MetadataResponseTopic{
			topic:      t.topic
			topic_id:   tid
			partitions: partitions
		}
	}
	for i, port in cl.ports {
		resp.brokers << kmsg.MetadataResponseBroker{
			node_id: i
			host:    '127.0.0.1'
			port:    port
		}
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// answer_produce fully validates each produced batch (CRC, decompression,
// record decode) before storing; transactional batches additionally check
// producer epoch (fencing) and sequence continuity.
fn (mut cl Cluster) answer_produce(body []u8, version i16) []u8 {
	mut req := kmsg.ProduceRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.ProduceResponse{
		version: version
	}
	for t in req.topics {
		cl.mu.lock()
		topic := cl.resolve_topic(t.topic, t.topic_id) or {
			cl.mu.unlock()
			continue
		}
		tid := cl.uuid_for(topic)
		cl.mu.unlock()
		mut resp_topic := kmsg.ProduceResponseTopic{
			topic:    topic
			topic_id: tid
		}
		for p in t.partitions {
			mut code := cl.cfg.produce_error_code
			mut base_offset := i64(-1)
			if code == 0 {
				batch_bytes := p.records or { []u8{} }
				meta, mut recs := kmsg.parse_record_batch(batch_bytes) or {
					resp_topic.partitions << kmsg.ProduceResponseTopicPartition{
						partition:  p.partition
						error_code: 2 // CORRUPT_MESSAGE
					}
					continue
				}
				skey := '${topic}/${p.partition}'
				cl.mu.lock()
				if meta.producer_id >= 0 {
					cur := cl.pid_epoch[meta.producer_id] or { i16(-1) }
					if meta.producer_epoch < cur {
						code = 90 // PRODUCER_FENCED
					} else if meta.first_sequence >= 0 {
						qkey := '${meta.producer_id}/${skey}'
						expected := cl.seqs[qkey] or { 0 }
						if meta.first_sequence != expected {
							code = 45 // OUT_OF_ORDER_SEQUENCE_NUMBER
						} else {
							cl.seqs[qkey] = expected + recs.len
						}
					}
				}
				if code == 0 {
					base_offset = partition_high(cl.stored[skey])
					for mut rec in recs {
						rec.topic = topic
						rec.partition = p.partition
						rec.offset += base_offset
					}
					if kmsg.is_transactional(meta) {
						okey := '${skey}/${meta.producer_id}'
						if okey !in cl.open_txn {
							cl.open_txn[okey] = base_offset
						}
					}
					cl.stored[skey] << kmsg.StoredBatch{
						base_offset:   base_offset
						records:       recs
						pid:           meta.producer_id
						epoch:         meta.producer_epoch
						base_sequence: meta.first_sequence
						transactional: kmsg.is_transactional(meta)
					}
				}
				cl.mu.unlock()
			}
			resp_topic.partitions << kmsg.ProduceResponseTopicPartition{
				partition:   p.partition
				error_code:  code
				base_offset: base_offset
			}
		}
		resp.topics << resp_topic
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// answer_fetch re-serves stored batches (including control markers) from
// the requested offset, with the partition's aborted-transaction ranges
// for client-side read_committed filtering.
fn (mut cl Cluster) answer_fetch(body []u8, version i16) []u8 {
	mut req := kmsg.FetchRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.FetchResponse{
		version: version
	}
	for t in req.topics {
		cl.mu.lock()
		tname := cl.resolve_topic(t.topic, t.topic_id) or {
			cl.mu.unlock()
			continue
		}
		ftid := cl.uuid_for(tname)
		cl.mu.unlock()
		mut resp_topic := kmsg.FetchResponseTopic{
			topic:    tname
			topic_id: ftid
		}
		for p in t.partitions {
			skey := '${tname}/${p.partition}'
			cl.mu.lock()
			batches := cl.stored[skey].clone()
			aborted_ranges := cl.aborted[skey].clone()
			cl.mu.unlock()
			high := partition_high(batches)
			cl.mu.lock()
			lstart := cl.log_start[skey] or { i64(0) }
			cl.mu.unlock()
			from := p.fetch_offset
			if from < lstart || from > high {
				resp_topic.partitions << kmsg.FetchResponseTopicPartition{
					partition:      p.partition
					error_code:     1 // OFFSET_OUT_OF_RANGE
					high_watermark: high
				}
				continue
			}
			mut payload := []u8{}
			for b in batches {
				if b.end_offset() <= from || b.end_offset() <= lstart {
					continue
				}
				rebuilt := if b.control {
					kmsg.control_marker_batch(b.base_offset, b.pid, b.epoch, b.commit, 0) or {
						continue
					}
				} else {
					kmsg.build_record_batch(b.records, kmsg.BatchOpts{
						base_offset:    b.base_offset
						producer_id:    b.pid
						producer_epoch: b.epoch
						base_sequence:  b.base_sequence
						transactional:  b.transactional
					}) or { continue }
				}
				payload << rebuilt
			}
			mut aborted := []kmsg.FetchResponseTopicPartitionAbortedTransaction{}
			for ar in aborted_ranges {
				// only ranges whose closing marker falls inside the
				// served window; fully-consumed aborts are irrelevant
				if ar.marker_offset < from {
					continue
				}
				aborted << kmsg.FetchResponseTopicPartitionAbortedTransaction{
					producer_id:  ar.pid
					first_offset: ar.first_offset
				}
			}
			resp_topic.partitions << kmsg.FetchResponseTopicPartition{
				partition:            p.partition
				high_watermark:       high
				last_stable_offset:   high
				record_batches:       payload
				aborted_transactions: aborted
			}
		}
		resp.topics << resp_topic
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// answer_list_offsets resolves earliest (-2) to 0 and latest (-1) to the
// stored record count.
fn (mut cl Cluster) answer_list_offsets(body []u8, version i16) []u8 {
	mut req := kmsg.ListOffsetsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.ListOffsetsResponse{
		version: version
	}
	for t in req.topics {
		mut resp_topic := kmsg.ListOffsetsResponseTopic{
			topic: t.topic
		}
		for p in t.partitions {
			cl.mu.lock()
			high := partition_high(cl.stored['${t.topic}/${p.partition}'])
			lstart := cl.log_start['${t.topic}/${p.partition}'] or { i64(0) }
			cl.mu.unlock()
			off := if p.timestamp == -2 { lstart } else { high }
			resp_topic.partitions << kmsg.ListOffsetsResponseTopicPartition{
				partition: p.partition
				offset:    off
				timestamp: -1
			}
		}
		resp.topics << resp_topic
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// ---------------------------------------------------------------------------
// Group coordinator: enough of the classic protocol for hermetic group
// tests. Generations bump when membership changes; members whose join
// generation lags get REBALANCE_IN_PROGRESS on heartbeat until they
// rejoin; SyncGroup followers wait for the leader's assignments.
// ---------------------------------------------------------------------------

struct FakeMember {
mut:
	metadata  []u8 // protocol metadata of the first offered protocol
	protocols []string
}

@[heap]
struct FakeGroup {
mut:
	generation      int
	members         map[string]kmsg.FakeMember
	member_gen      map[string]int
	protocol        string
	assignments     map[string][]u8
	assignments_gen int
	offsets         map[string]i64 // 'topic/partition' -> committed
}

// StoredBatch is one appended record batch with its producer metadata,
// so fetch can re-serve faithfully and EndTxn can write markers.
struct StoredBatch {
mut:
	base_offset   i64
	records       []kmsg.Record // empty for control batches
	pid           i64 = -1
	epoch         i16 = -1
	base_sequence int = -1
	transactional bool
	control       bool
	commit        bool // marker type for control batches
}

fn (b &StoredBatch) end_offset() i64 {
	if b.control {
		return b.base_offset + 1
	}
	return b.base_offset + b.records.len
}

// AbortedRange is one aborted transaction's extent in a partition; the
// closing marker's offset bounds it, so fetches can include only ranges
// overlapping the served window (as real brokers do).
struct AbortedRange {
	pid           i64
	first_offset  i64
	marker_offset i64
}

// FakeTxn is per-transactional-id coordinator state.
@[heap]
struct FakeTxn {
mut:
	pid        i64
	epoch      i16
	partitions map[string]bool           // 'topic/partition' touched this txn
	staged     map[string]map[string]i64 // group -> 'topic/partition' -> offset
}

fn partition_high(batches []StoredBatch) i64 {
	if batches.len == 0 {
		return 0
	}
	return batches[batches.len - 1].end_offset()
}

// uuid_for assigns a deterministic non-zero uuid per topic name.
fn (mut cl Cluster) uuid_for(topic string) [16]u8 {
	if topic in cl.topic_uuids {
		return cl.topic_uuids[topic]
	}
	mut id := [16]u8{}
	id[0] = u8(topic.len + 1)
	for i, ch in topic.bytes() {
		id[1 + (i % 15)] ^= ch + u8(i)
	}
	cl.topic_uuids[topic] = id
	cl.uuid_names[id[..].hex()] = topic
	return id
}

fn (mut cl Cluster) topic_from(id [16]u8) ?string {
	return cl.uuid_names[id[..].hex()] or { return none }
}

// resolve_topic returns the name for a request topic addressed by name or
// by uuid.
fn (mut cl Cluster) resolve_topic(name string, id [16]u8) ?string {
	if name != '' {
		return name
	}
	return cl.topic_from(id)
}

fn (mut cl Cluster) group(name string) &FakeGroup {
	if name !in cl.groups {
		cl.groups[name] = &kmsg.FakeGroup{}
	}
	return cl.groups[name]
}

fn leader_of(g &FakeGroup) string {
	mut ids := g.members.keys()
	ids.sort()
	if ids.len == 0 {
		return ''
	}
	return ids[0]
}

fn (mut cl Cluster) answer_find_coordinator(node_id int, body []u8, version i16) []u8 {
	mut req := kmsg.FindCoordinatorRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.FindCoordinatorResponse{
		version: version
		node_id: node_id
		host:    '127.0.0.1'
		port:    cl.ports[node_id]
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_join_group(body []u8, version i16) []u8 {
	mut req := kmsg.JoinGroupRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.JoinGroupResponse{
		version:    version
		generation: -1
	}
	cl.mu.lock()
	mut g := cl.group(req.group)
	if req.member_id == '' {
		cl.next_member++
		resp.error_code = 79 // MEMBER_ID_REQUIRED
		resp.member_id = 'kfake-member-${cl.next_member}'
		cl.mu.unlock()
		mut w0 := kmsg.Writer{}
		resp.write_to(mut w0)
		return w0.buf
	}

	mut protos := []string{}
	for p in req.protocols {
		protos << p.name
	}
	meta := if req.protocols.len > 0 { req.protocols[0].metadata.clone() } else { []u8{} }
	if req.member_id !in g.members {
		g.generation++ // membership change: everyone else must rejoin
	} else if g.assignments_gen == g.generation {
		// a member rejoining a stable group (e.g. cooperative second
		// round) starts a fresh rebalance
		g.generation++
	}
	g.members[req.member_id] = kmsg.FakeMember{
		metadata:  meta
		protocols: protos
	}
	g.member_gen[req.member_id] = g.generation
	if g.protocol == '' && protos.len > 0 {
		g.protocol = protos[0]
	}
	leader := leader_of(g)
	resp.error_code = 0
	resp.generation = g.generation
	resp.member_id = req.member_id
	resp.leader_id = leader
	resp.protocol = g.protocol
	if req.member_id == leader {
		mut ids := g.members.keys()
		ids.sort()
		for id in ids {
			resp.members << kmsg.JoinGroupResponseMember{
				member_id:         id
				protocol_metadata: g.members[id].metadata.clone()
			}
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_sync_group(body []u8, version i16) []u8 {
	mut req := kmsg.SyncGroupRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.SyncGroupResponse{
		version: version
	}
	cl.mu.lock()
	mut g := cl.group(req.group)
	if req.generation != g.generation {
		resp.error_code = 22 // ILLEGAL_GENERATION
		cl.mu.unlock()
		mut we := kmsg.Writer{}
		resp.write_to(mut we)
		return we.buf
	}
	if req.member_id == leader_of(g) && req.group_assignment.len > 0 {
		g.assignments = map[string][]u8{}
		for a in req.group_assignment {
			g.assignments[a.member_id] = a.member_assignment.clone()
		}
		g.assignments_gen = g.generation
	}
	gen := g.generation
	cl.mu.unlock()

	// wait for the leader's assignments of this generation
	mut waited := 0
	for {
		cl.mu.lock()
		mut g2 := cl.group(req.group)
		if g2.assignments_gen == gen && g2.generation == gen {
			resp.member_assignment = g2.assignments[req.member_id] or { []u8{} }.clone()
			cl.mu.unlock()
			break
		}
		if g2.generation != gen {
			resp.error_code = 27 // REBALANCE_IN_PROGRESS
			cl.mu.unlock()
			break
		}
		cl.mu.unlock()
		waited += 5
		if waited > 3000 {
			resp.error_code = 27
			break
		}
		time.sleep(5 * time.millisecond)
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_heartbeat(body []u8, version i16) []u8 {
	mut req := kmsg.HeartbeatRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.HeartbeatResponse{
		version: version
	}
	cl.mu.lock()
	mut g := cl.group(req.group)
	if req.member_id !in g.members {
		resp.error_code = 25 // UNKNOWN_MEMBER_ID
	} else if g.member_gen[req.member_id] < g.generation {
		resp.error_code = 27 // REBALANCE_IN_PROGRESS
	} else if req.generation != g.generation {
		resp.error_code = 22 // ILLEGAL_GENERATION
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_leave_group(body []u8, version i16) []u8 {
	mut req := kmsg.LeaveGroupRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	cl.mu.lock()
	mut g := cl.group(req.group)
	if req.member_id in g.members {
		g.members.delete(req.member_id)
		g.member_gen.delete(req.member_id)
		if g.members.len > 0 {
			g.generation++ // survivors must rejoin
		}
	}
	cl.mu.unlock()
	mut resp := kmsg.LeaveGroupResponse{
		version: version
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_offset_commit(body []u8, version i16) []u8 {
	mut req := kmsg.OffsetCommitRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.OffsetCommitResponse{
		version: version
	}
	cl.mu.lock()
	mut g := cl.group(req.group)
	stale := req.generation != g.generation
	for t in req.topics {
		// already under cl.mu here (locked before the topic loop)
		ctname := cl.resolve_topic(t.topic, t.topic_id) or { continue }
		ctid := cl.uuid_for(ctname)
		mut rt := kmsg.OffsetCommitResponseTopic{
			topic:    ctname
			topic_id: ctid
		}
		for p in t.partitions {
			mut code := i16(0)
			if stale {
				code = 22 // ILLEGAL_GENERATION
			} else {
				g.offsets['${ctname}/${p.partition}'] = p.offset
			}
			rt.partitions << kmsg.OffsetCommitResponseTopicPartition{
				partition:  p.partition
				error_code: code
			}
		}
		resp.topics << rt
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_offset_fetch(body []u8, version i16) []u8 {
	mut req := kmsg.OffsetFetchRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.OffsetFetchResponse{
		version: version
	}
	cl.mu.lock()
	mut g := cl.group(req.group)
	req_topics := req.topics or {
		// null topics = every partition the group has offsets for
		mut per_topic := map[string][]int{}
		mut okeys := g.offsets.keys()
		okeys.sort()
		for key in okeys {
			idx := key.last_index('/') or { continue }
			per_topic[key[..idx]] << key[idx + 1..].int()
		}
		mut all := []kmsg.OffsetFetchRequestTopic{}
		mut tnames := per_topic.keys()
		tnames.sort()
		for tname in tnames {
			all << kmsg.OffsetFetchRequestTopic{
				topic:      tname
				partitions: per_topic[tname]
			}
		}
		all
	}

	for t in req_topics {
		mut rt := kmsg.OffsetFetchResponseTopic{
			topic: t.topic
		}
		for p in t.partitions {
			off := g.offsets['${t.topic}/${p}'] or { i64(-1) }
			rt.partitions << kmsg.OffsetFetchResponseTopicPartition{
				partition: p
				offset:    off
			}
		}
		resp.topics << rt
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// ---------------------------------------------------------------------------
// Transaction coordinator: InitProducerId with epoch bumps (fencing),
// AddPartitionsToTxn, staged TxnOffsetCommit applied on commit, and
// EndTxn writing control markers plus aborted-range bookkeeping.
// ---------------------------------------------------------------------------

fn (mut cl Cluster) txn(tid string) &FakeTxn {
	if tid !in cl.txns {
		cl.txns[tid] = &kmsg.FakeTxn{
			pid:   cl.next_pid
			epoch: -1
		}
		cl.next_pid++
	}
	return cl.txns[tid]
}

fn (mut cl Cluster) answer_init_producer_id(body []u8, version i16) []u8 {
	mut req := kmsg.InitProducerIDRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	tid := req.transactional_id or { '' }

	mut resp := kmsg.InitProducerIDResponse{
		version: version
	}
	cl.mu.lock()
	if tid == '' {
		// plain idempotent producer: fresh pid, epoch 0
		resp.producer_id = cl.next_pid
		cl.next_pid++
		cl.pid_epoch[resp.producer_id] = 0
	} else {
		mut t := cl.txn(tid)
		t.epoch++ // re-init bumps the epoch: zombie fencing
		t.partitions = map[string]bool{}
		t.staged = map[string]map[string]i64{}
		cl.pid_epoch[t.pid] = t.epoch
		resp.producer_id = t.pid
		resp.producer_epoch = t.epoch
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) txn_check(tid string, pid i64, epoch i16) i16 {
	t := cl.txns[tid] or { return 90 }
	if t.pid != pid {
		return 90
	}
	if epoch < t.epoch {
		return 90 // PRODUCER_FENCED
	}
	return 0
}

fn (mut cl Cluster) answer_add_partitions_to_txn(body []u8, version i16) []u8 {
	mut req := kmsg.AddPartitionsToTxnRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.AddPartitionsToTxnResponse{
		version: version
	}
	cl.mu.lock()
	code := cl.txn_check(req.transactional_id, req.producer_id, req.producer_epoch)
	mut t := cl.txn(req.transactional_id)
	for rt in req.topics {
		mut resp_topic := kmsg.AddPartitionsToTxnResponseTopic{
			topic: rt.topic
		}
		for p in rt.partitions {
			if code == 0 {
				t.partitions['${rt.topic}/${p}'] = true
			}
			resp_topic.partitions << kmsg.AddPartitionsToTxnResponseTopicPartition{
				partition:  p
				error_code: code
			}
		}
		resp.topics << resp_topic
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_add_offsets_to_txn(body []u8, version i16) []u8 {
	mut req := kmsg.AddOffsetsToTxnRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	cl.mu.lock()
	code := cl.txn_check(req.transactional_id, req.producer_id, req.producer_epoch)
	cl.mu.unlock()
	mut resp := kmsg.AddOffsetsToTxnResponse{
		version:    version
		error_code: code
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_txn_offset_commit(body []u8, version i16) []u8 {
	mut req := kmsg.TxnOffsetCommitRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	mut resp := kmsg.TxnOffsetCommitResponse{
		version: version
	}
	cl.mu.lock()
	code := cl.txn_check(req.transactional_id, req.producer_id, req.producer_epoch)
	mut t := cl.txn(req.transactional_id)
	if req.group !in t.staged {
		t.staged[req.group] = map[string]i64{}
	}
	for rt in req.topics {
		mut resp_topic := kmsg.TxnOffsetCommitResponseTopic{
			topic: rt.topic
		}
		for p in rt.partitions {
			if code == 0 {
				t.staged[req.group]['${rt.topic}/${p.partition}'] = p.offset
			}
			resp_topic.partitions << kmsg.TxnOffsetCommitResponseTopicPartition{
				partition:  p.partition
				error_code: code
			}
		}
		resp.topics << resp_topic
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_end_txn(body []u8, version i16) []u8 {
	mut req := kmsg.EndTxnRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }

	cl.mu.lock()
	code := cl.txn_check(req.transactional_id, req.producer_id, req.producer_epoch)
	if code == 0 {
		mut t := cl.txn(req.transactional_id)
		// write a control marker into every partition the txn touched
		mut pkeys := t.partitions.keys()
		pkeys.sort()
		for pkey in pkeys {
			base := partition_high(cl.stored[pkey])
			cl.stored[pkey] << kmsg.StoredBatch{
				base_offset: base
				pid:         t.pid
				epoch:       t.epoch
				control:     true
				commit:      req.commit
			}
			okey := '${pkey}/${t.pid}'
			if first := cl.open_txn[okey] {
				if !req.commit {
					cl.aborted[pkey] << kmsg.AbortedRange{
						pid:           t.pid
						first_offset:  first
						marker_offset: base
					}
				}
				cl.open_txn.delete(okey)
			}
		}
		if req.commit {
			// staged group offsets become visible
			for group, offsets in t.staged {
				mut g := cl.group(group)
				for key, off in offsets {
					g.offsets[key] = off
				}
			}
		}
		t.partitions = map[string]bool{}
		t.staged = map[string]map[string]i64{}
	}
	cl.mu.unlock()
	mut resp := kmsg.EndTxnResponse{
		version:    version
		error_code: code
	}
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

// ---------------------------------------------------------------------------
// Admin APIs: topic lifecycle, configs, group listing/description/deletion,
// record deletion.
// ---------------------------------------------------------------------------

fn (mut cl Cluster) answer_create_topics(body []u8, version i16) []u8 {
	mut req := kmsg.CreateTopicsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.CreateTopicsResponse{
		version: version
	}
	cl.mu.lock()
	for t in req.topics {
		mut code := i16(0)
		if t.topic in cl.topics {
			code = 36 // TOPIC_ALREADY_EXISTS
		} else {
			nparts := if t.num_partitions > 0 { t.num_partitions } else { 1 }
			cl.topics[t.topic] = nparts
			cl.uuid_for(t.topic)
			mut cfgs := map[string]string{}
			for c in t.configs {
				cfgs[c.name] = c.value or { '' }
			}
			cl.tconfigs[t.topic] = cfgs.move()
		}
		resp.topics << kmsg.CreateTopicsResponseTopic{
			topic:      t.topic
			error_code: code
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_delete_topics(body []u8, version i16) []u8 {
	mut req := kmsg.DeleteTopicsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.DeleteTopicsResponse{
		version: version
	}
	cl.mu.lock()
	for name in req.topic_names {
		mut code := i16(0)
		if name !in cl.topics {
			code = 3 // UNKNOWN_TOPIC_OR_PARTITION
		} else {
			nparts := cl.topics[name]
			cl.topics.delete(name)
			cl.tconfigs.delete(name)
			for p in 0 .. nparts {
				cl.stored.delete('${name}/${p}')
				cl.log_start.delete('${name}/${p}')
				cl.aborted.delete('${name}/${p}')
			}
		}
		resp.topics << kmsg.DeleteTopicsResponseTopic{
			topic:      name
			error_code: code
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_create_partitions(body []u8, version i16) []u8 {
	mut req := kmsg.CreatePartitionsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.CreatePartitionsResponse{
		version: version
	}
	cl.mu.lock()
	for t in req.topics {
		mut code := i16(0)
		if t.topic !in cl.topics {
			code = 3
		} else if t.count <= cl.topics[t.topic] {
			code = 37 // INVALID_PARTITIONS
		} else {
			cl.topics[t.topic] = t.count
		}
		resp.topics << kmsg.CreatePartitionsResponseTopic{
			topic:      t.topic
			error_code: code
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_describe_configs(body []u8, version i16) []u8 {
	mut req := kmsg.DescribeConfigsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.DescribeConfigsResponse{
		version: version
	}
	cl.mu.lock()
	for res in req.resources {
		mut rr := kmsg.DescribeConfigsResponseResource{
			resource_type: res.resource_type
			resource_name: res.resource_name
		}
		if res.resource_type == 2 { // topic
			if res.resource_name !in cl.topics {
				rr.error_code = 3
			} else {
				// one default entry every topic has, plus explicit ones
				rr.configs << kmsg.DescribeConfigsResponseResourceConfig{
					name:       'cleanup.policy'
					value:      cl.tconfigs[res.resource_name]['cleanup.policy'] or { 'delete' }
					is_default: 'cleanup.policy' !in cl.tconfigs[res.resource_name]
				}
				mut names := cl.tconfigs[res.resource_name].keys()
				names.sort()
				for name in names {
					if name == 'cleanup.policy' {
						continue
					}
					rr.configs << kmsg.DescribeConfigsResponseResourceConfig{
						name:  name
						value: cl.tconfigs[res.resource_name][name]
					}
				}
			}
		}
		resp.resources << rr
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_incremental_alter_configs(body []u8, version i16) []u8 {
	mut req := kmsg.IncrementalAlterConfigsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.IncrementalAlterConfigsResponse{
		version: version
	}
	cl.mu.lock()
	for res in req.resources {
		mut code := i16(0)
		if res.resource_type != 2 || res.resource_name !in cl.topics {
			code = 3
		} else {
			if res.resource_name !in cl.tconfigs {
				cl.tconfigs[res.resource_name] = map[string]string{}
			}
			for c in res.configs {
				match i8(c.op) {
					0 { cl.tconfigs[res.resource_name][c.name] = c.value or { '' } }
					1 { cl.tconfigs[res.resource_name].delete(c.name) }
					else { code = 44 } // INVALID_CONFIG
				}
			}
		}
		resp.resources << kmsg.IncrementalAlterConfigsResponseResource{
			resource_type: res.resource_type
			resource_name: res.resource_name
			error_code:    code
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_list_groups(body []u8, version i16) []u8 {
	mut resp := kmsg.ListGroupsResponse{
		version: version
	}
	cl.mu.lock()
	mut names := cl.groups.keys()
	names.sort()
	for name in names {
		g := cl.groups[name] or { continue }
		state := if g.members.len == 0 { 'Empty' } else { 'Stable' }
		resp.groups << kmsg.ListGroupsResponseGroup{
			group:         name
			protocol_type: 'consumer'
			group_state:   state
		}
	}
	cl.mu.unlock()
	_ = body
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_describe_groups(body []u8, version i16) []u8 {
	mut req := kmsg.DescribeGroupsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.DescribeGroupsResponse{
		version: version
	}
	cl.mu.lock()
	for name in req.groups {
		mut rg := kmsg.DescribeGroupsResponseGroup{
			group:         name
			protocol_type: 'consumer'
		}
		if name !in cl.groups {
			rg.state = 'Dead'
		} else {
			g := cl.groups[name] or { continue }
			rg.state = if g.members.len == 0 { 'Empty' } else { 'Stable' }
			rg.protocol = g.protocol
			mut ids := g.members.keys()
			ids.sort()
			for id in ids {
				rg.members << kmsg.DescribeGroupsResponseGroupMember{
					member_id:         id
					client_id:         'franz-v'
					client_host:       '/127.0.0.1'
					protocol_metadata: g.members[id].metadata.clone()
					member_assignment: g.assignments[id] or { []u8{} }.clone()
				}
			}
		}
		resp.groups << rg
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_delete_groups(body []u8, version i16) []u8 {
	mut req := kmsg.DeleteGroupsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.DeleteGroupsResponse{
		version: version
	}
	cl.mu.lock()
	for name in req.groups {
		mut code := i16(0)
		if name !in cl.groups {
			code = 69 // GROUP_ID_NOT_FOUND
		} else {
			g := cl.groups[name] or { continue }
			if g.members.len > 0 {
				code = 68 // NON_EMPTY_GROUP
			} else {
				cl.groups.delete(name)
			}
		}
		resp.groups << kmsg.DeleteGroupsResponseGroup{
			group:      name
			error_code: code
		}
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_delete_records(body []u8, version i16) []u8 {
	mut req := kmsg.DeleteRecordsRequest{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.DeleteRecordsResponse{
		version: version
	}
	cl.mu.lock()
	for t in req.topics {
		mut rt := kmsg.DeleteRecordsResponseTopic{
			topic: t.topic
		}
		for p in t.partitions {
			skey := '${t.topic}/${p.partition}'
			high := partition_high(cl.stored[skey])
			mut code := i16(0)
			mut low := i64(-1)
			if p.offset < 0 || p.offset > high {
				code = 1 // OFFSET_OUT_OF_RANGE
			} else {
				cur := cl.log_start[skey] or { i64(0) }
				if p.offset > cur {
					cl.log_start[skey] = p.offset
				}
				low = cl.log_start[skey] or { i64(0) }
			}
			rt.partitions << kmsg.DeleteRecordsResponseTopicPartition{
				partition:     p.partition
				low_watermark: low
				error_code:    code
			}
		}
		resp.topics << rt
	}
	cl.mu.unlock()
	mut w := kmsg.Writer{}
	resp.write_to(mut w)
	return w.buf
}
