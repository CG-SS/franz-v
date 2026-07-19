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
		i16(18): i16(3)
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
	conns       map[int]int              // node -> connections accepted
	stored      map[string][]kmsg.Record // 'topic/partition' -> records
	groups      map[string]&kmsg.FakeGroup
	next_member int
	topic_uuids map[string][16]u8
	uuid_names  map[string]string
}

// start launches a fake cluster with the given node count.
pub fn start(nodes int, cfg ClusterCfg) &Cluster {
	mut cl := &kmsg.Cluster{
		cfg:   cfg
		nodes: nodes
		fails: cfg.fail_first_conns
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

// records returns a copy of everything produced to topic/partition, in
// offset order.
pub fn (mut cl Cluster) records(topic string, partition int) []Record {
	cl.mu.lock()
	defer {
		cl.mu.unlock()
	}
	return cl.stored['${topic}/${partition}'].clone()
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
	req_topics := req.topics or { []kmsg.MetadataRequestTopic{} }

	mut resp := kmsg.MetadataResponse{
		version:       version
		cluster_id:    'kfake'
		controller_id: node_id
	}
	for t in req_topics {
		tname0 := t.topic or { '' }
		cl.mu.lock()
		tid := cl.uuid_for(tname0)
		cl.mu.unlock()
		mut partitions := []kmsg.MetadataResponseTopicPartition{}
		for p in 0 .. cl.cfg.partitions_per_topic {
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
// record decode) before storing; a batch that fails validation answers
// with CORRUPT_MESSAGE (2) so client tests catch encoder bugs loudly.
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
				_, mut recs := kmsg.parse_record_batch(batch_bytes) or {
					resp_topic.partitions << kmsg.ProduceResponseTopicPartition{
						partition:  p.partition
						error_code: 2 // CORRUPT_MESSAGE
					}
					continue
				}
				skey := '${topic}/${p.partition}'
				cl.mu.lock()
				base_offset = cl.stored[skey].len
				for mut rec in recs {
					rec.topic = topic
					rec.partition = p.partition
					rec.offset += base_offset
					cl.stored[skey] << rec
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

// answer_fetch re-serves stored records from the requested offset as a
// freshly built record batch.
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
		tid := cl.uuid_for(tname)
		cl.mu.unlock()
		mut resp_topic := kmsg.FetchResponseTopic{
			topic:    tname
			topic_id: tid
		}
		for p in t.partitions {
			skey := '${tname}/${p.partition}'
			cl.mu.lock()
			all := cl.stored[skey].clone()
			cl.mu.unlock()
			high := i64(all.len)
			from := p.fetch_offset
			if from < 0 || from > high {
				resp_topic.partitions << kmsg.FetchResponseTopicPartition{
					partition:      p.partition
					error_code:     1 // OFFSET_OUT_OF_RANGE
					high_watermark: high
				}
				continue
			}
			mut batches := ?[]u8(none)
			if from >= 0 && from < high {
				serve := all[from..].clone()
				batch := kmsg.build_record_batch(serve, kmsg.BatchOpts{
					base_offset: from
				}) or { continue }
				batches = batch.clone()
			} else {
				batches = []u8{}
			}
			resp_topic.partitions << kmsg.FetchResponseTopicPartition{
				partition:      p.partition
				high_watermark: high
				record_batches: batches
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
			high := i64(cl.stored['${t.topic}/${p.partition}'].len)
			cl.mu.unlock()
			off := if p.timestamp == -2 { i64(0) } else { high }
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
	req_topics := req.topics or { []kmsg.OffsetFetchRequestTopic{} }
	cl.mu.lock()
	mut g := cl.group(req.group)
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
