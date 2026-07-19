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

// ClusterCfg configures a fake cluster.
pub struct ClusterCfg {
pub mut:
	// advertised maps API key -> max version advertised via ApiVersions.
	advertised map[i16]i16 = {
		i16(0):  i16(9)
		i16(3):  i16(12)
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
	mu     &sync.Mutex = sync.new_mutex()
	fails  int
	stored map[string][]kmsg.Record // 'topic/partition' -> records
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

fn (mut cl Cluster) serve_conn(node_id int, mut conn net.TcpConn) {
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
			|| (key == 0 && version >= 9)
		if flexible {
			num_tags := r.read_uvarint()
			for _ in 0 .. num_tags {
				r.read_uvarint()
				sz := int(r.read_uvarint())
				r.span(sz)
			}
		}
		body := payload[r.off..].clone()

		resp_body := match key {
			18 { cl.answer_api_versions(body, version) }
			3 { cl.answer_metadata(node_id, body, version) }
			0 { cl.answer_produce(body, version) }
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
		topic := t.topic
		mut resp_topic := kmsg.ProduceResponseTopic{
			topic: topic
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
