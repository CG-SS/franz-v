module kgo

import kbin
import kmsg
import net
import sync
import time

// ---------------------------------------------------------------------------
// A fake multi-node Kafka cluster over loopback TCP. Each node serves
// ApiVersions and Metadata; Metadata advertises every node with its real
// loopback port (so discovery actually dials them) and sets controller_id
// to the serving node so tests can tell who answered.
// ---------------------------------------------------------------------------

struct ClusterCfg {
	// advertised maps API key -> max version the cluster advertises.
	advertised map[i16]i16 = {
		i16(0):  i16(9)
		i16(3):  i16(12)
		i16(18): i16(3)
	}
	// api_versions_max caps the ApiVersions *request* version the fake
	// accepts; higher requests get a v0-encoded UNSUPPORTED_VERSION reply
	// (KIP-511 old-broker behavior).
	api_versions_max i16 = 3
	// fail_first_conns: this many first connections (cluster-wide) are
	// accepted and instantly closed, to exercise retries.
	fail_first_conns int
}

struct FakeCluster {
	cfg kmsg.ClusterCfg
mut:
	mu        &sync.Mutex = sync.new_mutex()
	ports     []int
	fails     int
	served_by []int // node ids that served a request, in order
}

fn start_cluster(nodes int, cfg ClusterCfg) &FakeCluster {
	mut cl := &kmsg.FakeCluster{
		cfg: cfg
	}
	cl.fails = cfg.fail_first_conns
	mut listeners := []&net.TcpListener{}
	for _ in 0 .. nodes {
		mut l := net.listen_tcp(.ip, '127.0.0.1:0') or { panic('listen: ${err}') }
		addr := l.addr() or { panic('addr: ${err}') }
		cl.ports << '${addr}'.split(':')[1].int()
		listeners << l
	}
	for i, mut l in listeners {
		spawn cl.serve_node(i, mut l)
	}
	return cl
}

fn (mut cl FakeCluster) serve_node(node_id int, mut l net.TcpListener) {
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

fn (mut cl FakeCluster) serve_conn(node_id int, mut conn net.TcpConn) {
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
		if flexible {
			num_tags := r.read_uvarint()
			for _ in 0 .. num_tags {
				r.read_uvarint()
				sz := int(r.read_uvarint())
				r.span(sz)
			}
		}
		body := payload[r.off..].clone()

		cl.mu.lock()
		cl.served_by << node_id
		cl.mu.unlock()

		resp_body := match key {
			18 { cl.answer_api_versions(body, version) }
			3 { cl.answer_metadata(node_id, body, version) }
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

fn (cl &FakeCluster) api_keys() []ApiVersionsResponseApiKey {
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

fn (mut cl FakeCluster) answer_api_versions(body []u8, version i16) []u8 {
	if version > cl.cfg.api_versions_max {
		// KIP-511: v0-encoded UNSUPPORTED_VERSION response
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

fn (mut cl FakeCluster) answer_metadata(node_id int, body []u8, version i16) []u8 {
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
		cluster_id:    'fake-cluster'
		controller_id: node_id // tells the test which node served
		topics:        req_topics.map(kmsg.MetadataResponseTopic{
			topic:      it.topic
			partitions: [
				kmsg.MetadataResponseTopicPartition{
					partition: 0
					leader:    1 % cl.ports.len
					replicas:  [0, 1]
					isr:       [0, 1]
				},
			]
		})
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

fn (cl &FakeCluster) seed_addr() string {
	return '127.0.0.1:${cl.ports[0]}'
}

fn decode_metadata(body []u8, version i16) !MetadataResponse {
	mut resp := kmsg.MetadataResponse{
		version: version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	return resp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn test_bootstrap_discovery_and_targeted_requests() {
	cl := start_cluster(3, kmsg.ClusterCfg{})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}

	// bootstrap: metadata via seed discovers all 3 nodes
	resp := c.metadata(['orders']) or {
		assert false, 'metadata failed: ${err}'
		return
	}
	assert resp.cluster_id or { '' } == 'fake-cluster'
	assert resp.brokers.len == 3
	assert c.known_brokers() == [0, 1, 2]

	// leader lookup from cache
	leader := c.partition_leader('orders', 0) or {
		assert false, 'leader expected'
		return
	}
	assert leader == 1

	// targeted requests actually reach the addressed nodes
	for node in [0, 1, 2] {
		mut req := kmsg.MetadataRequest{}
		body := c.request_broker(node, mut req) or {
			assert false, 'request to node ${node} failed: ${err}'
			return
		}
		mresp := decode_metadata(body, req.version) or {
			assert false, '${err}'
			return
		}
		assert mresp.controller_id == node, 'expected node ${node} to serve'
	}
}

fn test_version_negotiation_caps() {
	// cluster only speaks Metadata up to v9 while the client supports v13
	cl := start_cluster(1, kmsg.ClusterCfg{
		advertised: {
			i16(3):  i16(9)
			i16(18): i16(3)
		}
	})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}

	mut req := kmsg.MetadataRequest{}
	body := c.request(mut req) or {
		assert false, '${err}'
		return
	}
	// the client negotiated down to the broker max and mutated req.version
	assert req.version == 9
	decode_metadata(body, 9) or {
		assert false, 'decode at negotiated version failed: ${err}'
		return
	}
	nv := c.negotiated_version(cl.seed_addr(), 3) or {
		assert false, 'negotiated version expected'
		return
	}
	assert nv == 9
}

fn test_api_versions_v0_fallback() {
	// ancient broker: rejects ApiVersions v3 with a v0-encoded error 35
	cl := start_cluster(1, kmsg.ClusterCfg{
		api_versions_max: 0
	})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut req := kmsg.MetadataRequest{}
	body := c.request(mut req) or {
		assert false, 'fallback negotiation failed: ${err}'
		return
	}
	resp := decode_metadata(body, req.version) or {
		assert false, '${err}'
		return
	}
	assert resp.brokers.len == 1
}

fn test_unsupported_key_is_terminal() {
	// cluster does not advertise Produce (key 0)
	cl := start_cluster(1, kmsg.ClusterCfg{
		advertised: {
			i16(3):  i16(12)
			i16(18): i16(3)
		}
	})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut req := kmsg.ProduceRequest{}
	start := time.now()
	if _ := c.request(mut req) {
		assert false, 'produce must be unsupported'
	}
	// terminal error: no retries, so this must return fast
	assert time.now() - start < c.cfg.retry_backoff_min
}

fn test_retry_on_connection_failure() {
	// the first two connections are accepted and dropped
	cl := start_cluster(1, kmsg.ClusterCfg{
		fail_first_conns: 2
	})
	mut c := new_client(kmsg.Config{
		seed_brokers:      [cl.seed_addr()]
		retry_backoff_min: 10 * time.millisecond
		retry_backoff_max: 20 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut req := kmsg.MetadataRequest{}
	body := c.request(mut req) or {
		assert false, 'request should survive dropped connections: ${err}'
		return
	}
	decode_metadata(body, req.version) or {
		assert false, '${err}'
		return
	}

	// with retries disabled, the same failure surfaces immediately
	cl2 := start_cluster(1, kmsg.ClusterCfg{
		fail_first_conns: 1
	})
	mut c2 := new_client(kmsg.Config{
		seed_brokers:    [cl2.seed_addr()]
		request_retries: 0
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c2.close()
	}
	mut req2 := kmsg.MetadataRequest{}
	if _ := c2.request(mut req2) {
		assert false, 'zero retries must surface the failure'
	}
}

fn client_worker(mut c Client, n int, results chan int) {
	mut ok := 0
	for i in 0 .. n {
		mut req := kmsg.MetadataRequest{
			topics: [
				kmsg.MetadataRequestTopic{
					topic: 'w-${i}'
				},
			]
		}
		body := c.request(mut req) or { continue }
		resp := decode_metadata(body, req.version) or { continue }
		tname := resp.topics[0].topic or { '' }
		if tname == 'w-${i}' {
			ok++
		}
	}
	results <- ok
}

fn test_concurrent_requests() {
	cl := start_cluster(2, kmsg.ClusterCfg{})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	c.metadata([]) or {
		assert false, '${err}'
		return
	}
	assert c.known_brokers().len == 2

	workers := 8
	per := 10
	results := chan int{cap: workers}
	for _ in 0 .. workers {
		spawn client_worker(mut c, per, results)
	}
	mut total := 0
	for _ in 0 .. workers {
		total += <-results
	}
	assert total == workers * per, 'expected ${workers * per} successes, got ${total}'
	// both nodes served traffic
	cl.mu.lock()
	mut seen := map[int]bool{}
	for id in cl.served_by {
		seen[id] = true
	}
	cl.mu.unlock()
	assert seen.len == 2
}

fn test_close_unblocks_and_fails_fast() {
	cl := start_cluster(1, kmsg.ClusterCfg{})
	mut c := new_client(kmsg.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	c.metadata([]) or {
		assert false, '${err}'
		return
	}
	c.close()
	time.sleep(20 * time.millisecond)
	start := time.now()
	mut req := kmsg.MetadataRequest{}
	if _ := c.request(mut req) {
		assert false, 'request after close must fail'
	}
	assert time.now() - start < 2 * time.second
}
