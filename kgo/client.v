// Client: the multi-broker request path. It bootstraps from seed brokers,
// negotiates per-broker API versions via ApiVersions (with the v0 fallback
// old brokers require), discovers the cluster through Metadata, caches
// metadata, and retries retriable failures with backoff.
//
// Concurrency follows the port plan's mutex-first rule: one client mutex
// guards the broker registry, negotiated versions, and the metadata cache;
// each Broker worker owns its connection outright.
module kgo

import kbin
import kmsg
import kversion
import sync
import time

// Client is a Kafka client handle. Obtain one from new_client (the seed
// broker workers and internal state require initialization) and shut it
// down with close().
@[heap]
pub struct Client {
pub mut:
	cfg    kversion.Config
	cancel &kversion.Cancel
mut:
	mu          &sync.Mutex = sync.new_mutex()
	seeds       []&kversion.Broker
	brokers     map[int]&kversion.Broker     // node id -> discovered broker
	versions    map[string]kversion.Versions // broker addr -> negotiated versions
	topic_ids   map[string][16]u8            // topic name -> uuid (KIP-516)
	names_by_id map[string]string            // uuid hex -> topic name
	meta        ?kversion.MetadataResponse
	rr          u32 // round-robin cursor for any_broker
}

// new_client validates cfg, starts a worker per seed broker, and returns
// the client. No network traffic happens until the first request.
pub fn new_client(cfg Config) !&Client {
	cfg.validate()!
	mut cancel := new_cancel()
	mut c := &kversion.Client{
		cfg:    cfg
		cancel: cancel
	}
	for i, seed in cfg.seed_brokers {
		host, port := parse_broker_addr(seed)!
		mut b := new_broker(kversion.BrokerMetadata{
			node_id: -(i + 1) // seeds get negative ids, like franz-go
			host:    host
			port:    port
		}, cfg, cancel)
		b.start()
		c.seeds << b
	}
	return c
}

// close shuts the client down: all broker workers exit and outstanding
// requests are answered with ClientClosedError.
pub fn (mut c Client) close() {
	c.cancel.cancel()
}

// ---------------------------------------------------------------------------
// Broker selection
// ---------------------------------------------------------------------------

// any_broker picks a broker round-robin, preferring discovered brokers
// over seeds.
fn (mut c Client) any_broker() ?&Broker {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	mut pool := []&kversion.Broker{}
	for _, b in c.brokers {
		pool << b
	}
	if pool.len == 0 {
		pool = c.seeds.clone()
	}
	if pool.len == 0 {
		return none
	}
	b := pool[int(c.rr % u32(pool.len))]
	c.rr++
	return b
}

// broker_by_node returns the discovered broker with the given node id, or
// a seed for negative ids.
fn (mut c Client) broker_by_node(node_id int) ?&Broker {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	if node_id < 0 {
		idx := -node_id - 1
		if idx < c.seeds.len {
			return c.seeds[idx]
		}
		return none
	}
	return c.brokers[node_id] or { return none }
}

// ---------------------------------------------------------------------------
// Version negotiation
// ---------------------------------------------------------------------------

// negotiate_api_versions performs the ApiVersions exchange with the
// old-broker fallback: request at v3; if the response does not decode as
// v3 the broker answered with a v0 body (ancient broker, or
// UNSUPPORTED_VERSION per KIP-511), so decode as v0 and, on error 35,
// re-request at v0.
fn (mut c Client) negotiate_api_versions(mut b Broker) !ApiVersionsResponse {
	mut req := kversion.ApiVersionsRequest{
		version:                 3
		client_software_name:    c.cfg.software_name
		client_software_version: c.cfg.software_version
	}
	body := b.request(req)!
	mut resp := kversion.ApiVersionsResponse{
		version: req.version
	}
	mut r := kversion.Reader{
		src: body
	}
	resp.read_from(mut r) or {
		// v0-encoded body from an older broker
		resp = kversion.ApiVersionsResponse{
			version: 0
		}
		mut r0 := kversion.Reader{
			src: body
		}
		resp.read_from(mut r0)!
	}
	if resp.error_code == 35 { // UNSUPPORTED_VERSION: re-request at v0
		mut req0 := kversion.ApiVersionsRequest{
			version: 0
		}
		body0 := b.request(req0)!
		resp = kversion.ApiVersionsResponse{
			version: 0
		}
		mut r0 := kversion.Reader{
			src: body0
		}
		resp.read_from(mut r0)!
	}
	return resp
}

// versions_for returns (negotiating on first use) the broker's supported
// versions.
fn (mut c Client) versions_for(mut b Broker) !Versions {
	addr := b.meta.addr()
	c.mu.lock()
	if vs := c.versions[addr] {
		c.mu.unlock()
		return vs
	}
	c.mu.unlock()

	resp := c.negotiate_api_versions(mut b)!
	vs := kversion.from_api_versions_response(&resp)
	c.mu.lock()
	c.versions[addr] = vs
	c.mu.unlock()
	c.cfg.log(.debug, 'negotiated ${vs.reqs.len} api keys with ${addr}')
	return vs
}

// negotiated_version reports the version the client would use for the
// given key on the given broker address, once negotiated.
pub fn (mut c Client) negotiated_version(addr string, key i16) ?i16 {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	vs := c.versions[addr] or { return none }
	bmax := vs.lookup_max_key_version(key)?
	mut v := bmax
	if cmax := c.cfg.max_versions.lookup_max_key_version(key) {
		if cmax < v {
			v = cmax
		}
	}
	return v
}

// choose_version resolves the version to use for req against one broker:
// the minimum of the client's supported max, the configured cap, and the
// broker's advertised max.
fn (c &Client) choose_version(req Request, broker_vs Versions) !i16 {
	key := req.key()
	bmax := broker_vs.lookup_max_key_version(key) or {
		return kversion.UnsupportedVersionError{
			key: key
		}
	}
	mut v := req.max_version()
	if cmax := c.cfg.max_versions.lookup_max_key_version(key) {
		if cmax < v {
			v = cmax
		}
	}
	if bmax < v {
		v = bmax
	}
	if v < 0 {
		return kversion.UnsupportedVersionError{
			key: key
		}
	}
	return v
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

// request_on issues req on one specific broker, negotiating versions as
// needed and setting req.version to the chosen version.
// cap, when >= 0, bounds the negotiated version (used e.g. to stay on
// name-addressed Produce until topic-id resolution is implemented).
fn (mut c Client) request_on(mut b Broker, mut req Request, cap i16) PromisedResp {
	vs := c.versions_for(mut b) or {
		return kversion.PromisedResp{
			err_msg:   err.msg()
			retriable: is_retriable_err(err) || err.msg().contains('connection')
		}
	}
	mut ver := c.choose_version(req, vs) or {
		return kversion.PromisedResp{
			err_msg: err.msg()
		}
	}
	if cap >= 0 && ver > cap {
		ver = cap
	}
	req.version = ver
	return b.promise(req)
}

// request issues req against any broker, retrying retriable failures on
// (possibly different) brokers with backoff, up to cfg.request_retries
// retries. On success, req.version holds the negotiated version — decode
// the returned body with the matching response type at that version.
pub fn (mut c Client) request(mut req Request) ![]u8 {
	mut attempt := 0
	for {
		mut b := c.any_broker() or { return kversion.NoBrokersError{} }
		res := c.request_on(mut b, mut req, -1)
		if body := res.body {
			return body
		}
		if !res.retriable || attempt >= c.cfg.request_retries {
			return error(res.err_msg)
		}
		attempt++
		c.cfg.log(.debug, 'retrying request key ${req.key()} (attempt ${attempt}): ${res.err_msg}')
		time.sleep(c.cfg.backoff_for(attempt - 1))
		if c.cancel.is_done() {
			return kversion.ClientClosedError{}
		}
	}
	return kversion.NoBrokersError{}
}

// request_broker issues req against one specific broker node, retrying
// retriable failures on that same broker.
pub fn (mut c Client) request_broker(node_id int, mut req Request) ![]u8 {
	return c.request_broker_capped(node_id, mut req, -1)
}

// request_broker_capped is request_broker with an upper bound on the
// negotiated version (cap < 0 means uncapped).
pub fn (mut c Client) request_broker_capped(node_id int, mut req Request, cap i16) ![]u8 {
	mut attempt := 0
	for {
		mut b := c.broker_by_node(node_id) or { return error('unknown broker node ${node_id}') }
		res := c.request_on(mut b, mut req, cap)
		if body := res.body {
			return body
		}
		if !res.retriable || attempt >= c.cfg.request_retries {
			return error(res.err_msg)
		}
		attempt++
		time.sleep(c.cfg.backoff_for(attempt - 1))
		if c.cancel.is_done() {
			return kversion.ClientClosedError{}
		}
	}
	return kversion.NoBrokersError{}
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

// metadata requests cluster metadata for the given topics (empty = all
// topics), updates the cached metadata, and starts workers for any newly
// discovered brokers.
pub fn (mut c Client) metadata(topics []string) !MetadataResponse {
	mut req := kversion.MetadataRequest{}
	if topics.len > 0 {
		req.topics = topics.map(kversion.MetadataRequestTopic{
			topic: it
		})
	}
	body := c.request(mut req)!
	mut resp := kversion.MetadataResponse{
		version: req.version
	}
	mut r := kversion.Reader{
		src: body
	}
	resp.read_from(mut r)!
	c.apply_metadata(resp)
	return resp
}

// apply_metadata stores the cache and registers newly discovered brokers.
fn (mut c Client) apply_metadata(resp MetadataResponse) {
	c.mu.lock()
	c.meta = resp
	zero := [16]u8{}
	for t in resp.topics {
		tname := t.topic or { continue }
		if t.topic_id != zero {
			c.topic_ids[tname] = t.topic_id
			c.names_by_id[t.topic_id[..].hex()] = tname
		}
	}
	mut to_start := []&kversion.Broker{}
	for br in resp.brokers {
		if br.node_id !in c.brokers {
			mut b := new_broker(kversion.BrokerMetadata{
				node_id: br.node_id
				host:    br.host
				port:    br.port
			}, c.cfg, c.cancel)
			c.brokers[br.node_id] = b
			to_start << b
		}
	}
	c.mu.unlock()
	for mut b in to_start {
		b.start()
		c.cfg.log(.info, 'discovered broker ${b.meta.node_id} at ${b.meta.addr()}')
	}
}

// cached_metadata returns the last metadata response, if any.
pub fn (mut c Client) cached_metadata() ?MetadataResponse {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	return c.meta
}

// partition_leader returns the node id leading the given partition,
// according to cached metadata.
pub fn (mut c Client) partition_leader(topic string, partition int) ?int {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	meta := c.meta or { return none }
	for t in meta.topics {
		tname := t.topic or { continue }
		if tname != topic {
			continue
		}
		for p in t.partitions {
			if p.partition == partition {
				return p.leader
			}
		}
	}
	return none
}

// topic_id returns the KIP-516 uuid of a topic, once learned from
// metadata; none if unknown.
pub fn (mut c Client) topic_id(topic string) ?[16]u8 {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	id := c.topic_ids[topic] or { return none }
	return id
}

// topic_by_id resolves a KIP-516 topic uuid back to its name.
pub fn (mut c Client) topic_by_id(id [16]u8) ?string {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	return c.names_by_id[id[..].hex()] or { return none }
}

// known_brokers returns the node ids of all discovered brokers, sorted.
pub fn (mut c Client) known_brokers() []int {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	mut ids := c.brokers.keys()
	ids.sort()
	return ids
}
