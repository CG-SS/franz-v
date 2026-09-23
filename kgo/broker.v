// Broker: one long-lived worker per broker consuming promised requests
// from a channel — the concurrency shape validated in the original
// proof-of-concept. The worker owns the TCP connection outright, so no
// locking is needed around connection state; requests are answered
// strictly in order (correctness before pipelining, per the port plan).
module kgo

import kbin
import kmsg
import net
import sync
import time

// PromisedResp answers a PromisedReq: the raw response body on success,
// or an error message. body is none exactly when err_msg is non-empty.
pub struct PromisedResp {
pub:
	body      ?[]u8
	err_msg   string
	retriable bool
}

// PromisedReq is a request paired with the channel its response is
// promised on.
pub struct PromisedReq {
pub:
	req  kmsg.Request
	resp chan PromisedResp
}

// Broker is a handle to one broker's worker loop. Obtain it from
// new_broker (the request channel requires initialization) and start the
// worker with start().
@[heap]
pub struct Broker {
pub:
	meta BrokerMetadata
pub mut:
	cfg    Config
	cancel &Cancel
	reqs   chan PromisedReq
mut:
	// sasl_enabled lists the mechanisms the broker reported enabled when
	// it rejected one; later connections pick a configured one of them.
	sasl_enabled []string
}

// new_broker returns a Broker handle for the given address.
pub fn new_broker(meta BrokerMetadata, cfg Config, cancel &Cancel) &Broker {
	return &Broker{
		meta:   meta
		cfg:    cfg
		cancel: cancel
		reqs:   chan PromisedReq{cap: 128}
	}
}

// start spawns the worker loop.
pub fn (mut b Broker) start() {
	spawn b.run()
}

// promise performs one synchronous request, returning the full
// PromisedResp (including the retriable classification). Selecting on
// cancel at both steps ensures callers never hang once the broker (or
// client) is shut down.
pub fn (b &Broker) promise(req kmsg.Request) PromisedResp {
	resp_ch := chan PromisedResp{cap: 1}
	preq := PromisedReq{
		req:  req
		resp: resp_ch
	}
	closed := PromisedResp{
		err_msg: ClientClosedError{}.msg()
	}
	select {
		b.reqs <- preq {
		}
		_ := <-b.cancel.done {
			return closed
		}
	}
	select {
		resp := <-resp_ch {
			return resp
		}
		_ := <-b.cancel.done {
			return closed
		}
	}
	return closed
}

// request is promise() reduced to a Result: the raw response body on
// success. Decode it with the matching kmsg response type at req.version.
pub fn (b &Broker) request(req kmsg.Request) ![]u8 {
	resp := b.promise(req)
	if body := resp.body {
		return body
	}
	return error(resp.err_msg)
}

// ConnClass separates traffic onto distinct connections per broker.
// Kafka processes each connection serially, so long-polling requests
// (fetch max_wait, coordinator-held joins) would otherwise head-of-line
// block unrelated fast requests like metadata, produce, and heartbeats.
enum ConnClass {
	normal
	fetch
	coord
}

fn class_for_key(key i16) ConnClass {
	return match key {
		1 { ConnClass.fetch }
		8, 9, 10, 11, 12, 13, 14 { ConnClass.coord }
		else { ConnClass.normal }
	}
}

// Inflight is one pipelined request awaiting its response, in send order.
struct Inflight {
	corr     int
	flexible bool
	key      i16
	resp     chan PromisedResp
}

// ConnState is one pipelined connection: the writer appends to inflight
// after each successful send; the connection's reader consumes entries in
// order, reads the matching response, and resolves the promise. Kafka
// guarantees in-order responses per connection.
@[heap]
struct ConnState {
mut:
	conn      &net.TcpConn
	inflight  chan Inflight
	mu        &sync.Mutex = sync.new_mutex()
	dead      bool // set by reader on transport/correlation failure
	closed    bool // set once the socket is closed: by the writer, or by a retired connection's reader
	next_corr int
	// reauth_at (unix ms) is when the SASL session is close to expiry;
	// the writer then retires the connection. 0 = never.
	reauth_at i64
	retired   bool // no new requests; the reader closes it once drained
}

fn (mut b Broker) run() {
	mut conns := map[int]&ConnState{}
	mut retired := []&ConnState{}
	defer {
		for _, mut st in conns {
			b.close_state(mut st)
		}
		for mut st in retired {
			b.close_state(mut st)
		}
	}
	for {
		select {
			preq := <-b.reqs {
				b.dispatch(preq, mut conns, mut retired)
			}
			_ := <-b.cancel.done {
				// drain whatever is already queued (non-blocking), then
				// exit; the deferred close resolves in-flight promises
				for {
					select {
						preq := <-b.reqs {
							preq.resp <- PromisedResp{
								err_msg: ClientClosedError{}.msg()
							}
						}
						else {
							return
						}
					}
				}
			}
		}
	}
}

// dispatch writes one request on its class connection and registers it
// in-flight; the connection's reader resolves the promise later. Retired
// connections that are still answering requests are kept in retired.
fn (mut b Broker) dispatch(preq PromisedReq, mut conns map[int]&ConnState, mut retired []&ConnState) {
	class := class_for_key(preq.req.key())
	idx := int(class)

	// reap a connection its reader declared dead, and retire one whose
	// SASL session is about to expire (a new one re-authenticates)
	if idx in conns {
		mut st0 := conns[idx] or { return }
		st0.mu.lock()
		dead := st0.dead
		st0.mu.unlock()
		if dead {
			b.close_state(mut st0)
			conns.delete(idx)
		} else if st0.reauth_at > 0 && time.now().unix_milli() >= st0.reauth_at {
			b.retire_state(mut st0)
			conns.delete(idx)
			// forget the retired connections their readers have closed
			for i := retired.len - 1; i >= 0; i-- {
				mut old := retired[i]
				old.mu.lock()
				closed := old.closed
				old.mu.unlock()
				if closed {
					retired.delete(i)
				}
			}
			retired << st0
		}
	}
	mut st := conns[idx] or {
		conn, session := b.dial_authenticated() or {
			b.cfg.log(.warn, 'connecting to ${b.meta.addr()} failed: ${err.msg()}')
			preq.resp <- PromisedResp{
				err_msg:   err.msg()
				retriable: err !is SaslError
			}
			return
		}
		b.cfg.log(.debug, 'connected to ${b.meta.addr()} (${class})')
		mut nst := &ConnState{
			conn:      conn
			inflight:  chan Inflight{cap: b.cfg.max_inflight}
			next_corr: session.next_corr
			reauth_at: session.reauth_at
		}
		spawn b.reader(mut nst)
		conns[idx] = nst
		nst
	}

	corr := st.next_corr
	st.next_corr++
	frame := frame_request(preq.req, corr, b.cfg.client_id)
	wstart := time.now()
	write_all(mut st.conn, frame) or {
		for mut h in b.cfg.hooks.on_write {
			h.on_broker_write(b.meta, preq.req.key(), 0, time.now() - wstart, false)
		}
		b.close_state(mut st)
		conns.delete(idx)
		preq.resp <- PromisedResp{
			err_msg:   BrokerConnError{
				host:   b.meta.addr()
				detail: err.msg()
			}.msg()
			retriable: true
		}
		return
	}
	for mut h in b.cfg.hooks.on_write {
		h.on_broker_write(b.meta, preq.req.key(), frame.len, time.now() - wstart, true)
	}
	// registering after the write is safe: the reader takes the entry
	// first and only then reads the socket
	st.inflight <- Inflight{
		corr:     corr
		flexible: response_header_is_flexible(preq.req)
		key:      preq.req.key()
		resp:     preq.resp
	}
}

// reader resolves in-flight promises for one connection, in order. On any
// transport or correlation failure it fails the current and all further
// entries (retriable) and waits for the writer to close the channel.
fn (mut b Broker) reader(mut st ConnState) {
	mut failed := false
	mut fail_msg := ''
	for {
		entry := <-st.inflight or { break } // closed by writer: exit
		if failed {
			entry.resp <- PromisedResp{
				err_msg:   fail_msg
				retriable: true
			}
			continue
		}
		rstart := time.now()
		payload := read_response_frame(mut st.conn, b.cfg.max_response_bytes) or {
			for mut h in b.cfg.hooks.on_read {
				h.on_broker_read(b.meta, entry.key, 0, time.now() - rstart, false)
			}
			failed = true
			fail_msg = BrokerConnError{
				host:   b.meta.addr()
				detail: err.msg()
			}.msg()
			st.mu.lock()
			st.dead = true
			st.mu.unlock()
			entry.resp <- PromisedResp{
				err_msg:   fail_msg
				retriable: err !is ResponseTooLargeError
			}
			continue
		}
		for mut h in b.cfg.hooks.on_read {
			h.on_broker_read(b.meta, entry.key, 4 + payload.len, time.now() - rstart, true)
		}
		body := strip_response_header(payload, entry.corr, entry.flexible) or {
			// correlation confusion poisons the connection
			failed = true
			fail_msg = err.msg()
			st.mu.lock()
			st.dead = true
			st.mu.unlock()
			entry.resp <- PromisedResp{
				err_msg:   fail_msg
				retriable: true
			}
			continue
		}
		entry.resp <- PromisedResp{
			body: body
		}
	}
	// a retired connection is closed here, once its last response is read
	st.mu.lock()
	retired := st.retired && !st.closed
	if retired {
		st.closed = true
	}
	st.mu.unlock()
	if retired {
		st.conn.close() or {}
		for mut h in b.cfg.hooks.on_disconnect {
			h.on_broker_disconnect(b.meta)
		}
	}
}

// close_state closes a connection's socket and in-flight channel exactly
// once; the reader then fails whatever remains queued and exits.
fn (mut b Broker) close_state(mut st ConnState) {
	st.mu.lock()
	already := st.closed
	retired := st.retired
	st.closed = true
	st.dead = true
	st.mu.unlock()
	if already {
		return
	}
	st.conn.close() or {}
	if !retired {
		st.inflight.close()
	}
	for mut h in b.cfg.hooks.on_disconnect {
		h.on_broker_disconnect(b.meta)
	}
}

// retire_state stops sending on a connection whose SASL session is about
// to expire. Its reader still answers every request already in flight,
// then closes the socket; new requests go to a new, freshly authenticated
// connection.
fn (mut b Broker) retire_state(mut st ConnState) {
	st.mu.lock()
	skip := st.closed || st.retired
	st.retired = true
	st.mu.unlock()
	if !skip {
		st.inflight.close()
	}
}

// request_api_versions negotiates ApiVersions with the broker and returns
// the parsed response; the first step of the client request path.
pub fn (b &Broker) request_api_versions() !kmsg.ApiVersionsResponse {
	mut req := kmsg.ApiVersionsRequest{
		version:                 3
		client_software_name:    b.cfg.software_name
		client_software_version: b.cfg.software_version
	}
	body := b.request(req)!
	mut resp := kmsg.ApiVersionsResponse{
		version: req.version
	}
	mut r := kbin.Reader{
		src: body
	}
	resp.read_from(mut r)!
	return resp
}
