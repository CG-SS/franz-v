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
	resp chan kmsg.PromisedResp
}

// Broker is a handle to one broker's worker loop. Obtain it from
// new_broker (the request channel requires initialization) and start the
// worker with start().
@[heap]
pub struct Broker {
pub:
	meta kmsg.BrokerMetadata
pub mut:
	cfg    kmsg.Config
	cancel &kmsg.Cancel
	reqs   chan kmsg.PromisedReq
}

// new_broker returns a Broker handle for the given address.
pub fn new_broker(meta BrokerMetadata, cfg Config, cancel &Cancel) &Broker {
	return &kmsg.Broker{
		meta:   meta
		cfg:    cfg
		cancel: cancel
		reqs:   chan kmsg.PromisedReq{cap: 128}
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
pub fn (b &Broker) promise(req Request) PromisedResp {
	resp_ch := chan kmsg.PromisedResp{cap: 1}
	preq := kmsg.PromisedReq{
		req:  req
		resp: resp_ch
	}
	closed := kmsg.PromisedResp{
		err_msg: kmsg.ClientClosedError{}.msg()
	}
	select {
		b.reqs <- preq {}
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
pub fn (b &Broker) request(req Request) ![]u8 {
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
		1 { kmsg.ConnClass.fetch }
		8, 9, 10, 11, 12, 13, 14 { kmsg.ConnClass.coord }
		else { kmsg.ConnClass.normal }
	}
}

// Inflight is one pipelined request awaiting its response, in send order.
struct Inflight {
	corr     int
	flexible bool
	key      i16
	resp     chan kmsg.PromisedResp
}

// ConnState is one pipelined connection: the writer appends to inflight
// after each successful send; the connection's reader consumes entries in
// order, reads the matching response, and resolves the promise. Kafka
// guarantees in-order responses per connection.
@[heap]
struct ConnState {
mut:
	conn      &net.TcpConn
	inflight  chan kmsg.Inflight
	mu        &sync.Mutex = sync.new_mutex()
	dead      bool // set by reader on transport/correlation failure
	closed    bool // set by writer once socket + channel are closed
	next_corr int
}

fn (mut b Broker) run() {
	mut conns := map[int]&kmsg.ConnState{}
	defer {
		for _, mut st in conns {
			b.close_state(mut st)
		}
	}
	for {
		select {
			preq := <-b.reqs {
				b.dispatch(preq, mut conns)
			}
			_ := <-b.cancel.done {
				// drain whatever is already queued (non-blocking), then
				// exit; the deferred close resolves in-flight promises
				for {
					select {
						preq := <-b.reqs {
							preq.resp <- kmsg.PromisedResp{
								err_msg: kmsg.ClientClosedError{}.msg()
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
// in-flight; the connection's reader resolves the promise later.
fn (mut b Broker) dispatch(preq PromisedReq, mut conns map[int]&ConnState) {
	class := class_for_key(preq.req.key())
	idx := int(class)

	// reap a connection its reader declared dead
	if idx in conns {
		mut st0 := conns[idx] or { return }
		st0.mu.lock()
		dead := st0.dead
		st0.mu.unlock()
		if dead {
			b.close_state(mut st0)
			conns.delete(idx)
		}
	}
	mut st := conns[idx] or {
		conn := dial_broker(b.meta, mut b.cfg) or {
			b.cfg.log(.warn, 'dial ${b.meta.addr()} failed: ${err.msg()}')
			preq.resp <- kmsg.PromisedResp{
				err_msg:   err.msg()
				retriable: true
			}
			return
		}
		b.cfg.log(.debug, 'connected to ${b.meta.addr()} (${class})')
		mut nst := &kmsg.ConnState{
			conn:     conn
			inflight: chan kmsg.Inflight{cap: b.cfg.max_inflight}
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
		preq.resp <- kmsg.PromisedResp{
			err_msg:   kmsg.BrokerConnError{
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
	st.inflight <- kmsg.Inflight{
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
			entry.resp <- kmsg.PromisedResp{
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
			fail_msg = kmsg.BrokerConnError{
				host:   b.meta.addr()
				detail: err.msg()
			}.msg()
			st.mu.lock()
			st.dead = true
			st.mu.unlock()
			entry.resp <- kmsg.PromisedResp{
				err_msg:   fail_msg
				retriable: err !is kmsg.ResponseTooLargeError
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
			entry.resp <- kmsg.PromisedResp{
				err_msg:   fail_msg
				retriable: true
			}
			continue
		}
		entry.resp <- kmsg.PromisedResp{
			body: body
		}
	}
}

// close_state closes a connection's socket and in-flight channel exactly
// once; the reader then fails whatever remains queued and exits.
fn (mut b Broker) close_state(mut st ConnState) {
	st.mu.lock()
	already := st.closed
	st.closed = true
	st.dead = true
	st.mu.unlock()
	if already {
		return
	}
	st.conn.close() or {}
	st.inflight.close()
	for mut h in b.cfg.hooks.on_disconnect {
		h.on_broker_disconnect(b.meta)
	}
}

// request_api_versions negotiates ApiVersions with the broker and returns
// the parsed response; the first step of the client request path.
pub fn (b &Broker) request_api_versions() !ApiVersionsResponse {
	mut req := kmsg.ApiVersionsRequest{
		version:                 3
		client_software_name:    b.cfg.software_name
		client_software_version: b.cfg.software_version
	}
	body := b.request(req)!
	mut resp := kmsg.ApiVersionsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	return resp
}
