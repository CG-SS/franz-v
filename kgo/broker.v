// Broker: one long-lived worker per broker consuming promised requests
// from a channel — the concurrency shape validated in the original
// proof-of-concept. The worker owns the TCP connection outright, so no
// locking is needed around connection state; requests are answered
// strictly in order (correctness before pipelining, per the port plan).
module kgo

import kbin
import kmsg
import net
import time

// PromisedResp answers a PromisedReq: the raw response body on success,
// or an error message. body is none exactly when err_msg is non-empty.
pub struct PromisedResp {
pub:
	body    ?[]u8
	err_msg string
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

// request performs one synchronous request, returning the raw response
// body. Decode it with the matching kmsg response type at req.version.
// Selecting on cancel at both steps ensures callers never hang once the
// broker (or client) is shut down.
pub fn (b &Broker) request(req kmsg.Request) ![]u8 {
	resp_ch := chan PromisedResp{cap: 1}
	preq := PromisedReq{
		req:  req
		resp: resp_ch
	}
	select {
		b.reqs <- preq {}
		_ := <-b.cancel.done {
			return ClientClosedError{}
		}
	}
	select {
		resp := <-resp_ch {
			if body := resp.body {
				return body
			}
			return error(resp.err_msg)
		}
		_ := <-b.cancel.done {
			return ClientClosedError{}
		}
	}
	return ClientClosedError{}
}

struct BrokerState {
mut:
	conn      ?&net.TcpConn
	next_corr int
}

fn (mut b Broker) run() {
	mut st := BrokerState{}
	defer {
		b.close_conn(mut st)
	}
	for {
		select {
			preq := <-b.reqs {
				resp := b.serve(mut st, preq.req)
				preq.resp <- resp
			}
			_ := <-b.cancel.done {
				// drain whatever is already queued (non-blocking), then
				// exit; the deferred close fires the disconnect hooks
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

fn (mut b Broker) close_conn(mut st BrokerState) {
	if mut conn := st.conn {
		conn.close() or {}
		for mut h in b.cfg.hooks.on_disconnect {
			h.on_broker_disconnect(b.meta)
		}
	}
	st.conn = none
}

// serve performs one request/response exchange, connecting on demand and
// tearing the connection down on any transport error.
fn (mut b Broker) serve(mut st BrokerState, req kmsg.Request) PromisedResp {
	mut conn := st.conn or {
		c := dial_broker(b.meta, mut b.cfg) or {
			b.cfg.log(.warn, 'dial ${b.meta.addr()} failed: ${err.msg()}')
			return PromisedResp{
				err_msg: err.msg()
			}
		}
		b.cfg.log(.debug, 'connected to ${b.meta.addr()}')
		st.conn = c
		c
	}

	corr := st.next_corr
	st.next_corr++

	frame := frame_request(req, corr, b.cfg.client_id)
	wstart := time.now()
	write_all(mut conn, frame) or {
		b.close_conn(mut st)
		for mut h in b.cfg.hooks.on_write {
			h.on_broker_write(b.meta, req.key(), 0, time.now() - wstart, false)
		}
		return PromisedResp{
			err_msg: BrokerConnError{
				host:   b.meta.addr()
				detail: err.msg()
			}.msg()
		}
	}
	for mut h in b.cfg.hooks.on_write {
		h.on_broker_write(b.meta, req.key(), frame.len, time.now() - wstart, true)
	}

	rstart := time.now()
	payload := read_response_frame(mut conn, b.cfg.max_response_bytes) or {
		b.close_conn(mut st)
		for mut h in b.cfg.hooks.on_read {
			h.on_broker_read(b.meta, req.key(), 0, time.now() - rstart, false)
		}
		return PromisedResp{
			err_msg: BrokerConnError{
				host:   b.meta.addr()
				detail: err.msg()
			}.msg()
		}
	}
	for mut h in b.cfg.hooks.on_read {
		h.on_broker_read(b.meta, req.key(), 4 + payload.len, time.now() - rstart, true)
	}

	body := strip_response_header(payload, corr, response_header_is_flexible(req)) or {
		// correlation/framing confusion poisons the connection
		b.close_conn(mut st)
		return PromisedResp{
			err_msg: err.msg()
		}
	}
	return PromisedResp{
		body: body
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
