module kgo

import kbin
import kmsg
import time

fn test_cancel_basics() {
	mut c := new_cancel()
	assert !c.is_done()
	c.cancel()
	assert c.is_done()
	c.cancel() // idempotent
	assert c.is_done()
	// done channel fires
	select {
		_ := <-c.done {
			assert true
		}
		100 * time.millisecond {
			assert false, 'done channel should fire after cancel'
		}
	}
}

fn test_cancel_timeout() {
	mut c := new_cancel_timeout(30 * time.millisecond)
	assert !c.is_done()
	rem := c.remaining() or {
		assert false, 'deadline expected'
		return
	}
	assert i64(rem) > 0
	time.sleep(60 * time.millisecond)
	assert c.is_done()
}

fn test_cancel_child_propagation() {
	mut parent := new_cancel()
	mut child := parent.child()
	assert !child.is_done()
	parent.cancel()
	// forwarder runs concurrently; wait for the child's done to fire
	select {
		_ := <-child.done {
			assert true
		}
		500 * time.millisecond {
			assert false, 'child must cancel when parent does'
		}
	}
	assert child.is_done()

	// canceling a child does not cancel the parent
	mut p2 := new_cancel()
	mut c2 := p2.child()
	c2.cancel()
	time.sleep(10 * time.millisecond)
	assert !p2.is_done()
}

fn test_config_defaults_and_validate() {
	cfg := Config{}
	cfg.validate() or {
		assert false, 'default config must validate: ${err}'
		return
	}
	cid := cfg.client_id or { '' }
	assert cid == 'franz-v'
	assert cfg.max_versions.has_key(0)

	mut bad := Config{
		seed_brokers: []
	}
	if _ := bad.validate() {
		assert false, 'empty seeds must fail'
	}
	mut bad2 := Config{
		seed_brokers: ['host:notaport']
	}
	if _ := bad2.validate() {
		assert false, 'bad port must fail'
	}
}

fn test_parse_broker_addr() {
	h1, p1 := parse_broker_addr('broker.example:19092') or {
		assert false, '${err}'
		return
	}
	assert h1 == 'broker.example'
	assert p1 == 19092
	h2, p2 := parse_broker_addr('plain-host') or {
		assert false, '${err}'
		return
	}
	assert h2 == 'plain-host'
	assert p2 == 9092
	if _, _ := parse_broker_addr(':9092') {
		assert false, 'empty host must fail'
	}
	if _, _ := parse_broker_addr('h:99999') {
		assert false, 'oversized port must fail'
	}
}

fn test_backoff_schedule() {
	cfg := Config{
		retry_backoff_min: 100 * time.millisecond
		retry_backoff_max: 1 * time.second
	}
	assert cfg.backoff_for(0) == 100 * time.millisecond
	assert cfg.backoff_for(1) == 200 * time.millisecond
	assert cfg.backoff_for(2) == 400 * time.millisecond
	assert cfg.backoff_for(10) == 1 * time.second // clamped
}

fn test_is_retriable_err() {
	assert is_retriable_err(BrokerConnError{
		host:   'h'
		detail: 'x'
	})
	assert is_retriable_err(RequestTimeoutError{})
	assert !is_retriable_err(ClientClosedError{})
	assert !is_retriable_err(CorrelationMismatchError{
		got:  1
		want: 2
	})
}

fn test_frame_request_wire_bytes() {
	// ApiVersionsRequest v0 (non-flexible, empty body), client id 'me':
	// size=int32(12): key(2)+version(2)+corr(4)+client id(2+2)
	mut req := kmsg.ApiVersionsRequest{
		version: 0
	}
	frame := frame_request(req, 7, 'me')
	assert frame == [u8(0x00), 0x00, 0x00, 0x0c, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
		0x00, 0x02, 0x6d, 0x65]

	// flexible request header (v3) appends an empty tag section (0x00)
	mut req3 := kmsg.ApiVersionsRequest{
		version: 3
	}
	frame3 := frame_request(req3, 1, none)
	// header: key 18, ver 3, corr 1, client id -1 (0xffff), header tags
	// 0x00; body: two empty compact strings (0x01 0x01) + body tags 0x00
	assert frame3[4..] == [u8(0x00), 0x12, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0x00,
		0x01, 0x01, 0x00]
}

fn test_strip_response_header() {
	mut w := kbin.Writer{}
	w.write_int32(42) // correlation
	w.write_uvarint(0) // flexible header tag section
	w.buf << [u8(0xaa), 0xbb]
	body := strip_response_header(w.buf, 42, true) or {
		assert false, '${err}'
		return
	}
	assert body == [u8(0xaa), 0xbb]

	if _ := strip_response_header(w.buf, 43, true) {
		assert false, 'correlation mismatch must error'
	}

	mut w0 := kbin.Writer{}
	w0.write_int32(9)
	w0.buf << [u8(0x01)]
	body0 := strip_response_header(w0.buf, 9, false) or {
		assert false, '${err}'
		return
	}
	assert body0 == [u8(0x01)]
}
