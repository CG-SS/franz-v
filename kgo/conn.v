// Wire framing: Kafka requests and responses are length-prefixed frames
// with versioned headers. Request header v1 is key, version, correlation,
// nullable client id; v2 (used by flexible requests) adds a tag section.
// Response headers carry the correlation id, plus a tag section for
// flexible responses — except ApiVersions responses, which always use
// header v0 so that clients can bootstrap version negotiation.
module kgo

import io
import kbin
import kmsg
import net
import time

// dial_broker establishes a TCP connection with the configured timeout.
fn dial_broker(meta BrokerMetadata, mut cfg Config) !&net.TcpConn {
	start := time.now()
	mut conn := net.dial_tcp(meta.addr()) or {
		elapsed := time.now() - start
		for mut h in cfg.hooks.on_connect {
			h.on_broker_connect(meta, elapsed, false, err.msg())
		}
		return BrokerConnError{
			host:   meta.addr()
			detail: 'dial: ${err.msg()}'
		}
	}
	conn.set_read_timeout(cfg.request_timeout)
	conn.set_write_timeout(cfg.request_timeout)
	elapsed := time.now() - start
	for mut h in cfg.hooks.on_connect {
		h.on_broker_connect(meta, elapsed, true, '')
	}
	return conn
}

// write_all writes the whole buffer, looping over partial writes.
fn write_all(mut conn net.TcpConn, buf []u8) ! {
	mut off := 0
	for off < buf.len {
		n := conn.write(buf[off..]) or { return error('write: ${err.msg()}') }
		if n <= 0 {
			return error('write: connection closed')
		}
		off += n
	}
}

// read_full reads exactly buf.len bytes, looping over partial reads.
fn read_full(mut conn net.TcpConn, mut buf []u8) ! {
	mut off := 0
	for off < buf.len {
		n := conn.read(mut buf[off..]) or {
			// the peer closing the connection is an io.Eof, with no message
			if err is io.Eof {
				return error('read: connection closed')
			}
			return error('read: ${err.msg()}')
		}
		if n <= 0 {
			return error('read: connection closed')
		}
		off += n
	}
}

// frame_request renders the length-prefixed wire frame for a request.
fn frame_request(req kmsg.Request, corr int, client_id ?string) []u8 {
	mut header := kbin.Writer{}
	header.write_int16(req.key())
	header.write_int16(req.version)
	header.write_int32(corr)
	header.write_nullable_string(client_id)
	if req.is_flexible() {
		header.write_uvarint(0) // empty request-header tag section
	}
	mut body := kbin.Writer{}
	req.write_to(mut body)

	mut framed := kbin.Writer{
		buf: []u8{cap: 4 + header.buf.len + body.buf.len}
	}
	framed.write_int32(header.buf.len + body.buf.len)
	framed.buf << header.buf
	framed.buf << body.buf
	return framed.buf
}

// response_header_is_flexible reports whether a response to the given
// request carries a flexible response header. ApiVersions responses always
// use header v0 regardless of the request's flexibility.
fn response_header_is_flexible(req kmsg.Request) bool {
	return req.is_flexible() && req.key() != 18
}

// read_response_frame reads one length-prefixed frame and returns its
// payload (correlation id onward).
fn read_response_frame(mut conn net.TcpConn, max_bytes int) ![]u8 {
	mut size_buf := []u8{len: 4}
	read_full(mut conn, mut size_buf)!
	mut r := kbin.Reader{
		src: size_buf
	}
	size := r.read_int32()
	if size <= 0 || size > max_bytes {
		return ResponseTooLargeError{
			size: size
		}
	}
	mut payload := []u8{len: size}
	read_full(mut conn, mut payload)!
	return payload
}

// strip_response_header verifies the correlation id and skips the response
// header, returning the response body.
fn strip_response_header(payload []u8, want_corr int, flexible_header bool) ![]u8 {
	mut r := kbin.Reader{
		src: payload
	}
	corr := r.read_int32()
	if flexible_header {
		num_tags := r.read_uvarint()
		for _ in 0 .. num_tags {
			r.read_uvarint() // tag
			size := int(r.read_uvarint())
			r.span(size)
		}
	}
	r.complete() or { return error('response header truncated') }
	if corr != want_corr {
		return CorrelationMismatchError{
			got:  corr
			want: want_corr
		}
	}
	return payload[r.off..]
}
