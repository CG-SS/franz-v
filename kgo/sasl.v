// SASL authentication: when Config.sasl is set, every new connection runs
// SaslHandshake (KIP-43) and SaslAuthenticate (KIP-152) with a configured
// mechanism before it carries any other request, and is replaced before
// a broker-set session lifetime (KIP-368) runs out.
module kgo

import kbin
import kerr
import kmsg
import net
import sasl
import time

// SaslSession is the outcome of authenticating one connection.
struct SaslSession {
	next_corr int // first correlation id left for regular requests
	reauth_at i64 // unix ms to replace the connection by; 0 = never
}

// dial_authenticated dials the broker and, when SASL is configured,
// authenticates the new connection. A broker that rejects the mechanism
// names the ones it enables; if another configured mechanism is among
// them, a second connection authenticates with it.
fn (mut b Broker) dial_authenticated() !(&net.TcpConn, SaslSession) {
	for attempt in 0 .. 2 {
		mut conn := dial_broker(b.meta, mut b.cfg)!
		if b.cfg.sasl.len == 0 {
			return conn, SaslSession{}
		}
		session := b.sasl_authenticate(mut conn) or {
			conn.close() or {}
			for mut h in b.cfg.hooks.on_disconnect {
				h.on_broker_disconnect(b.meta)
			}
			if err is SaslError && err.fallback && attempt == 0 {
				continue
			}
			return err
		}
		return conn, session
	}
	return error('SASL: no usable mechanism')
}

// sasl_mechanism returns the first configured mechanism the broker is
// known to enable, else the first configured one.
fn (b &Broker) sasl_mechanism() sasl.Mechanism {
	for m in b.cfg.sasl {
		if m.name() in b.sasl_enabled {
			return m
		}
	}
	return b.cfg.sasl[0]
}

// sasl_version picks the highest version of req's API both sides support:
// the client (req.max_version, Config.max_versions) and the broker, as
// listed in its ApiVersions response.
fn (b &Broker) sasl_version(req kmsg.Request, keys []kmsg.ApiVersionsResponseApiKey) ?i16 {
	key := req.key()
	mut v := req.max_version()
	if cap := b.cfg.max_versions.lookup_max_key_version(key) {
		if cap < v {
			v = cap
		}
	}
	for k in keys {
		if k.api_key == key {
			if k.max_version < v {
				v = k.max_version
			}
			if v < k.min_version {
				return none
			}
			return v
		}
	}
	return none
}

// round_trip writes req as the next request on conn and returns its
// response body. SASL runs before the connection's reader starts, so the
// exchange is synchronous.
fn (b &Broker) round_trip(mut conn net.TcpConn, req kmsg.Request, corr int) ![]u8 {
	write_all(mut conn, frame_request(req, corr, b.cfg.client_id)) or {
		return BrokerConnError{
			host:   b.meta.addr()
			detail: err.msg()
		}
	}
	payload := read_response_frame(mut conn, b.cfg.max_response_bytes) or {
		return BrokerConnError{
			host:   b.meta.addr()
			detail: err.msg()
		}
	}
	return strip_response_header(payload, corr, response_header_is_flexible(req))
}

// sasl_authenticate authenticates a freshly dialed connection.
fn (mut b Broker) sasl_authenticate(mut conn net.TcpConn) !SaslSession {
	mech := b.sasl_mechanism()
	fail := fn [b, mech] (detail string) SaslError {
		return SaslError{
			host:      b.meta.addr()
			mechanism: mech.name()
			detail:    detail
		}
	}
	mut corr := 0

	// ApiVersions is allowed before authentication; v0 is understood by
	// every broker and tells which SASL request versions it speaks
	av_body := b.round_trip(mut conn, kmsg.ApiVersionsRequest{}, corr)!
	corr++
	mut av := kmsg.ApiVersionsResponse{}
	mut avr := kbin.Reader{
		src: av_body
	}
	av.read_from(mut avr)!
	if av.error_code != 0 {
		e := kerr.error_for_code(av.error_code) or { kerr.unknown_server_error }
		return fail('ApiVersions: ${e.msg()}')
	}

	mut hs := kmsg.SASLHandshakeRequest{
		mechanism: mech.name()
	}
	// v0 would carry the authentication bytes unframed (pre-1.0 brokers)
	hs.version = b.sasl_version(hs, av.api_keys) or {
		return fail('broker does not support SaslHandshake v1')
	}
	if hs.version < 1 {
		return fail('broker only supports the legacy SaslHandshake v0')
	}
	hs_body := b.round_trip(mut conn, hs, corr)!
	corr++
	mut hs_resp := kmsg.SASLHandshakeResponse{
		version: hs.version
	}
	mut hsr := kbin.Reader{
		src: hs_body
	}
	hs_resp.read_from(mut hsr)!
	if hs_resp.error_code == 33 { // UNSUPPORTED_SASL_MECHANISM
		b.sasl_enabled = hs_resp.supported_mechanisms.clone()
		mut fallback := false
		for m in b.cfg.sasl {
			if m.name() != mech.name() && m.name() in b.sasl_enabled {
				fallback = true
			}
		}
		return SaslError{
			host:      b.meta.addr()
			mechanism: mech.name()
			detail:    'mechanism not enabled by the broker (enabled: ${b.sasl_enabled.join(', ')})'
			fallback:  fallback
		}
	}
	if hs_resp.error_code != 0 {
		e := kerr.error_for_code(hs_resp.error_code) or { kerr.unknown_server_error }
		return fail(e.msg())
	}

	mut auth := kmsg.SASLAuthenticateRequest{}
	auth.version = b.sasl_version(auth, av.api_keys) or {
		return fail('broker does not support SaslAuthenticate')
	}
	start := mech.authenticate(b.meta.host) or { return fail(err.msg()) }
	mut session := start.session
	mut msg := start.msg.clone()
	started := time.now().unix_milli()
	mut lifetime := i64(0)
	for {
		auth.sasl_auth_bytes = msg
		body := b.round_trip(mut conn, auth, corr)!
		corr++
		mut resp := kmsg.SASLAuthenticateResponse{
			version: auth.version
		}
		mut r := kbin.Reader{
			src: body
		}
		resp.read_from(mut r)!
		if resp.error_code != 0 {
			e := kerr.error_for_code(resp.error_code) or { kerr.unknown_server_error }
			if detail := resp.error_message {
				return fail('${e.msg()} (${detail})')
			}
			return fail(e.msg())
		}
		lifetime = resp.session_lifetime_millis
		step := session.challenge(resp.sasl_auth_bytes) or { return fail(err.msg()) }
		if step.done {
			if step.msg.len > 0 {
				return fail('mechanism finished with a message left to send')
			}
			break
		}
		msg = step.msg.clone()
	}
	b.cfg.log(.debug, 'authenticated to ${b.meta.addr()} with SASL ${mech.name()}')
	return SaslSession{
		next_corr: corr
		// replace the connection once 90% of the session lifetime is used,
		// before the broker closes it on the first request past expiry
		reauth_at: if lifetime > 0 { started + lifetime * 9 / 10 } else { 0 }
	}
}
