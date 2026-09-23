// SASL on the fake cluster: SaslHandshake (KIP-43) and SaslAuthenticate
// (KIP-152) for PLAIN and SCRAM-SHA-256/512 (RFC 5802), plus KIP-368
// session lifetimes. A SASL-enabled node answers only ApiVersions and the
// SASL requests on a connection until it has authenticated.
module kfake

import crypto.hmac
import crypto.pbkdf2
import crypto.rand
import crypto.sha256
import crypto.sha512
import encoding.base64
import kbin
import kmsg
import time

const scram_iterations = 4096

// SaslConn is one connection's SASL progress.
struct SaslConn {
mut:
	mechanism   string
	done        bool
	close_after bool // the broker drops the connection after this response
	expires_at  i64  // unix ms when the session lifetime ends; 0 = never
	// SCRAM exchange state
	step              int
	user              string
	client_first_bare string
	server_first      string
	nonce             string
	salt              []u8
}

fn (cl &Cluster) sasl_enabled() bool {
	return cl.cfg.sasl_users.len > 0
}

// set_sasl_user adds a SASL user or changes its password, as
// kafka-configs --alter does for SCRAM credentials. Later
// authentications use it; established sessions are not affected. SASL
// itself is on only for a cluster started with ClusterCfg.sasl_users.
pub fn (mut cl Cluster) set_sasl_user(user string, pass string) {
	cl.mu.lock()
	defer {
		cl.mu.unlock()
	}
	cl.sasl_users[user] = pass
}

fn (mut cl Cluster) sasl_password(user string) ?string {
	cl.mu.lock()
	defer {
		cl.mu.unlock()
	}
	if pass := cl.sasl_users[user] {
		return pass
	}
	return none
}

fn (cl &Cluster) sasl_mechanism_names() []string {
	if cl.cfg.sasl_mechanisms.len > 0 {
		return cl.cfg.sasl_mechanisms
	}
	return ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
}

// sasl_blocks reports whether a request must be refused on a connection:
// before authentication only ApiVersions and the SASL requests are
// allowed, and after the session lifetime ends nothing is (Kafka then
// closes the connection).
fn (cl &Cluster) sasl_blocks(key i16, auth &SaslConn) bool {
	if !cl.sasl_enabled() || key in [i16(17), 18, 36] {
		return false
	}
	if !auth.done {
		return true
	}
	return auth.expires_at > 0 && time.now().unix_milli() >= auth.expires_at
}

fn (mut cl Cluster) answer_sasl_handshake(body []u8, version i16, mut auth SaslConn) []u8 {
	mut req := kmsg.SASLHandshakeRequest{
		version: version
	}
	mut r := kbin.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	names := cl.sasl_mechanism_names()
	mut resp := kmsg.SASLHandshakeResponse{
		version:              version
		supported_mechanisms: names
	}
	if !cl.sasl_enabled() || req.mechanism !in names {
		resp.error_code = 33 // UNSUPPORTED_SASL_MECHANISM
		auth.close_after = true
	} else {
		auth = SaslConn{
			mechanism: req.mechanism
		}
	}
	mut w := kbin.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) answer_sasl_authenticate(body []u8, version i16, mut auth SaslConn) []u8 {
	mut req := kmsg.SASLAuthenticateRequest{
		version: version
	}
	mut r := kbin.Reader{
		src: body
	}
	req.read_from(mut r) or { return []u8{} }
	mut resp := kmsg.SASLAuthenticateResponse{
		version: version
	}
	if out := cl.sasl_step(mut auth, req.sasl_auth_bytes) {
		resp.sasl_auth_bytes = out
		lifetime := i64(cl.cfg.sasl_session_lifetime) / i64(time.millisecond)
		if auth.done && lifetime > 0 && version >= 1 {
			resp.session_lifetime_millis = lifetime
			auth.expires_at = time.now().unix_milli() + lifetime
		}
	} else {
		resp.error_code = 58 // SASL_AUTHENTICATION_FAILED
		resp.error_message = err.msg()
		auth.close_after = true
	}
	mut w := kbin.Writer{}
	resp.write_to(mut w)
	return w.buf
}

fn (mut cl Cluster) sasl_step(mut auth SaslConn, msg []u8) ![]u8 {
	if auth.done {
		return error('Already authenticated')
	}
	match auth.mechanism {
		'PLAIN' {
			// [authzid] NUL authcid NUL passwd (RFC 4616)
			parts := msg.bytestr().split('\0')
			if parts.len != 3 || parts[1] == '' {
				return error('Authentication failed: invalid PLAIN request')
			}
			if cl.sasl_password(parts[1]) or { '' } != parts[2] || parts[2] == '' {
				return error('Authentication failed: Invalid username or password')
			}
			auth.done = true
			return []u8{}
		}
		'SCRAM-SHA-256', 'SCRAM-SHA-512' {
			return cl.scram_step(mut auth, msg.bytestr())
		}
		else {
			return error('Unexpected SaslAuthenticate before a successful SaslHandshake')
		}
	}
}

fn scram_unescape(s string) string {
	return s.replace('=2C', ',').replace('=3D', '=')
}

fn scram_hmac(sha512_hash bool, key []u8, data []u8) []u8 {
	return if sha512_hash {
		hmac.new(key, data, sha512.sum512, sha512.block_size)
	} else {
		hmac.new(key, data, sha256.sum, sha256.block_size)
	}
}

fn (mut cl Cluster) scram_step(mut auth SaslConn, msg string) ![]u8 {
	use_sha512 := auth.mechanism == 'SCRAM-SHA-512'
	match auth.step {
		0 {
			// client-first-message: gs2-header then n=user,r=nonce[,ext]
			if !msg.starts_with('n,') {
				return error('Authentication failed: channel binding is not supported')
			}
			gs2_end := msg.index_after(',', 2) or {
				return error('Authentication failed: invalid client-first-message')
			}
			bare := msg[gs2_end + 1..]
			kvs := bare.split(',')
			if kvs.len < 2 || !kvs[0].starts_with('n=') || !kvs[1].starts_with('r=')
				|| kvs[1].len < 3 {
				return error('Authentication failed: invalid client-first-message')
			}
			user := scram_unescape(kvs[0][2..])
			cl.sasl_password(user) or {
				return error('Authentication failed: Invalid user credentials')
			}
			salt := rand.bytes(16)!
			server_nonce := base64.encode(rand.bytes(18)!)
			nonce := kvs[1][2..] + server_nonce
			server_first := 'r=${nonce},s=${base64.encode(salt)},i=${scram_iterations}'
			auth.step = 1
			auth.user = user
			auth.client_first_bare = bare
			auth.server_first = server_first
			auth.nonce = nonce
			auth.salt = salt
			return server_first.bytes()
		}
		1 {
			// client-final-message: c=biws,r=nonce,p=proof
			proof_at := msg.last_index(',p=') or {
				return error('Authentication failed: client proof missing')
			}
			without_proof := msg[..proof_at]
			kvs := without_proof.split(',')
			if kvs.len < 2 || kvs[0] != 'c=biws' || kvs[1] != 'r=${auth.nonce}' {
				return error('Authentication failed: invalid client-final-message')
			}
			password := cl.sasl_password(auth.user) or {
				return error('Authentication failed: Invalid user credentials')
			}
			hash_size := if use_sha512 { sha512.size } else { sha256.size }
			salted := if use_sha512 {
				pbkdf2.key(password.bytes(), auth.salt, scram_iterations, hash_size, sha512.new())!
			} else {
				pbkdf2.key(password.bytes(), auth.salt, scram_iterations, hash_size, sha256.new())!
			}
			client_key := scram_hmac(use_sha512, salted, 'Client Key'.bytes())
			stored_key := if use_sha512 {
				sha512.sum512(client_key)
			} else {
				sha256.sum(client_key)
			}
			auth_msg := '${auth.client_first_bare},${auth.server_first},${without_proof}'.bytes()
			client_signature := scram_hmac(use_sha512, stored_key, auth_msg)
			proof := base64.decode(msg[proof_at + 3..])
			if proof.len != client_signature.len {
				return error('Authentication failed: Invalid user credentials')
			}
			mut recovered := []u8{len: proof.len}
			for i in 0 .. proof.len {
				recovered[i] = proof[i] ^ client_signature[i]
			}
			recovered_stored := if use_sha512 {
				sha512.sum512(recovered)
			} else {
				sha256.sum(recovered)
			}
			if !hmac.equal(recovered_stored, stored_key) {
				return error('Authentication failed: Invalid user credentials')
			}
			server_key := scram_hmac(use_sha512, salted, 'Server Key'.bytes())
			server_signature := scram_hmac(use_sha512, server_key, auth_msg)
			auth.step = 2
			auth.done = true
			return 'v=${base64.encode(server_signature)}'.bytes()
		}
		else {
			return error('Authentication failed: SCRAM exchange already complete')
		}
	}
}
