// SCRAM-SHA-256 and SCRAM-SHA-512 SASL (RFC 5802 / RFC 7677), as used by
// Kafka; a V implementation of franz-go's pkg/sasl/scram.
module sasl

import crypto.hmac
import crypto.pbkdf2
import crypto.rand
import crypto.sha256
import crypto.sha512
import encoding.base64

// ScramAuth holds SCRAM credentials.
pub struct ScramAuth {
pub mut:
	// zid is an optional authorization ID.
	zid string
	// user is the SASL username (SASLprep normalization is not applied;
	// usernames with '=' or ',' are escaped per RFC 5802).
	user string
	// pass is the SASL password.
	pass string
	// nonce, if provided, overrides the random nonce (for tests).
	nonce []u8
	// is_token signals that user/pass are a Kafka delegation token
	// HMAC, adding the tokenauth=true extension (KIP-48).
	is_token bool
}

// Scram is the SCRAM mechanism; the sha512 flag selects SCRAM-SHA-512
// over the default SCRAM-SHA-256.
pub struct Scram {
pub mut:
	auth   ScramAuth
	sha512 bool
}

// name returns 'SCRAM-SHA-256' or 'SCRAM-SHA-512'.
pub fn (s &Scram) name() string {
	return if s.sha512 { 'SCRAM-SHA-512' } else { 'SCRAM-SHA-256' }
}

// escape applies RFC 5802 saslname escaping: '=' -> '=3D', ',' -> '=2C'.
// The '=' pass runs first so '='s introduced by the ',' pass stay intact,
// matching simultaneous-replacement semantics.
fn escape(s string) string {
	return s.replace('=', '=3D').replace(',', '=2C')
}

// base64 without padding, as SCRAM nonces use.
fn b64_raw(b []u8) string {
	return base64.encode(b).trim_right('=')
}

// authenticate begins a SCRAM flow with the client-first-message.
pub fn (s &Scram) authenticate(host string) !SaslStart {
	_ = host
	if s.auth.user == '' || s.auth.pass == '' {
		return error('${s.name()} user and pass must be non-empty')
	}
	raw_nonce := if s.auth.nonce.len > 0 {
		s.auth.nonce
	} else {
		rand.bytes(20)!
	}
	nonce := b64_raw(raw_nonce).bytes()

	mut bare := []u8{cap: 100}
	bare << 'n='.bytes()
	bare << escape(s.auth.user).bytes()
	bare << ',r='.bytes()
	bare << nonce
	if s.auth.is_token {
		bare << ',tokenauth=true'.bytes() // KIP-48
	}

	mut gs2 := 'n,' // no channel binding
	if s.auth.zid != '' {
		gs2 += 'a=' + escape(s.auth.zid)
	}
	gs2 += ','

	mut first := gs2.bytes()
	first << bare

	return SaslStart{
		session: Session(ScramSession{
			pass:              s.auth.pass
			sha512:            s.sha512
			nonce:             nonce
			client_first_bare: bare
			mechanism_name:    s.name()
		})
		msg:     first
	}
}

struct ScramSession {
	pass              string
	sha512            bool
	nonce             []u8
	client_first_bare []u8
	mechanism_name    string
mut:
	step                 int
	exp_server_signature string
}

fn (s &ScramSession) hash_sum(b []u8) []u8 {
	return if s.sha512 { sha512.sum512(b) } else { sha256.sum(b) }
}

fn (s &ScramSession) hmac_sum(key []u8, data []u8) []u8 {
	return if s.sha512 {
		hmac.new(key, data, sha512.sum512, sha512.block_size)
	} else {
		hmac.new(key, data, sha256.sum, sha256.block_size)
	}
}

fn (s &ScramSession) hash_size() int {
	return if s.sha512 { sha512.size } else { sha256.size }
}

fn (mut s ScramSession) challenge(resp []u8) !SaslStep {
	step := s.step
	s.step++
	match step {
		0 {
			return SaslStep{
				done: false
				msg:  s.client_final(resp)!
			}
		}
		1 {
			s.verify_server(resp)!
			return SaslStep{
				done: true
			}
		}
		else {
			return error('challenge / response should be done, but still going at ${step}')
		}
	}
}

// client_final handles the server-first-message
// (nonce "," salt "," iteration-count; extensions ignored) and builds the
// client-final-message with proof.
fn (mut s ScramSession) client_final(server_first []u8) ![]u8 {
	kvs := server_first.bytestr().split(',')
	if kvs.len < 3 {
		return error('got ${kvs.len} kvs != exp min 3')
	}

	if !kvs[0].starts_with('r=') {
		return error('unexpected kv `${kvs[0]}` where nonce expected')
	}
	server_nonce := kvs[0][2..]
	if !server_nonce.starts_with(s.nonce.bytestr()) {
		return error('server did not reply with nonce beginning with client nonce')
	}

	if !kvs[1].starts_with('s=') {
		return error('unexpected kv `${kvs[1]}` where salt expected')
	}
	salt := base64.decode(kvs[1][2..])
	if salt.len == 0 {
		return error('server salt `${kvs[1][2..]}` decode error')
	}

	if !kvs[2].starts_with('i=') {
		return error('unexpected kv `${kvs[2]}` where iterations expected')
	}
	iters_str := kvs[2][2..]
	for c in iters_str {
		if c < `0` || c > `9` {
			return error('server iterations `${iters_str}` parse error')
		}
	}
	iters := iters_str.int()
	if iters < 4096 {
		return error('server iterations ${iters} less than minimum 4096')
	}

	// SaltedPassword := Hi(password, salt, i)
	salted := if s.sha512 {
		pbkdf2.key(s.pass.bytes(), salt, iters, s.hash_size(), sha512.new())!
	} else {
		pbkdf2.key(s.pass.bytes(), salt, iters, s.hash_size(), sha256.new())!
	}

	client_key :=
		s.hmac_sum(salted, 'Client Key'.bytes()) // ClientKey := HMAC(SaltedPassword, "Client Key")
	stored_key := s.hash_sum(client_key) // StoredKey := H(ClientKey)

	// biws is base64('n,,'); like franz-go, the gs2 header echoed here is
	// fixed since Kafka never uses channel binding
	client_final_without_proof := 'c=biws,r=' + server_nonce

	// AuthMessage := client-first-bare "," server-first "," client-final-without-proof
	mut auth_msg := s.client_first_bare.clone()
	auth_msg << `,`
	auth_msg << server_first
	auth_msg << `,`
	auth_msg << client_final_without_proof.bytes()

	client_signature :=
		s.hmac_sum(stored_key, auth_msg) // ClientSignature := HMAC(StoredKey, AuthMessage)
	mut proof := []u8{len: client_key.len}
	for i in 0 .. proof.len {
		proof[i] = client_key[i] ^ client_signature[i] // ClientProof := ClientKey XOR ClientSignature
	}

	server_key :=
		s.hmac_sum(salted, 'Server Key'.bytes()) // ServerKey := HMAC(SaltedPassword, "Server Key")
	server_signature :=
		s.hmac_sum(server_key, auth_msg) // ServerSignature := HMAC(ServerKey, AuthMessage)
	s.exp_server_signature = base64.encode(server_signature)

	return (client_final_without_proof + ',p=' + base64.encode(proof)).bytes()
}

// verify_server checks the server-final-message signature.
fn (s &ScramSession) verify_server(server_final []u8) ! {
	msg := server_final.bytestr()
	if msg.starts_with('e=') {
		return error('server sent authentication error `${msg[2..]}`')
	}
	if !msg.starts_with('v=') {
		return error('unexpected server final `${msg}`')
	}
	if !hmac.equal(msg[2..].bytes(), s.exp_server_signature.bytes()) {
		return error('server signature mismatch')
	}
}
