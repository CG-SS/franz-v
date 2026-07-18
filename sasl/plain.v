// PLAIN SASL (RFC 4616), as used by Kafka's SASL_PLAINTEXT / SASL_SSL
// PLAIN mechanism.
module sasl

// PlainAuth holds PLAIN credentials.
pub struct PlainAuth {
pub mut:
	// zid is an optional authorization ID to use in authenticating.
	zid string
	// user is the SASL username.
	user string
	// pass is the SASL password.
	pass string
}

// Plain is the PLAIN mechanism; use it directly as a Mechanism:
//
//	mech := sasl.Plain{
//		auth: sasl.PlainAuth{ user: 'u', pass: 'p' }
//	}
pub struct Plain {
pub mut:
	auth PlainAuth
}

// name returns 'PLAIN'.
pub fn (p &Plain) name() string {
	return 'PLAIN'
}

// authenticate begins a PLAIN flow: the whole exchange is the single
// initial message zid NUL user NUL pass.
pub fn (p &Plain) authenticate(host string) !SaslStart {
	_ = host
	if p.auth.user == '' || p.auth.pass == '' {
		return error('PLAIN user and pass must be non-empty')
	}
	mut msg := []u8{cap: p.auth.zid.len + p.auth.user.len + p.auth.pass.len + 2}
	msg << p.auth.zid.bytes()
	msg << u8(0)
	msg << p.auth.user.bytes()
	msg << u8(0)
	msg << p.auth.pass.bytes()
	return SaslStart{
		session: Session(PlainSession{})
		msg:     msg
	}
}

struct PlainSession {}

fn (mut s PlainSession) challenge(resp []u8) !SaslStep {
	_ = resp
	return SaslStep{
		done: true
	}
}
