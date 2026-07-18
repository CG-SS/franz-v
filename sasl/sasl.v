// Module sasl specifies interfaces for SASL authentication mechanisms; a V
// implementation of franz-go's pkg/sasl. PLAIN and SCRAM-SHA-256/512 are
// provided; OAUTH, AWS and GSSAPI are out of scope for now (GSSAPI needs a
// Kerberos binding V does not have).
module sasl

// SaslStart is the result of beginning an authentication flow: the session
// to continue with and the initial client message.
pub struct SaslStart {
pub:
	session Session
	msg     []u8
}

// SaslStep is the result of one challenge exchange: whether authentication
// is complete, and the client response to send if not empty.
pub struct SaslStep {
pub:
	done bool
	msg  []u8
}

// Mechanism authenticates.
pub interface Mechanism {
	// name returns the SASL mechanism name (e.g. 'PLAIN').
	name() string
	// authenticate initializes an authentication flow for the given host,
	// returning the session and the initial client message.
	authenticate(host string) !SaslStart
}

// Session is an active authentication flow.
pub interface Session {
mut:
	// challenge processes a server response. Authentication is complete
	// when done is true; until then, msg is the next client message.
	challenge(resp []u8) !SaslStep
}
