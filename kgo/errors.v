// Client-side error types. All implement IError so they flow through !T
// returns and remain matchable with `err is kgo.XxxError`.
module kgo

import kerr

// ClientClosedError is returned when a request is attempted on a closed
// (canceled) client or broker.
pub struct ClientClosedError {
	Error
}

// msg implements IError.
pub fn (e ClientClosedError) msg() string {
	return 'client closed'
}

// RequestTimeoutError is returned when a request's deadline elapsed before
// a response arrived.
pub struct RequestTimeoutError {
	Error
}

// msg implements IError.
pub fn (e RequestTimeoutError) msg() string {
	return 'request timed out'
}

// BrokerConnError wraps dial/read/write failures on a broker connection.
pub struct BrokerConnError {
	Error
pub:
	host   string
	detail string
}

// msg implements IError.
pub fn (e BrokerConnError) msg() string {
	return 'broker ${e.host} connection error: ${e.detail}'
}

// CorrelationMismatchError is returned when a broker responds with an
// unexpected correlation ID; the connection is considered broken.
pub struct CorrelationMismatchError {
	Error
pub:
	got  int
	want int
}

// msg implements IError.
pub fn (e CorrelationMismatchError) msg() string {
	return 'correlation mismatch: got ${e.got}, want ${e.want}'
}

// ResponseTooLargeError guards against absurd frame sizes from a broken or
// malicious peer.
pub struct ResponseTooLargeError {
	Error
pub:
	size int
}

// msg implements IError.
pub fn (e ResponseTooLargeError) msg() string {
	return 'response frame of ${e.size} bytes exceeds the maximum allowed'
}

// is_retriable_err reports whether an error is worth retrying: transient
// connection problems, timeouts, and retriable Kafka error codes.
pub fn is_retriable_err(err IError) bool {
	if err is BrokerConnError || err is RequestTimeoutError {
		return true
	}
	if err is kerr.KafkaError {
		return err.retriable
	}
	return false
}
