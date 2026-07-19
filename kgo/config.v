// Config configures a client. It is a plain struct with defaults — the
// V-first replacement for franz-go's functional options: construct it
// directly and override only what you need.
module kgo

import kversion
import time

pub struct Config {
pub mut:
	// seed_brokers are the host:port addresses used to discover the
	// cluster; the port defaults to 9092 when omitted.
	seed_brokers []string = ['127.0.0.1:9092']
	// client_id is sent in every request header; none omits it.
	client_id ?string = 'franz-v'
	// software_name/version are reported in ApiVersions requests (v3+).
	software_name    string = 'franz-v'
	software_version string = '0.1.0'
	// dial_timeout bounds TCP connection establishment.
	dial_timeout time.Duration = 10 * time.second
	// request_timeout bounds a single request/response exchange.
	request_timeout time.Duration = 30 * time.second
	// retry_backoff_min/max bound the retry backoff schedule.
	retry_backoff_min time.Duration = 250 * time.millisecond
	retry_backoff_max time.Duration = 2500 * time.millisecond
	// compression selects the codec for produced record batches.
	compression kversion.Codec = .uncompressed
	// required_acks for produce: -1 waits for all in-sync replicas,
	// 1 for the leader only, 0 for no acknowledgment.
	required_acks i16 = -1
	// produce_timeout is the broker-side timeout for produce requests.
	produce_timeout time.Duration = 10 * time.second
	// partitioner assigns partitions to records that do not pin one.
	partitioner kversion.Partitioner = kversion.Partitioner(kversion.KafkaPartitioner{})
	// fetch_max_wait bounds how long the broker may hold a fetch waiting
	// for fetch_min_bytes of data.
	fetch_max_wait time.Duration = 1 * time.second
	// fetch_min_bytes is the least data a broker should return per fetch.
	fetch_min_bytes int = 1
	// fetch_max_bytes bounds one fetch response.
	fetch_max_bytes int = 50 << 20
	// fetch_partition_max_bytes bounds one partition within a fetch.
	fetch_partition_max_bytes int = 1 << 20
	// offset_reset is where a consumer resumes when its offset is out of
	// range (compacted/truncated away).
	offset_reset kversion.StartOffset = .earliest
	// isolation_level: read_committed hides records of open and aborted
	// transactions.
	isolation_level kversion.IsolationLevel = .read_uncommitted
	// request_retries is how many times a retriable request failure is
	// retried (with backoff) before surfacing the error.
	request_retries int = 3
	// max_response_bytes guards frame reads from broken peers.
	max_response_bytes int = 100 << 20
	// max_versions caps the request versions the client will use.
	max_versions kversion.Versions = kversion.stable()
	// logger receives client logs; silent by default.
	logger kversion.Logger = kversion.Logger(kversion.NopLogger{})
	// hooks observe client internals.
	hooks kversion.Hooks
}

// validate returns an error if the configuration is unusable.
pub fn (c &Config) validate() ! {
	if c.seed_brokers.len == 0 {
		return error('config: at least one seed broker is required')
	}
	for s in c.seed_brokers {
		parse_broker_addr(s) or { return error('config: seed broker `${s}`: ${err.msg()}') }
	}
	if i64(c.dial_timeout) <= 0 || i64(c.request_timeout) <= 0 {
		return error('config: timeouts must be positive')
	}
	if i64(c.retry_backoff_min) <= 0 || i64(c.retry_backoff_max) < i64(c.retry_backoff_min) {
		return error('config: retry backoff min must be positive and <= max')
	}
	if c.max_response_bytes < 1024 {
		return error('config: max_response_bytes unreasonably small')
	}
}

// parse_broker_addr splits host[:port], defaulting the port to 9092.
pub fn parse_broker_addr(s string) !(string, int) {
	if s == '' {
		return error('empty address')
	}
	idx := s.last_index(':') or { return s, 9092 }
	host := s[..idx]
	port_str := s[idx + 1..]
	if host == '' {
		return error('empty host')
	}
	for ch in port_str {
		if ch < `0` || ch > `9` {
			return error('invalid port `${port_str}`')
		}
	}
	port := port_str.int()
	if port < 1 || port > 65535 {
		return error('port ${port} out of range')
	}
	return host, port
}

// backoff_for returns the exponential backoff for the given attempt
// (0-based), clamped to the configured bounds.
pub fn (c &Config) backoff_for(attempt int) time.Duration {
	mut d := i64(c.retry_backoff_min)
	for _ in 0 .. attempt {
		d *= 2
		if d >= i64(c.retry_backoff_max) {
			return c.retry_backoff_max
		}
	}
	return time.Duration(d)
}
