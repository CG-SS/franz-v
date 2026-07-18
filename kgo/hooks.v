// Hooks observe client internals (connections, request writes, response
// reads). Following the port plan, each hook interface has its own
// registered slice rather than one heterogeneous list.
module kgo

import time

// BrokerMetadata identifies a broker.
pub struct BrokerMetadata {
pub:
	node_id int = -1
	host    string
	port    int
}

// addr returns the broker's dialable host:port address.
pub fn (m BrokerMetadata) addr() string {
	return '${m.host}:${m.port}'
}

// HookBrokerConnect is called after a dial attempt.
pub interface HookBrokerConnect {
mut:
	on_broker_connect(meta BrokerMetadata, dial_time time.Duration, ok bool, err_msg string)
}

// HookBrokerDisconnect is called when a broker connection closes.
pub interface HookBrokerDisconnect {
mut:
	on_broker_disconnect(meta BrokerMetadata)
}

// HookBrokerWrite is called after a request write attempt.
pub interface HookBrokerWrite {
mut:
	on_broker_write(meta BrokerMetadata, key i16, bytes_written int, write_time time.Duration, ok bool)
}

// HookBrokerRead is called after a response read attempt.
pub interface HookBrokerRead {
mut:
	on_broker_read(meta BrokerMetadata, key i16, bytes_read int, read_time time.Duration, ok bool)
}

// Hooks holds all registered hooks.
pub struct Hooks {
pub mut:
	on_connect    []HookBrokerConnect
	on_disconnect []HookBrokerDisconnect
	on_write      []HookBrokerWrite
	on_read       []HookBrokerRead
}
