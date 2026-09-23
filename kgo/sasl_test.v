module kgo

import kfake
import kmsg
import sasl
import sync
import time

// SaslConnHooks counts the connections a client opens and closes; the
// workers of different brokers call it concurrently.
struct SaslConnHooks {
mut:
	mu          &sync.Mutex = sync.new_mutex()
	connects    int
	disconnects int
}

fn (mut h SaslConnHooks) on_broker_connect(meta BrokerMetadata, d time.Duration, ok bool, err_msg string) {
	if ok {
		h.mu.lock()
		h.connects++
		h.mu.unlock()
	}
}

fn (mut h SaslConnHooks) on_broker_disconnect(meta BrokerMetadata) {
	h.mu.lock()
	h.disconnects++
	h.mu.unlock()
}

// counts returns how many connections were opened and closed so far.
fn (mut h SaslConnHooks) counts() (int, int) {
	h.mu.lock()
	defer {
		h.mu.unlock()
	}
	return h.connects, h.disconnects
}

// wait_closed gives a closed client's workers up to 2s to close every
// connection, then returns the counts.
fn (mut h SaslConnHooks) wait_closed() (int, int) {
	for _ in 0 .. 100 {
		connects, disconnects := h.counts()
		if disconnects >= connects {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	return h.counts()
}

fn sasl_cluster(cfg kfake.ClusterCfg) &kfake.Cluster {
	mut c := cfg
	c.sasl_users = {
		'alice': 'alice-secret'
	}
	return kfake.start(1, c)
}

fn sasl_client(cl &kfake.Cluster, mechs []sasl.Mechanism, retries int, hooks &SaslConnHooks) &Client {
	mut cfg := Config{
		seed_brokers:    [cl.seed_addr()]
		fetch_max_wait:  100 * time.millisecond
		request_retries: retries
		sasl:            mechs
	}
	cfg.hooks.on_connect << hooks
	cfg.hooks.on_disconnect << hooks
	return new_client(cfg) or { panic('client: ${err}') }
}

fn plain_mech(pass string) sasl.Mechanism {
	return sasl.Plain{
		auth: sasl.PlainAuth{
			user: 'alice'
			pass: pass
		}
	}
}

fn scram_mech(pass string, sha512 bool) sasl.Mechanism {
	return sasl.Scram{
		auth:   sasl.ScramAuth{
			user: 'alice'
			pass: pass
		}
		sha512: sha512
	}
}

fn test_sasl_mechanisms_authenticate_every_connection() {
	for mech in [plain_mech('alice-secret'), scram_mech('alice-secret', false),
		scram_mech('alice-secret', true)] {
		name := mech.name()
		mut cl := sasl_cluster(kfake.ClusterCfg{
			sasl_mechanisms: [name]
		})
		mut hooks := &SaslConnHooks{}
		mut c := sasl_client(cl, [mech], 3, hooks)
		mut recs := [Record{
			value: 'via ${name}'.bytes()
		}]
		c.produce('events', mut recs) or {
			assert false, '${name} produce: ${err}'
			return
		}
		assert cl.records('events', 0).len == 1
		// fetches use a connection of their own, authenticated as well
		mut co := c.new_consumer(['events'], ConsumerOpts{}) or {
			assert false, '${err}'
			return
		}
		mut got := []Record{}
		for _ in 0 .. 20 {
			got << co.poll() or {
				assert false, '${name} poll: ${err}'
				return
			}
			if got.len > 0 {
				break
			}
		}
		assert got.len == 1
		assert (got[0].value or { []u8{} }).bytestr() == 'via ${name}'
		c.close()
		connects, disconnects := hooks.wait_closed()
		assert connects == cl.connections(0)
		assert disconnects == connects
	}
}

fn test_sasl_rejected_credentials_are_not_retried() {
	for mech in [plain_mech('wrong'), scram_mech('wrong', false), scram_mech('wrong', true)] {
		name := mech.name()
		mut cl := sasl_cluster(kfake.ClusterCfg{})
		mut hooks := &SaslConnHooks{}
		mut c := sasl_client(cl, [mech], 3, hooks)
		mut failed := false
		c.metadata([]) or {
			assert err.msg().contains('SASL ${name}'), err.msg()
			assert err.msg().contains('SASL_AUTHENTICATION_FAILED'), err.msg()
			failed = true
		}
		c.close()
		assert failed, '${name} with a wrong password authenticated'
		// a single connection, closed: the failure is final, not retriable
		assert cl.connections(0) == 1
		connects, disconnects := hooks.counts()
		assert connects == 1
		assert disconnects == 1
	}
}

fn test_sasl_failure_on_a_later_connection_is_not_retried() {
	// A password change leaves established sessions alone, but the next
	// connection fails to authenticate; that is final too, not retried.
	mut cl := sasl_cluster(kfake.ClusterCfg{})
	mut c := sasl_client(cl, [scram_mech('alice-secret', false)], 3, &SaslConnHooks{})
	defer {
		c.close()
	}
	c.metadata([]) or {
		assert false, '${err}'
		return
	}
	cl.set_sasl_user('alice', 'rotated')
	before := cl.connections(0)
	// FindCoordinator travels on a connection of its own
	mut req := kmsg.FindCoordinatorRequest{
		coordinator_key: 'g'
	}
	c.request(mut req) or {
		assert err.msg().contains('SASL_AUTHENTICATION_FAILED'), err.msg()
		assert cl.connections(0) == before + 1
		return
	}
	assert false, 'authenticated with a changed password'
}

fn test_sasl_listener_refuses_unauthenticated_clients() {
	mut cl := sasl_cluster(kfake.ClusterCfg{})
	mut c := sasl_client(cl, []sasl.Mechanism{}, 0, &SaslConnHooks{})
	defer {
		c.close()
	}
	c.metadata([]) or {
		assert err.msg().ends_with('connection error: read: connection closed'), err.msg()
		return
	}
	assert false, 'metadata without SASL succeeded'
}

fn test_sasl_falls_back_to_a_mechanism_the_broker_enables() {
	mut cl := sasl_cluster(kfake.ClusterCfg{
		sasl_mechanisms: ['SCRAM-SHA-512']
	})
	mut c := sasl_client(cl, [plain_mech('alice-secret'), scram_mech('alice-secret', true)],
		0, &SaslConnHooks{})
	defer {
		c.close()
	}
	meta := c.metadata([]) or {
		assert false, 'fallback to SCRAM-SHA-512: ${err}'
		return
	}
	assert meta.brokers.len == 1

	// with no enabled mechanism configured, the broker's list is reported
	mut only_plain := sasl_client(cl, [plain_mech('alice-secret')], 3, &SaslConnHooks{})
	defer {
		only_plain.close()
	}
	only_plain.metadata([]) or {
		assert err.msg().contains('SASL PLAIN'), err.msg()
		assert err.msg().contains('enabled: SCRAM-SHA-512'), err.msg()
		return
	}
	assert false, 'PLAIN authenticated although the broker disabled it'
}

fn test_sasl_connections_are_replaced_before_the_session_expires() {
	// The broker closes a connection on its first request after the
	// session lifetime (KIP-368) ran out; the client must move to a newly
	// authenticated connection before that. Retries are off, so a request
	// sent on an expired connection fails the test.
	mut cl := sasl_cluster(kfake.ClusterCfg{
		sasl_session_lifetime: time.second
	})
	mut hooks := &SaslConnHooks{}
	mut c := sasl_client(cl, [scram_mech('alice-secret', false)], 0, hooks)
	start := time.now()
	mut n := 0
	for time.since(start) < 2500 * time.millisecond {
		c.metadata([]) or {
			assert false, 'metadata #${n} after ${time.since(start)}: ${err}'
			return
		}
		n++
		time.sleep(50 * time.millisecond)
	}
	// the seed connection, the discovered broker's first one, and at
	// least two re-authenticated replacements, the older ones closed
	assert cl.connections(0) >= 4, 'connections: ${cl.connections(0)}'
	connects, disconnects := hooks.counts()
	assert connects == cl.connections(0)
	assert disconnects >= 2
	c.close()
	all_connects, all_disconnects := hooks.wait_closed()
	assert all_disconnects == all_connects
}
