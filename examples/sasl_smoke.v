// SASL smoke test for franz-v against a real broker: a SASL_PLAINTEXT
// listener enabling PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 for the user
// alice (password alice-secret), and a second SASL_PLAINTEXT listener
// enabling only SCRAM-SHA-512. With connections.max.reauth.ms set to a few
// seconds it also checks that the client moves to newly authenticated
// connections before sessions expire (KIP-368).
//
//	v run examples/sasl_smoke.v [broker:port] [scram-sha-512-only broker:port]
//	(default 127.0.0.1:19093 and 127.0.0.1:19095)
//
// The Kafka 4.3.1 settings it was run with; the SCRAM credentials come from
// `kafka-storage.sh format --add-scram 'SCRAM-SHA-256=[name=alice,password=alice-secret]'`
// (and the same for SCRAM-SHA-512) or `kafka-configs.sh --alter`:
//
//	listeners=PLAINTEXT://localhost:19092,SASL_PLAINTEXT://localhost:19093,CONTROLLER://localhost:19094,SCRAM512://localhost:19095
//	listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,SCRAM512:SASL_PLAINTEXT
//	inter.broker.listener.name=PLAINTEXT
//	sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
//	listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_alice="alice-secret";
//	listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
//	listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
//	listener.name.scram512.sasl.enabled.mechanisms=SCRAM-SHA-512
//	listener.name.scram512.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
//	connections.max.reauth.ms=3000
//
// Exit code 0 means every step verified.
module main

import kadm
import kgo
import os
import rand
import sasl
import sync
import time

// ConnCounter counts the broker connections a client establishes; the
// workers of different brokers call it concurrently.
struct ConnCounter {
mut:
	mu       &sync.Mutex = sync.new_mutex()
	connects int
}

fn (mut h ConnCounter) on_broker_connect(meta kgo.BrokerMetadata, d time.Duration, ok bool, err_msg string) {
	if ok {
		h.mu.lock()
		h.connects++
		h.mu.unlock()
	}
}

fn (mut h ConnCounter) count() int {
	h.mu.lock()
	defer {
		h.mu.unlock()
	}
	return h.connects
}

@[noreturn]
fn fail(msg string) {
	eprintln('SMOKE FAIL: ${msg}')
	exit(1)
}

fn plain(pass string) sasl.Mechanism {
	return sasl.Plain{
		auth: sasl.PlainAuth{
			user: 'alice'
			pass: pass
		}
	}
}

fn scram(pass string, sha512 bool) sasl.Mechanism {
	return sasl.Scram{
		auth:   sasl.ScramAuth{
			user: 'alice'
			pass: pass
		}
		sha512: sha512
	}
}

fn new_client(addr string, mechs []sasl.Mechanism, retries int, hooks &ConnCounter) &kgo.Client {
	mut cfg := kgo.Config{
		seed_brokers:    [addr]
		fetch_max_wait:  200 * time.millisecond
		request_retries: retries
		sasl:            mechs
	}
	cfg.hooks.on_connect << hooks
	return kgo.new_client(cfg) or { fail('client: ${err.msg()}') }
}

// expect_rejected checks that a metadata request by a client with mechs
// fails with want in the message, on the one connection it opened: the
// failure is final, not retried.
fn expect_rejected(addr string, mechs []sasl.Mechanism, retries int, what string, want string) {
	mut hooks := &ConnCounter{}
	mut c := new_client(addr, mechs, retries, hooks)
	defer {
		c.close()
	}
	start := time.now()
	c.metadata([]) or {
		took := time.since(start)
		if !err.msg().contains(want) {
			fail('${what}: got `${err.msg()}`, want `${want}`')
		}
		if hooks.count() != 1 {
			fail('${what}: failed on ${hooks.count()} connections, it was retried')
		}
		println('  ${what}: rejected in ${took}: ${err.msg()}')
		return
	}
	fail('${what}: metadata succeeded')
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:19093' }
	addr512 := if os.args.len > 2 { os.args[2] } else { '127.0.0.1:19095' }
	topic := 'franzv-sasl-${rand.intn(1000000) or { 0 }}'
	println('SASL smoke against ${addr} (all mechanisms) and ${addr512} (SCRAM-SHA-512 only)')

	// ---- each mechanism produces; everything is consumed back ----
	mut want := []string{}
	for i, mech in [plain('alice-secret'), scram('alice-secret', false), scram('alice-secret', true)] {
		mut c := new_client(addr, [mech], 3, &ConnCounter{})
		if i == 0 {
			mut a := kadm.new(c)
			res := a.create_topics([kadm.TopicSpec{
				topic: topic
			}]) or { fail('create ${topic}: ${err.msg()}') }
			res[0].ok() or { fail('create ${topic}: ${err.msg()}') }
		}
		mut recs := []kgo.Record{}
		for j in 0 .. 3 {
			value := '${mech.name()}-${j}'
			want << value
			recs << kgo.Record{
				value: value.bytes()
			}
		}
		c.produce(topic, mut recs) or { fail('${mech.name()} produce: ${err.msg()}') }
		c.close()
		println('  ${mech.name()}: authenticated, produced 3 records')
	}
	mut c := new_client(addr, [scram('alice-secret', true)], 3, &ConnCounter{})
	mut co := c.new_consumer([topic], kgo.ConsumerOpts{}) or {
		fail('consumer: ${err.msg()}')
	}
	mut got := []string{}
	for _ in 0 .. 50 {
		recs := co.poll() or { fail('poll: ${err.msg()}') }
		for r in recs {
			got << (r.value or { []u8{} }).bytestr()
		}
		if got.len >= want.len {
			break
		}
	}
	if got != want {
		fail('consumed ${got}, want ${want}')
	}
	c.close()
	println('  consumed all ${got.len} records back (fetches authenticated too)')

	// ---- rejected clients fail at once ----
	for mech in [plain('wrong'), scram('wrong', false), scram('wrong', true)] {
		expect_rejected(addr, [mech], 3, '${mech.name()} with a wrong password', 'SASL_AUTHENTICATION_FAILED')
	}
	// the broker just closes the connection, which is retriable in general
	expect_rejected(addr, []sasl.Mechanism{}, 0, 'no SASL', 'read: connection closed')

	// ---- the SCRAM-SHA-512 only listener ----
	mut fb := new_client(addr512, [plain('alice-secret'), scram('alice-secret', true)],
		0, &ConnCounter{})
	fb.metadata([]) or { fail('fallback to SCRAM-SHA-512: ${err.msg()}') }
	fb.close()
	println('  [PLAIN, SCRAM-SHA-512] on the SCRAM-SHA-512 listener: fell back to SCRAM-SHA-512')
	expect_rejected(addr512, [plain('alice-secret')], 3, 'PLAIN on the SCRAM-SHA-512 listener',
		'enabled: SCRAM-SHA-512')

	// ---- sessions expiring (connections.max.reauth.ms) ----
	// no retries: a request sent on an expired session would fail
	mut counter := &ConnCounter{}
	mut rc := new_client(addr, [scram('alice-secret', false)], 0, counter)
	start := time.now()
	mut n := 0
	for time.since(start) < 8 * time.second {
		rc.metadata([]) or { fail('request #${n} after ${time.since(start)}: ${err.msg()}') }
		n++
		time.sleep(100 * time.millisecond)
	}
	rc.close()
	connects := counter.count()
	if connects < 4 {
		fail('${connects} connections in 8s: sessions were never renewed (is connections.max.reauth.ms set?)')
	}
	println('  ${n} requests in 8s, no retries, over ${connects} connections: sessions renewed before expiry')
	println('SASL smoke: OK')
}
