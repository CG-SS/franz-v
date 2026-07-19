// Transactions / exactly-once smoke against a real broker:
// commit visibility, abort isolation (read_committed vs read_uncommitted),
// zombie fencing via epoch bumps, and the consume-transform-produce EOS
// pattern with both commit and abort outcomes.
//
//	v run examples/txn_smoke.v [broker:port]
module main

import kgo
import kmsg
import os
import rand
import time

fn fail(msg string) {
	eprintln('TXN SMOKE FAIL: ${msg}')
	exit(1)
}

fn new_client(addr string, iso kmsg.IsolationLevel) &kmsg.Client {
	return kmsg.new_client(kmsg.Config{
		seed_brokers:    [addr]
		fetch_max_wait:  300 * time.millisecond
		isolation_level: iso
	}) or {
		fail('client: ${err.msg()}')
		exit(1)
	}
}

fn create_topic(mut c kmsg.Client, topic string, partitions int) {
	mut req := kmsg.CreateTopicsRequest{
		timeout_millis: 10000
		topics:         [
			kmsg.CreateTopicsRequestTopic{
				topic:              topic
				num_partitions:     partitions
				replication_factor: 1
			},
		]
	}
	c.request(mut req) or {}
	c.metadata([topic]) or { fail('metadata ${topic}: ${err.msg()}') }
}

fn poll_n(mut co kmsg.Consumer, want int, deadline_ms i64) []kmsg.Record {
	mut out := []kmsg.Record{}
	end := time.now().unix_milli() + deadline_ms
	for time.now().unix_milli() < end {
		out << co.poll() or {
			fail('poll: ${err.msg()}')
			return out
		}
		if out.len >= want {
			break
		}
		time.sleep(100 * time.millisecond)
	}
	return out
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:9092' }
	run := rand.intn(1000000) or { 0 }
	topic := 'franzv-txn-${run}'
	println('txn smoke against ${addr} (topic ${topic})')

	mut c := new_client(addr, .read_uncommitted)
	defer {
		c.close()
	}
	create_topic(mut c, topic, 1)

	mut cc := new_client(addr, .read_committed) // read_committed observer
	defer {
		cc.close()
	}
	mut cu := new_client(addr, .read_uncommitted) // read_uncommitted observer
	defer {
		cu.close()
	}

	// ---- 1. commit visibility ----
	mut t := c.new_txn_producer('franzv-txn-producer-${run}') or {
		fail('txn producer: ${err.msg()}')
		return
	}
	pid, epoch := t.producer()
	println('- initialized: producer id ${pid} epoch ${epoch}')

	t.begin() or {
		fail('begin: ${err.msg()}')
		return
	}
	mut committed := [
		kmsg.Record{
			key:   'c1'.bytes()
			value: 'committed one'.bytes()
		},
		kmsg.Record{
			key:   'c2'.bytes()
			value: 'committed two'.bytes()
		},
	]
	t.produce(topic, mut committed) or {
		fail('txn produce: ${err.msg()}')
		return
	}
	t.commit() or {
		fail('commit: ${err.msg()}')
		return
	}

	mut co_c := cc.new_consumer([topic], kmsg.ConsumerOpts{}) or {
		fail('consumer: ${err.msg()}')
		return
	}
	got := poll_n(mut co_c, 2, 10000)
	if got.len != 2 {
		fail('read_committed after commit: got ${got.len}, want 2')
	}
	v0 := got[0].value or { []u8{} }
	if v0.bytestr() != 'committed one' {
		fail('unexpected first committed record: ${v0.bytestr()}')
	}
	println('- committed transaction visible to read_committed (2 records)')

	// ---- 2. abort isolation ----
	t.begin() or {
		fail('begin 2: ${err.msg()}')
		return
	}
	mut doomed := [
		kmsg.Record{
			key:   'a1'.bytes()
			value: 'aborted one'.bytes()
		},
		kmsg.Record{
			key:   'a2'.bytes()
			value: 'aborted two'.bytes()
		},
	]
	t.produce(topic, mut doomed) or {
		fail('txn produce 2: ${err.msg()}')
		return
	}
	t.abort() or {
		fail('abort: ${err.msg()}')
		return
	}

	// read_committed: nothing new
	after_abort := poll_n(mut co_c, 1, 2500)
	if after_abort.len != 0 {
		v := after_abort[0].value or { []u8{} }
		fail('read_committed leaked aborted record: ${v.bytestr()}')
	}
	// read_uncommitted from earliest: sees committed 2 + aborted 2
	mut co_u := cu.new_consumer([topic], kmsg.ConsumerOpts{}) or {
		fail('consumer u: ${err.msg()}')
		return
	}
	all := poll_n(mut co_u, 4, 10000)
	if all.len != 4 {
		fail('read_uncommitted: got ${all.len}, want 4 (2 committed + 2 aborted)')
	}
	println('- aborted transaction hidden from read_committed, visible to read_uncommitted')

	// committed records still flow after an abort
	t.begin() or {
		fail('begin 3: ${err.msg()}')
		return
	}
	mut third := [
		kmsg.Record{
			value: 'committed three'.bytes()
		},
	]
	t.produce(topic, mut third) or {
		fail('txn produce 3: ${err.msg()}')
		return
	}
	t.commit() or {
		fail('commit 3: ${err.msg()}')
		return
	}
	next_committed := poll_n(mut co_c, 1, 10000)
	if next_committed.len != 1 {
		fail('read_committed after abort+commit: got ${next_committed.len}, want 1')
	}
	nv := next_committed[0].value or { []u8{} }
	if nv.bytestr() != 'committed three' {
		fail('read_committed skipped to wrong record: ${nv.bytestr()}')
	}
	println('- read_committed correctly skipped the aborted range to the next commit')

	// ---- 3. zombie fencing ----
	mut c2 := new_client(addr, .read_uncommitted)
	defer {
		c2.close()
	}
	mut t2 := c2.new_txn_producer('franzv-txn-producer-${run}') or {
		fail('second txn producer: ${err.msg()}')
		return
	}
	_, epoch2 := t2.producer()
	if epoch2 <= epoch {
		fail('epoch did not bump on re-init: ${epoch2} <= ${epoch}')
	}
	mut fenced := false
	t.begin() or {
		fenced = true
		println('- zombie producer fenced on begin: ${err.msg()}')
	}
	if !fenced {
		mut zombie := [
			kmsg.Record{
				value: 'from the zombie'.bytes()
			},
		]
		t.produce(topic, mut zombie) or {
			fenced = true
			println('- zombie producer fenced on produce: ${err.msg()}')
		}
		if !fenced {
			t.commit() or {
				fenced = true
				println('- zombie producer fenced on commit: ${err.msg()}')
			}
		}
	}
	if !fenced {
		fail('zombie producer was not fenced')
	}

	// ---- 4. exactly-once consume-transform-produce ----
	in_topic := 'franzv-eos-in-${run}'
	out_topic := 'franzv-eos-out-${run}'
	group := 'franzv-eos-group-${run}'
	create_topic(mut c, in_topic, 1)
	create_topic(mut c, out_topic, 1)

	mut input := []kmsg.Record{}
	for i in 0 .. 4 {
		input << kmsg.Record{
			value: 'in-${i}'.bytes()
		}
	}
	c.produce(in_topic, mut input) or {
		fail('produce input: ${err.msg()}')
		return
	}

	mut gc := new_client(addr, .read_committed)
	defer {
		gc.close()
	}
	gopts := kmsg.GroupOpts{
		heartbeat_interval: 300 * time.millisecond
	}
	mut g := gc.new_group_consumer(group, [in_topic], gopts) or {
		fail('eos group: ${err.msg()}')
		return
	}
	mut consumed := []kmsg.Record{}
	for _ in 0 .. 40 {
		consumed << g.poll() or {
			fail('eos poll: ${err.msg()}')
			return
		}
		if consumed.len >= 4 {
			break
		}
		time.sleep(150 * time.millisecond)
	}
	if consumed.len != 4 {
		fail('eos consumed ${consumed.len}/4')
	}

	t2.begin() or {
		fail('eos begin: ${err.msg()}')
		return
	}
	mut transformed := []kmsg.Record{}
	for r in consumed {
		v := r.value or { []u8{} }
		transformed << kmsg.Record{
			value: 'OUT(${v.bytestr()})'.bytes()
		}
	}
	t2.produce(out_topic, mut transformed) or {
		fail('eos produce out: ${err.msg()}')
		return
	}
	t2.send_offsets(group, g.positions()) or {
		fail('eos send offsets: ${err.msg()}')
		return
	}
	t2.commit() or {
		fail('eos commit: ${err.msg()}')
		return
	}

	// output visible to read_committed
	mut co_out := cc.new_consumer([out_topic], kmsg.ConsumerOpts{}) or {
		fail('out consumer: ${err.msg()}')
		return
	}
	outs := poll_n(mut co_out, 4, 10000)
	if outs.len != 4 {
		fail('eos out: got ${outs.len}, want 4')
	}
	ov := outs[0].value or { []u8{} }
	if ov.bytestr() != 'OUT(in-0)' {
		fail('eos out first record: ${ov.bytestr()}')
	}
	// offsets applied transactionally: a fresh member of the group sees
	// nothing left on the input topic
	g.close()
	mut gc2 := new_client(addr, .read_committed)
	defer {
		gc2.close()
	}
	mut g2 := gc2.new_group_consumer(group, [in_topic], gopts) or {
		fail('eos group 2: ${err.msg()}')
		return
	}
	leftovers := g2.poll() or {
		fail('eos g2 poll: ${err.msg()}')
		return
	}
	if leftovers.len != 0 {
		fail('eos offsets not applied: replayed ${leftovers.len} records')
	}
	println('- EOS commit: transformed output visible, offsets applied atomically')

	// ---- 5. EOS abort: neither output nor offsets take effect ----
	mut input2 := [
		kmsg.Record{
			value: 'in-late'.bytes()
		},
	]
	c.produce(in_topic, mut input2) or {
		fail('produce late input: ${err.msg()}')
		return
	}
	mut late := []kmsg.Record{}
	for _ in 0 .. 40 {
		late << g2.poll() or {
			fail('late poll: ${err.msg()}')
			return
		}
		if late.len >= 1 {
			break
		}
		time.sleep(150 * time.millisecond)
	}
	if late.len != 1 {
		fail('late consume: ${late.len}/1')
	}
	t2.begin() or {
		fail('abort-eos begin: ${err.msg()}')
		return
	}
	mut doomed_out := [
		kmsg.Record{
			value: 'OUT(in-late)'.bytes()
		},
	]
	t2.produce(out_topic, mut doomed_out) or {
		fail('abort-eos produce: ${err.msg()}')
		return
	}
	t2.send_offsets(group, g2.positions()) or {
		fail('abort-eos send offsets: ${err.msg()}')
		return
	}
	t2.abort() or {
		fail('abort-eos abort: ${err.msg()}')
		return
	}
	extra_out := poll_n(mut co_out, 1, 2500)
	if extra_out.len != 0 {
		fail('aborted EOS output leaked to read_committed')
	}
	g2.close()
	mut gc3 := new_client(addr, .read_committed)
	defer {
		gc3.close()
	}
	mut g3 := gc3.new_group_consumer(group, [in_topic], gopts) or {
		fail('eos group 3: ${err.msg()}')
		return
	}
	mut replayed := []kmsg.Record{}
	for _ in 0 .. 40 {
		replayed << g3.poll() or {
			fail('replay poll: ${err.msg()}')
			return
		}
		if replayed.len >= 1 {
			break
		}
		time.sleep(150 * time.millisecond)
	}
	if replayed.len != 1 {
		fail('aborted EOS offsets applied anyway: replay got ${replayed.len}')
	}
	rv := replayed[0].value or { []u8{} }
	if rv.bytestr() != 'in-late' {
		fail('replay content: ${rv.bytestr()}')
	}
	g3.close()
	println('- EOS abort: output hidden AND offsets rolled back (record replayed)')
	println('TXN SMOKE ALL OK')
}
