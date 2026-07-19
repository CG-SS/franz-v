// Consumer-group smoke against a real broker: two members of one group
// split a 4-partition topic, consume disjointly, then one leaves and the
// survivor reclaims everything.
//
//	v run examples/group_smoke.v [broker:port]
module main

import kgo
import kmsg
import os
import time

fn fail(msg string) {
	eprintln('GROUP SMOKE FAIL: ${msg}')
	exit(1)
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:9092' }
	topic := 'franzv-group-smoke'
	group := 'franzv-smoke-team'
	println('group smoke against ${addr} (topic ${topic}, group ${group})')

	mut c := kmsg.new_client(kmsg.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('client: ${err.msg()}')
		return
	}
	defer {
		c.close()
	}

	mut creq := kmsg.CreateTopicsRequest{
		timeout_millis: 10000
		topics:         [
			kmsg.CreateTopicsRequestTopic{
				topic:              topic
				num_partitions:     4
				replication_factor: 1
			},
		]
	}
	c.request(mut creq) or {}

	opts := kmsg.GroupOpts{
		heartbeat_interval: 300 * time.millisecond
		session_timeout:    10 * time.second
	}
	// one client per member: joins are long-polls and franz-v sends
	// strictly in order per connection
	mut c2 := kmsg.new_client(kmsg.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('client 2: ${err.msg()}')
		return
	}
	defer {
		c2.close()
	}
	mut g1 := c.new_group_consumer(group, [topic], opts) or {
		fail('g1: ${err.msg()}')
		return
	}
	mut g2 := c2.new_group_consumer(group, [topic], opts) or {
		fail('g2: ${err.msg()}')
		return
	}

	// drive g2's blocking first join from a thread while g1 polls along
	done := chan bool{cap: 1}
	spawn fn (mut g2 kmsg.GroupConsumer, done chan bool) {
		g2.poll() or { eprintln('g2 initial poll: ${err.msg()}') }
		done <- true
	}(mut g2, done)

	deadline := time.now().unix_milli() + 45000
	mut settled := false
	for time.now().unix_milli() < deadline {
		g1.poll() or {
			fail('g1 poll during join: ${err.msg()}')
			return
		}
		if g1.assigned()[topic].len == 2 {
			settled = true
			break
		}
		time.sleep(200 * time.millisecond)
	}
	if !settled {
		fail('two-member split never settled (g1 owns ${g1.assigned()[topic].len})')
	}
	_ := <-done
	if g2.assigned()[topic].len != 2 {
		fail('g2 owns ${g2.assigned()[topic].len} partitions, want 2')
	}
	mut owned := map[int]int{}
	for p in g1.assigned()[topic] {
		owned[p] = 1
	}
	for p in g2.assigned()[topic] {
		if p in owned {
			fail('partition ${p} assigned to both members')
		}
		owned[p] = 2
	}
	if owned.len != 4 {
		fail('assignments cover ${owned.len}/4 partitions')
	}
	println('- rebalance settled: g1=${g1.assigned()[topic]} g2=${g2.assigned()[topic]} (real coordinator)')

	// 8 records across the 4 partitions: each lands with exactly one member
	mut records := []kmsg.Record{}
	for i in 0 .. 8 {
		records << kmsg.Record{
			partition: i % 4
			value:     'rec-${i}'.bytes()
		}
	}
	c.produce(topic, mut records) or {
		fail('produce: ${err.msg()}')
		return
	}
	mut got1 := 0
	mut got2 := 0
	for _ in 0 .. 60 {
		got1 += (g1.poll() or {
			fail('g1 poll: ${err.msg()}')
			return
		}).len
		got2 += (g2.poll() or {
			fail('g2 poll: ${err.msg()}')
			return
		}).len
		if got1 + got2 >= 8 {
			break
		}
		time.sleep(100 * time.millisecond)
	}
	if got1 != 4 || got2 != 4 {
		fail('split consumption: g1=${got1} g2=${got2}, want 4/4')
	}
	println('- disjoint consumption: 4 records each')

	g1.commit() or {
		fail('g1 commit: ${err.msg()}')
		return
	}
	g2.commit() or {
		fail('g2 commit: ${err.msg()}')
		return
	}
	println('- offsets committed by both members')

	// g2 leaves; g1 reclaims all four partitions
	g2.close()
	mut reclaimed := false
	deadline2 := time.now().unix_milli() + 45000
	for time.now().unix_milli() < deadline2 {
		g1.poll() or {
			fail('g1 poll after leave: ${err.msg()}')
			return
		}
		if g1.assigned()[topic].len == 4 {
			reclaimed = true
			break
		}
		time.sleep(200 * time.millisecond)
	}
	if !reclaimed {
		fail('survivor never reclaimed all partitions')
	}
	println('- member left; survivor reclaimed all 4 partitions')

	// committed offsets held: new records only
	mut more := []kmsg.Record{}
	for i in 0 .. 4 {
		more << kmsg.Record{
			partition: i
			value:     'post-${i}'.bytes()
		}
	}
	c.produce(topic, mut more) or {
		fail('produce 2: ${err.msg()}')
		return
	}
	mut post := 0
	for _ in 0 .. 60 {
		recs := g1.poll() or {
			fail('g1 final poll: ${err.msg()}')
			return
		}
		for r in recs {
			v := r.value or { []u8{} }
			if !v.bytestr().starts_with('post-') {
				fail('unexpected replay of ${v.bytestr()} (commit not honored)')
			}
			post++
		}
		if post >= 4 {
			break
		}
		time.sleep(100 * time.millisecond)
	}
	if post != 4 {
		fail('survivor consumed ${post}/4 post-rebalance records')
	}
	g1.close()
	println('- survivor consumed exactly the 4 new records (commits honored)')
	println('GROUP SMOKE ALL OK')
}
