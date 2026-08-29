// Cooperative-sticky smoke against a real broker: a second member joins,
// the group converges via KIP-429 incremental rebalancing (revoke round +
// placement round), the first member retains part of its assignment with
// cursors intact (zero duplicate consumption despite never committing),
// and the freed partitions land on the joiner.
//
//	v run examples/cooperative_smoke.v [broker:port]
module main

import kgo
import kmsg
import os
import rand
import time

fn fail(msg string) {
	eprintln('COOP SMOKE FAIL: ${msg}')
	exit(1)
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:9092' }
	run := rand.intn(1000000) or { 0 }
	topic := 'franzv-coop-${run}'
	group := 'franzv-coop-team-${run}'
	println('cooperative smoke against ${addr} (topic ${topic})')

	mut c1 := kgo.new_client(kgo.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('client 1: ${err.msg()}')
		return
	}
	defer {
		c1.close()
	}
	mut c2 := kgo.new_client(kgo.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('client 2: ${err.msg()}')
		return
	}
	defer {
		c2.close()
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
	c1.request(mut creq) or {}
	// freshly created topics take a moment to get leaders
	mut ready := false
	for _ in 0 .. 40 {
		c1.metadata([topic]) or {}
		if n := c1.partition_count(topic) {
			if n == 4 {
				ready = true
				break
			}
		}
		time.sleep(250 * time.millisecond)
	}
	if !ready {
		fail('topic never became ready')
	}

	mut old := []kgo.Record{}
	for p in 0 .. 4 {
		old << kgo.Record{
			partition: p
			value:     'old-p${p}'.bytes()
		}
	}
	c1.produce(topic, mut old) or {
		fail('produce old: ${err.msg()}')
		return
	}

	coop := kgo.GroupOpts{
		balancers:          [kgo.BalancerKind.cooperative_sticky]
		heartbeat_interval: 300 * time.millisecond
		session_timeout:    10 * time.second
	}
	mut g1 := c1.new_group_consumer(group, [topic], coop) or {
		fail('g1: ${err.msg()}')
		return
	}
	mut first := []kgo.Record{}
	deadline0 := time.now().unix_milli() + 30000
	for time.now().unix_milli() < deadline0 {
		first << g1.poll() or {
			fail('g1 solo poll: ${err.msg()}')
			return
		}
		if first.len >= 4 {
			break
		}
		time.sleep(150 * time.millisecond)
	}
	if first.len != 4 {
		fail('solo phase consumed ${first.len}/4')
	}
	println('- solo member consumed all 4 partitions (no commits)')

	// both members now run in their own threads (real deployments poll
	// each member concurrently; joins are coordinator-held barriers)
	mut g2 := c2.new_group_consumer(group, [topic], coop) or {
		fail('g2: ${err.msg()}')
		return
	}
	stop := kgo.new_cancel()
	status := chan PollStatus{cap: 64}
	spawn member_loop(1, mut g1, stop, status)
	spawn member_loop(2, mut g2, stop, status)

	mut asg := map[int][]int{}
	mut recs := map[int][]kgo.Record{}
	recs[1] = []
	recs[2] = []
	deadline := time.now().unix_milli() + 60000
	mut settled := false
	for time.now().unix_milli() < deadline {
		st := <-status
		asg[st.who] = st.assigned.clone()
		recs[st.who] << st.records
		if asg[1].len == 2 && asg[2].len == 2 {
			settled = true
			break
		}
	}
	if !settled {
		stop.cancel()
		fail('cooperative convergence failed: g1=${asg[1]} g2=${asg[2]}')
	}
	mut seen := map[int]bool{}
	for p in asg[1] {
		seen[p] = true
	}
	for p in asg[2] {
		if p in seen {
			stop.cancel()
			fail('partition ${p} double-assigned')
		}
		seen[p] = true
	}
	if seen.len != 4 {
		stop.cancel()
		fail('coverage ${seen.len}/4')
	}
	println('- converged incrementally: g1=${asg[1]} g2=${asg[2]}')

	// continuity: one new record per partition; g1 must yield ONLY new
	// records (cursors kept without commits); g2 re-reads its 2 old + 2 new
	mut fresh := []kgo.Record{}
	for p in 0 .. 4 {
		fresh << kgo.Record{
			partition: p
			value:     'new-p${p}'.bytes()
		}
	}
	c1.produce(topic, mut fresh) or {
		stop.cancel()
		fail('produce new: ${err.msg()}')
		return
	}
	deadline2 := time.now().unix_milli() + 30000
	for time.now().unix_milli() < deadline2 {
		st := <-status
		asg[st.who] = st.assigned.clone()
		recs[st.who] << st.records
		mut n1 := 0
		for r in recs[1] {
			v := r.value or { []u8{} }
			if v.bytestr().starts_with('new-') {
				n1++
			} else {
				stop.cancel()
				fail('g1 re-consumed ${v.bytestr()} — cursors lost')
			}
		}
		if n1 >= 2 && recs[2].len >= 4 {
			break
		}
	}
	stop.cancel()
	news1 := recs[1].len
	if news1 != 2 {
		fail('g1 expected exactly 2 new records, got ${news1}')
	}
	if recs[2].len != 4 {
		fail('g2 expected 4 (2 old + 2 new), got ${recs[2].len}')
	}
	println('- continuity: g1 zero duplicates without commits; g2 recovered its history')
	println('COOP SMOKE ALL OK')
}

struct PollStatus {
	who      int
	assigned []int
	records  []kgo.Record
}

fn member_loop(who int, mut g kgo.GroupConsumer, stop &kgo.Cancel, status chan PollStatus) {
	topic := g.topics[0]
	for !stop.is_done() {
		records := g.poll() or {
			time.sleep(200 * time.millisecond)
			continue
		}
		st := PollStatus{
			who:      who
			assigned: g.assigned()[topic] or { []int{} }
			records:  records
		}
		select {
			status <- st {}
			else {}
		}
		time.sleep(100 * time.millisecond)
	}
	g.close()
}
