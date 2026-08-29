// Admin smoke against a real broker: full topic lifecycle, configs,
// group inspection, offsets/lag, and record truncation.
//
//	v run examples/admin_smoke.v [broker:port]
module main

import kadm
import kgo
import os
import rand
import time

fn fail(msg string) {
	eprintln('ADMIN SMOKE FAIL: ${msg}')
	exit(1)
}

fn main() {
	addr := if os.args.len > 1 { os.args[1] } else { '127.0.0.1:9092' }
	run := rand.intn(1000000) or { 0 }
	topic := 'franzv-adm-${run}'
	group := 'franzv-adm-grp-${run}'
	println('admin smoke against ${addr}')

	mut c := kgo.new_client(kgo.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('client: ${err.msg()}')
		return
	}
	defer {
		c.close()
	}
	mut a := kadm.new(c)

	// ---- topics ----
	results := a.create_topics([
		kadm.TopicSpec{
			topic:      topic
			partitions: 2
			configs:    {
				'retention.ms': '86400000'
			}
		},
	]) or {
		fail('create: ${err.msg()}')
		return
	}
	results[0].ok() or {
		fail('create result: ${err.msg()}')
		return
	}
	dup := a.create_topics([
		kadm.TopicSpec{
			topic: topic
		},
	]) or {
		fail('dup create: ${err.msg()}')
		return
	}
	if dup[0].code != 36 {
		fail('duplicate create: code ${dup[0].code}, want 36')
	}
	topics := a.list_topics(false) or {
		fail('list: ${err.msg()}')
		return
	}
	mut found := false
	for t in topics {
		if t.topic == topic {
			found = true
			if t.partitions.len != 2 {
				fail('listed ${t.partitions.len} partitions, want 2')
			}
			if t.topic_id == [16]u8{} {
				fail('listed topic has zero uuid')
			}
		}
	}
	if !found {
		fail('created topic missing from listing')
	}
	println('- topics: create (+already-exists), list with placement + uuid')

	// ---- configs ----
	cfgs := a.describe_topic_configs(topic) or {
		fail('describe configs: ${err.msg()}')
		return
	}
	mut retention := ''
	for e in cfgs {
		if e.name == 'retention.ms' {
			retention = e.value or { '' }
		}
	}
	if retention != '86400000' {
		fail('retention.ms = ${retention}')
	}
	a.alter_topic_configs(topic, {
		'cleanup.policy': 'compact'
	}, []) or {
		fail('alter configs: ${err.msg()}')
		return
	}
	cfgs2 := a.describe_topic_configs(topic) or {
		fail('describe 2: ${err.msg()}')
		return
	}
	mut policy := ''
	for e in cfgs2 {
		if e.name == 'cleanup.policy' {
			policy = e.value or { '' }
		}
	}
	if policy != 'compact' {
		fail('cleanup.policy = ${policy}')
	}
	a.alter_topic_configs(topic, {
		'cleanup.policy': 'delete'
	}, []) or {
		fail('alter back: ${err.msg()}')
		return
	}
	println('- configs: created value visible, incremental alter round-trip')

	// ---- partitions ----
	a.create_partitions(topic, 4) or {
		fail('create partitions: ${err.msg()}')
		return
	}
	mut grown := false
	for _ in 0 .. 20 {
		lt := a.list_topics(false) or {
			fail('list: ${err.msg()}')
			return
		}
		for t in lt {
			if t.topic == topic && t.partitions.len == 4 {
				grown = true
			}
		}
		if grown {
			break
		}
		time.sleep(250 * time.millisecond)
	}
	if !grown {
		fail('partition growth not visible')
	}
	println('- partitions: grew 2 -> 4')

	// ---- group activity for inspection ----
	mut records := []kgo.Record{}
	for i in 0 .. 8 {
		records << kgo.Record{
			partition: i % 4
			value:     'v${i}'.bytes()
		}
	}
	c.produce(topic, mut records) or {
		fail('produce: ${err.msg()}')
		return
	}
	mut gc := kgo.new_client(kgo.Config{
		seed_brokers:   [addr]
		fetch_max_wait: 300 * time.millisecond
	}) or {
		fail('group client: ${err.msg()}')
		return
	}
	defer {
		gc.close()
	}
	mut g := gc.new_group_consumer(group, [topic], kgo.GroupOpts{
		heartbeat_interval: 300 * time.millisecond
	}) or {
		fail('group: ${err.msg()}')
		return
	}
	mut consumed := 0
	for _ in 0 .. 60 {
		consumed += (g.poll() or {
			fail('poll: ${err.msg()}')
			return
		}).len
		if consumed >= 8 {
			break
		}
		time.sleep(150 * time.millisecond)
	}
	if consumed != 8 {
		fail('consumed ${consumed}/8')
	}
	g.commit() or {
		fail('commit: ${err.msg()}')
		return
	}

	groups := a.list_groups() or {
		fail('list groups: ${err.msg()}')
		return
	}
	mut glisted := false
	for gl in groups {
		if gl.group == group {
			glisted = true
		}
	}
	if !glisted {
		fail('group missing from listing')
	}
	desc := a.describe_group(group) or {
		fail('describe group: ${err.msg()}')
		return
	}
	if desc.state != 'Stable' || desc.members.len != 1 {
		fail('describe: state ${desc.state}, members ${desc.members.len}')
	}
	if desc.members[0].assigned[topic].len != 4 {
		fail('member assignment: ${desc.members[0].assigned}')
	}
	println('- groups: listed, described (Stable, 1 member, 4 partitions)')

	// ---- offsets and lag ----
	offsets := a.fetch_group_offsets(group) or {
		fail('fetch offsets: ${err.msg()}')
		return
	}
	if offsets['${topic}/0'] != 2 {
		fail('committed ${topic}/0 = ${offsets['${topic}/0']}, want 2')
	}
	lag0 := a.group_lag(group) or {
		fail('lag: ${err.msg()}')
		return
	}
	mut total0 := i64(0)
	for l in lag0 {
		if l.topic == topic {
			total0 += l.lag
		}
	}
	if total0 != 0 {
		fail('expected zero lag, got ${total0}')
	}
	mut more := []kgo.Record{}
	for i in 0 .. 4 {
		more << kgo.Record{
			partition: i
			value:     'late'.bytes()
		}
	}
	c.produce(topic, mut more) or {
		fail('produce more: ${err.msg()}')
		return
	}
	lag1 := a.group_lag(group) or {
		fail('lag 2: ${err.msg()}')
		return
	}
	mut total1 := i64(0)
	for l in lag1 {
		if l.topic == topic {
			total1 += l.lag
		}
	}
	if total1 != 4 {
		fail('expected lag 4, got ${total1}')
	}
	println('- offsets: committed correct; lag 0 -> 4 after producing')

	// ---- delete records ----
	low := a.delete_records(topic, 0, 2) or {
		fail('delete records: ${err.msg()}')
		return
	}
	if low != 2 {
		fail('low watermark ${low}, want 2')
	}
	starts := a.list_start_offsets([topic]) or {
		fail('starts: ${err.msg()}')
		return
	}
	if starts['${topic}/0'] != 2 {
		fail('log start ${starts['${topic}/0']}, want 2')
	}
	println('- delete records: partition 0 truncated to offset 2')

	// ---- teardown: live group protected, then deleted; topic deleted ----
	busy := a.delete_groups([group]) or {
		fail('delete busy group: ${err.msg()}')
		return
	}
	if busy[0].code != 68 {
		fail('live group delete: code ${busy[0].code}, want 68')
	}
	g.close()
	time.sleep(500 * time.millisecond)
	mut gone := false
	for _ in 0 .. 20 {
		res := a.delete_groups([group]) or {
			fail('delete group: ${err.msg()}')
			return
		}
		if res[0].code == 0 {
			gone = true
			break
		}
		time.sleep(250 * time.millisecond)
	}
	if !gone {
		fail('group never became deletable')
	}
	del := a.delete_topics([topic]) or {
		fail('delete topic: ${err.msg()}')
		return
	}
	del[0].ok() or {
		fail('delete topic result: ${err.msg()}')
		return
	}
	println('- teardown: live group rejected (68), deleted after leave; topic deleted')
	println('ADMIN SMOKE ALL OK')
}
