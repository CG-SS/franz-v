module kadm

import kfake
import kgo
import time

fn test_topic_lifecycle_and_configs() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := kgo.new_client(kgo.Config{
		seed_brokers: [cl.seed_addr()]
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut a := new(c)

	// create two topics, one with a config
	results := a.create_topics([
		TopicSpec{
			topic:      'orders'
			partitions: 3
		},
		TopicSpec{
			topic:   'audit'
			configs: {
				'retention.ms': '86400000'
			}
		},
	]) or {
		assert false, '${err}'
		return
	}
	assert results.len == 2
	for r in results {
		r.ok() or {
			assert false, '${err}'
			return
		}
	}
	// duplicate create reports TOPIC_ALREADY_EXISTS
	dup := a.create_topics([
		TopicSpec{
			topic: 'orders'
		},
	]) or {
		assert false, '${err}'
		return
	}
	assert dup[0].code == 36

	// listing shows both with partition detail
	topics := a.list_topics(false) or {
		assert false, '${err}'
		return
	}
	assert topics.len == 2
	assert topics[0].topic == 'audit'
	assert topics[1].topic == 'orders'
	assert topics[1].partitions.len == 3
	assert topics[1].topic_id != [16]u8{}

	// grow partitions
	a.create_partitions('orders', 5) or {
		assert false, '${err}'
		return
	}
	after := a.list_topics(false) or {
		assert false, '${err}'
		return
	}
	assert after[1].partitions.len == 5
	// shrinking is invalid
	mut shrank := false
	a.create_partitions('orders', 2) or { shrank = true }
	assert shrank

	// configs: created value visible, alter set + delete
	cfgs := a.describe_topic_configs('audit') or {
		assert false, '${err}'
		return
	}
	mut retention := ?string(none)
	for e in cfgs {
		if e.name == 'retention.ms' {
			retention = e.value
		}
	}
	rv := retention or {
		assert false, 'retention.ms missing'
		return
	}

	assert rv == '86400000'

	a.alter_topic_configs('audit', {
		'cleanup.policy': 'compact'
	}, ['retention.ms']) or {
		assert false, '${err}'
		return
	}
	cfgs2 := a.describe_topic_configs('audit') or {
		assert false, '${err}'
		return
	}
	mut policy := ''
	mut retention_gone := true
	for e in cfgs2 {
		if e.name == 'cleanup.policy' {
			policy = e.value or { '' }
			assert !e.is_default
		}
		if e.name == 'retention.ms' {
			retention_gone = false
		}
	}
	assert policy == 'compact'
	assert retention_gone

	// delete: gone from listing, unknown on re-delete
	del := a.delete_topics(['audit']) or {
		assert false, '${err}'
		return
	}
	del[0].ok() or {
		assert false, '${err}'
		return
	}
	assert (a.list_topics(false) or {
		assert false, '${err}'
		return
	}).len == 1
	del2 := a.delete_topics(['audit']) or {
		assert false, '${err}'
		return
	}
	assert del2[0].code == 3
}

fn test_groups_offsets_and_lag() {
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic: 2
	})
	mut c := kgo.new_client(kgo.Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut a := new(c)

	// produce 6 across 2 partitions, consume 6, commit
	mut records := []kgo.Record{}
	for i in 0 .. 6 {
		records << kgo.Record{
			partition: i % 2
			value:     'v${i}'.bytes()
		}
	}
	c.produce('events', mut records) or {
		assert false, '${err}'
		return
	}
	mut gc := kgo.new_client(kgo.Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		gc.close()
	}
	mut g := gc.new_group_consumer('workers', ['events'], kgo.GroupOpts{
		heartbeat_interval: 30 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	consumed := g.poll() or {
		assert false, '${err}'
		return
	}
	assert consumed.len == 6
	g.commit() or {
		assert false, '${err}'
		return
	}

	// list + describe while the member is live
	groups := a.list_groups() or {
		assert false, '${err}'
		return
	}
	assert groups.len == 1
	assert groups[0].group == 'workers'
	assert groups[0].state == 'Stable'
	assert groups[0].protocol_type == 'consumer'

	desc := a.describe_group('workers') or {
		assert false, '${err}'
		return
	}
	assert desc.state == 'Stable'
	assert desc.protocol == 'range'
	assert desc.members.len == 1
	assert desc.members[0].assigned['events'].len == 2

	// committed offsets and zero lag
	offsets := a.fetch_group_offsets('workers') or {
		assert false, '${err}'
		return
	}
	assert offsets['events/0'] == 3
	assert offsets['events/1'] == 3
	lag0 := a.group_lag('workers') or {
		assert false, '${err}'
		return
	}
	assert lag0.len == 2
	for l in lag0 {
		assert l.lag == 0
		assert l.end == 3
	}

	// produce 2 more without consuming: lag 1 per partition
	mut more := []kgo.Record{}
	for i in 0 .. 2 {
		more << kgo.Record{
			partition: i
			value:     'late'.bytes()
		}
	}
	c.produce('events', mut more) or {
		assert false, '${err}'
		return
	}
	lag1 := a.group_lag('workers') or {
		assert false, '${err}'
		return
	}
	for l in lag1 {
		assert l.lag == 1, '${l.topic}[${l.partition}] lag ${l.lag}'
		assert l.committed == 3
		assert l.end == 4
	}

	// deleting a live group is rejected; after leave it works
	busy := a.delete_groups(['workers']) or {
		assert false, '${err}'
		return
	}
	assert busy[0].code == 68 // NON_EMPTY_GROUP
	g.close()
	deleted := a.delete_groups(['workers']) or {
		assert false, '${err}'
		return
	}
	deleted[0].ok() or {
		assert false, '${err}'
		return
	}
	missing := a.delete_groups(['workers']) or {
		assert false, '${err}'
		return
	}
	assert missing[0].code == 69 // GROUP_ID_NOT_FOUND
}

fn test_delete_records_truncation() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := kgo.new_client(kgo.Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut a := new(c)

	mut records := []kgo.Record{}
	for i in 0 .. 5 {
		records << kgo.Record{
			value: 'r${i}'.bytes()
		}
	}
	c.produce('logs', mut records) or {
		assert false, '${err}'
		return
	}

	low := a.delete_records('logs', 0, 3) or {
		assert false, '${err}'
		return
	}
	assert low == 3
	starts := a.list_start_offsets(['logs']) or {
		assert false, '${err}'
		return
	}
	assert starts['logs/0'] == 3
	ends := a.list_end_offsets(['logs']) or {
		assert false, '${err}'
		return
	}
	assert ends['logs/0'] == 5

	// a consumer from earliest now starts at the truncation point
	mut co := c.new_consumer(['logs'], kgo.ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	pos := co.position('logs', 0) or { i64(-1) }
	assert pos == 3
	recs := co.poll() or {
		assert false, '${err}'
		return
	}
	assert recs.len == 2
	v := recs[0].value or { []u8{} }
	assert v.bytestr() == 'r3'
	assert recs[0].offset == 3
}
