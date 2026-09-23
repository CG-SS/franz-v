module kgo

import kfake
import time

fn produce_n(mut c Client, topic string, n int, prefix string) []Record {
	mut records := []Record{}
	for i in 0 .. n {
		records << Record{
			key:   '${prefix}-k${i}'.bytes()
			value: '${prefix}-v${i}'.bytes()
		}
	}
	c.produce(topic, mut records) or { panic('produce: ${err}') }
	return records
}

fn test_consume_from_earliest_and_advance() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'events', 5, 'a')

	mut co := c.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	pos := co.position('events', 0) or { i64(-99) }
	assert pos == 0

	recs := co.poll() or {
		assert false, 'poll: ${err}'
		return
	}
	assert recs.len == 5
	for i, r in recs {
		k := r.key or { []u8{} }
		v := r.value or { []u8{} }
		assert k.bytestr() == 'a-k${i}'
		assert v.bytestr() == 'a-v${i}'
		assert r.topic == 'events'
		assert r.partition == 0
		assert r.offset == i
	}
	pos2 := co.position('events', 0) or { i64(-99) }
	assert pos2 == 5

	// nothing new: empty poll
	empty := co.poll() or {
		assert false, '${err}'
		return
	}
	assert empty.len == 0

	// produce more; only the new ones arrive
	produce_n(mut c, 'events', 2, 'b')
	more := co.poll() or {
		assert false, '${err}'
		return
	}
	assert more.len == 2
	mk := more[0].key or { []u8{} }
	assert mk.bytestr() == 'b-k0'
	assert more[0].offset == 5
}

fn test_consume_from_latest() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'events', 3, 'old')

	mut co := c.new_consumer(['events'], ConsumerOpts{
		start: .latest
	}) or {
		assert false, '${err}'
		return
	}
	pos := co.position('events', 0) or { i64(-99) }
	assert pos == 3
	assert (co.poll() or {
		assert false, '${err}'
		return
	}).len == 0

	produce_n(mut c, 'events', 2, 'new')
	recs := co.poll() or {
		assert false, '${err}'
		return
	}
	assert recs.len == 2
	k := recs[0].key or { []u8{} }
	assert k.bytestr() == 'new-k0'
}

fn test_consume_multi_partition_multi_leader() {
	mut cl := kfake.start(3, kfake.ClusterCfg{
		partitions_per_topic: 6
	})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	mut records := []Record{}
	for p in 0 .. 6 {
		for i in 0 .. 3 {
			records << Record{
				partition: p
				value:     'p${p}-i${i}'.bytes()
			}
		}
	}
	c.produce('spread', mut records) or {
		assert false, '${err}'
		return
	}

	mut co := c.new_consumer(['spread'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	recs := co.poll() or {
		assert false, '${err}'
		return
	}
	assert recs.len == 18
	mut per_part := map[int]int{}
	for r in recs {
		per_part[r.partition]++
	}
	assert per_part.len == 6
	for p in 0 .. 6 {
		assert per_part[p] == 3, 'partition ${p}'
		pos := co.position('spread', p) or { i64(-99) }
		assert pos == 3
	}
}

fn test_seek_replays() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'events', 4, 'x')
	mut co := c.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	first := co.poll() or {
		assert false, '${err}'
		return
	}
	assert first.len == 4

	co.seek('events', 0, 2)
	replay := co.poll() or {
		assert false, '${err}'
		return
	}
	assert replay.len == 2
	assert replay[0].offset == 2
	k := replay[0].key or { []u8{} }
	assert k.bytestr() == 'x-k2'
}

fn test_offset_out_of_range_resets() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'events', 3, 'r')
	mut co := c.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	// far beyond the log end: the poll resets per policy (earliest) and
	// the next poll replays from 0
	co.seek('events', 0, 999)
	first := co.poll() or {
		assert false, '${err}'
		return
	}
	assert first.len == 0
	pos := co.position('events', 0) or { i64(-99) }
	assert pos == 0
	replay := co.poll() or {
		assert false, '${err}'
		return
	}
	assert replay.len == 3
}

fn test_consume_compressed() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 100 * time.millisecond
		compression:    .gzip
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'zipped', 3, 'g')
	mut co := c.new_consumer(['zipped'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	recs := co.poll() or {
		assert false, '${err}'
		return
	}
	assert recs.len == 3
	v := recs[2].value or { []u8{} }
	assert v.bytestr() == 'g-v2'
}

fn test_start_offsets_wait_for_a_leader_that_is_not_ready() {
	// Right after a topic is created, its leader can answer ListOffsets
	// with NOT_LEADER_OR_FOLLOWER for a moment: the consumer retries
	// after a metadata refresh instead of failing to start.
	mut cl := kfake.start(1, kfake.ClusterCfg{
		not_leader_list_offsets: 2
	})
	mut c := new_client(Config{
		seed_brokers:      [cl.seed_addr()]
		fetch_max_wait:    100 * time.millisecond
		retry_backoff_min: 10 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	produce_n(mut c, 'events', 3, 's')
	mut co := c.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, 'consumer start: ${err}'
		return
	}
	recs := co.poll() or {
		assert false, 'poll: ${err}'
		return
	}
	assert recs.len == 3

	// a leader that never becomes ready still fails, once retries run out
	mut stuck := kfake.start(1, kfake.ClusterCfg{
		not_leader_list_offsets: 1000
	})
	mut c2 := new_client(Config{
		seed_brokers:      [stuck.seed_addr()]
		retry_backoff_min: 10 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c2.close()
	}
	c2.new_consumer(['events'], ConsumerOpts{}) or {
		assert err.msg().starts_with('list offsets events[0]: NOT_LEADER'), err.msg()
		return
	}
	assert false, 'consumer started without start offsets'
}
