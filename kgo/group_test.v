module kgo

import kfake
import time

const fast = GroupOpts{
	heartbeat_interval: 30 * time.millisecond
}

fn test_balancers() {
	subs := [
		MemberSubscription{
			member_id: 'a'
			topics:    ['t']
		},
		MemberSubscription{
			member_id: 'b'
			topics:    ['t']
		},
	]
	counts := {
		't': 5
	}
	r := balance(.range_bal, subs, counts)
	// range: 5 partitions over 2 -> first member gets 3
	assert r['a']['t'] == [0, 1, 2]
	assert r['b']['t'] == [3, 4]

	rr := balance(.round_robin, subs, counts)
	assert rr['a']['t'] == [0, 2, 4]
	assert rr['b']['t'] == [1, 3]

	// range with a topic only one member subscribes to
	subs2 := [
		MemberSubscription{
			member_id: 'a'
			topics:    ['t', 'u']
		},
		MemberSubscription{
			member_id: 'b'
			topics:    ['t']
		},
	]
	counts2 := {
		't': 2
		'u': 2
	}
	r2 := balance(.range_bal, subs2, counts2)
	assert r2['a']['t'] == [0]
	assert r2['b']['t'] == [1]
	assert r2['a']['u'] == [0, 1]
	assert ('u' in r2['b']) == false
}

fn test_group_single_member_lifecycle() {
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic: 2
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
	for i in 0 .. 6 {
		records << Record{
			partition: i % 2
			value:     'v${i}'.bytes()
		}
	}
	c.produce('events', mut records) or {
		assert false, '${err}'
		return
	}

	mut g := c.new_group_consumer('workers', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	recs := g.poll() or {
		assert false, 'poll: ${err}'
		return
	}
	assert recs.len == 6
	asg := g.assigned()
	assert asg['events'].len == 2 // sole member owns both partitions
	assert g.member().starts_with('kfake-member-')

	g.commit() or {
		assert false, 'commit: ${err}'
		return
	}
	g.close()

	// a fresh member of the same group resumes at the committed offsets
	mut g2 := c.new_group_consumer('workers', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	none_new := g2.poll() or {
		assert false, '${err}'
		return
	}
	assert none_new.len == 0

	mut more := [
		Record{
			partition: 0
			value:     'post-commit'.bytes()
		},
	]
	c.produce('events', mut more) or {
		assert false, '${err}'
		return
	}
	got := g2.poll() or {
		assert false, '${err}'
		return
	}
	assert got.len == 1
	v := got[0].value or { []u8{} }
	assert v.bytestr() == 'post-commit'
	g2.close()
}

fn test_group_two_members_rebalance() {
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic: 2
	})
	mut c := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c.close()
	}
	// one client per member: members share nothing, matching real
	// coordinator semantics (serial per-connection processing)
	mut cm2 := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		cm2.close()
	}

	mut g1 := c.new_group_consumer('team', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	g1.poll() or {
		assert false, 'g1 first poll: ${err}'
		return
	}
	assert g1.assigned()['events'].len == 2

	// second member joins: coordinator bumps the generation; g1's next
	// heartbeat sees REBALANCE_IN_PROGRESS and rejoins; leader rebalances
	mut g2 := cm2.new_group_consumer('team', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	// g2's first join blocks in sync until g1 rejoins and (as leader)
	// submits assignments — drive both from this thread: spawn g2's poll
	done := chan bool{cap: 1}
	spawn fn (mut g2 GroupConsumer, done chan bool) {
		g2.poll() or {}
		done <- true
	}(mut g2, done)

	mut settled := false
	for _ in 0 .. 100 {
		time.sleep(40 * time.millisecond)
		g1.poll() or {
			assert false, 'g1 poll during rebalance: ${err}'
			return
		}
		if g1.assigned()['events'].len == 1 {
			settled = true
			break
		}
	}
	assert settled, 'rebalance never settled'
	_ := <-done
	assert g2.assigned()['events'].len == 1
	// disjoint, covering split
	p1 := g1.assigned()['events'][0]
	p2 := g2.assigned()['events'][0]
	assert p1 != p2
	assert (p1 == 0 && p2 == 1) || (p1 == 1 && p2 == 0)

	// records to each partition arrive at exactly one member
	mut records := [
		Record{
			partition: 0
			value:     'to-p0'.bytes()
		},
		Record{
			partition: 1
			value:     'to-p1'.bytes()
		},
	]
	c.produce('events', mut records) or {
		assert false, '${err}'
		return
	}
	mut got1 := []Record{}
	mut got2 := []Record{}
	for _ in 0 .. 40 {
		got1 << g1.poll() or {
			assert false, '${err}'
			return
		}
		got2 << g2.poll() or {
			assert false, '${err}'
			return
		}
		if got1.len + got2.len >= 2 {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	assert got1.len == 1
	assert got2.len == 1
	assert got1[0].partition == p1
	assert got2[0].partition == p2

	// one member leaves: the survivor reclaims both partitions
	g2.close()
	mut reclaimed := false
	for _ in 0 .. 100 {
		time.sleep(40 * time.millisecond)
		g1.poll() or {
			assert false, '${err}'
			return
		}
		if g1.assigned()['events'].len == 2 {
			reclaimed = true
			break
		}
	}
	assert reclaimed, 'survivor never reclaimed both partitions'
	g1.close()
}

fn test_group_follower_refreshes_stale_metadata() {
	// Both clients cache metadata when their consumer is created, and the
	// broker still lists the topic without partitions (as right after
	// CreateTopics). The leader refreshes before balancing; the follower
	// must also refresh before positioning its partitions rather than
	// fail on its stale cache (seen against Kafka 4.3.1 in
	// examples/group_smoke.v).
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic:   2
		pending_topic_metadata: 2
	})
	mut c1 := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c1.close()
	}
	mut c2 := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c2.close()
	}
	mut g1 := c1.new_group_consumer('stale', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	mut g2 := c2.new_group_consumer('stale', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	// g1 joins alone and leads
	g1.poll() or {
		assert false, 'g1 first poll: ${err}'
		return
	}
	assert g1.assigned()['events'].len == 2

	// g2 follows: it is handed a partition its cache knows nothing about
	result := chan string{cap: 1}
	spawn fn (mut g2 GroupConsumer, result chan string) {
		g2.poll() or {
			result <- err.msg()
			return
		}
		result <- ''
	}(mut g2, result)
	mut settled := false
	for _ in 0 .. 100 {
		time.sleep(40 * time.millisecond)
		g1.poll() or {
			assert false, 'g1 poll during rebalance: ${err}'
			return
		}
		if g1.assigned()['events'].len == 1 {
			settled = true
			break
		}
	}
	assert settled, 'rebalance never settled'
	g2_err := <-result
	assert g2_err == '', 'g2 first poll: ${g2_err}'
	assert g2.assigned()['events'].len == 1
	g2.poll() or {
		assert false, 'g2 poll: ${err}'
		return
	}
	g2.close()
	g1.close()
}

fn test_cooperative_sticky_balancer() {
	// sole member owns everything, balanced: nothing moves
	solo := [
		MemberSubscription{
			member_id:  'a'
			topics:     ['t']
			owned:      {
				't': [0, 1, 2, 3]
			}
			generation: 1
		},
	]
	counts := {
		't': 4
	}
	r := balance(.cooperative_sticky, solo, counts)
	assert r['a']['t'] == [0, 1, 2, 3]

	// second member joins: round 1 revokes the excess to nobody
	two := [
		MemberSubscription{
			member_id:  'a'
			topics:     ['t']
			owned:      {
				't': [0, 1, 2, 3]
			}
			generation: 1
		},
		MemberSubscription{
			member_id: 'b'
			topics:    ['t']
		},
	]
	r1 := balance(.cooperative_sticky, two, counts)
	assert r1['a']['t'] == [0, 1] // sticky keep, capped at target
	assert ('t' in r1['b']) == false // cooperative: not handed over yet
	// round 2: a rejoins owning only its kept share; freed parts land on b
	two2 := [
		MemberSubscription{
			member_id:  'a'
			topics:     ['t']
			owned:      {
				't': [0, 1]
			}
			generation: 2
		},
		MemberSubscription{
			member_id:  'b'
			topics:     ['t']
			generation: 2
		},
	]
	r2 := balance(.cooperative_sticky, two2, counts)
	assert r2['a']['t'] == [0, 1]
	assert r2['b']['t'] == [2, 3]

	// duplicate claim: higher generation wins
	dup := [
		MemberSubscription{
			member_id:  'stale'
			topics:     ['t']
			owned:      {
				't': [0]
			}
			generation: 1
		},
		MemberSubscription{
			member_id:  'fresh'
			topics:     ['t']
			owned:      {
				't': [0]
			}
			generation: 3
		},
	]
	rd := balance(.cooperative_sticky, dup, {
		't': 1
	})
	assert rd['fresh']['t'] == [0]
	assert ('t' in rd['stale']) == false
}

struct CoopStatus {
	who      int
	assigned []int
	records  []Record
}

fn coop_member_loop(who int, mut g GroupConsumer, mut stop Cancel, status chan CoopStatus) {
	for !stop.is_done() {
		records := g.poll() or {
			time.sleep(30 * time.millisecond)
			continue
		}
		st := CoopStatus{
			who:      who
			assigned: g.assigned()['events'] or { []int{} }
			records:  records
		}
		select {
			status <- st {
			}
			else {
			}
		}
		time.sleep(20 * time.millisecond)
	}
	g.close()
}

fn test_cooperative_group_rebalance_preserves_cursors() {
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic: 4
	})
	coop := GroupOpts{
		balancers:          [BalancerKind.cooperative_sticky]
		heartbeat_interval: 30 * time.millisecond
	}
	mut c1 := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c1.close()
	}
	mut c2 := new_client(Config{
		seed_brokers:   [cl.seed_addr()]
		fetch_max_wait: 50 * time.millisecond
	}) or {
		assert false, '${err}'
		return
	}
	defer {
		c2.close()
	}

	mut records := []Record{}
	for p in 0 .. 4 {
		records << Record{
			partition: p
			value:     'old-p${p}'.bytes()
		}
	}
	c1.produce('events', mut records) or {
		assert false, '${err}'
		return
	}

	// solo phase: g1 owns and consumes everything, never committing
	mut g1 := c1.new_group_consumer('coop', ['events'], coop) or {
		assert false, '${err}'
		return
	}
	mut first := []Record{}
	for _ in 0 .. 40 {
		first << g1.poll() or {
			assert false, '${err}'
			return
		}
		if first.len >= 4 {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	assert first.len == 4
	assert g1.assigned()['events'].len == 4

	// concurrent phase: both members polled by their own worker threads
	mut g2 := c2.new_group_consumer('coop', ['events'], coop) or {
		assert false, '${err}'
		return
	}
	mut stop := new_cancel()
	status := chan CoopStatus{cap: 64}
	spawn coop_member_loop(1, mut g1, mut stop, status)
	spawn coop_member_loop(2, mut g2, mut stop, status)

	mut asg := map[int][]int{}
	mut got := map[int][]Record{}
	got[1] = []
	got[2] = []
	mut settled := false
	for _ in 0 .. 600 {
		st := <-status
		asg[st.who] = st.assigned.clone()
		got[st.who] << st.records
		if asg[1].len == 2 && asg[2].len == 2 {
			settled = true
			break
		}
	}
	if !settled {
		stop.cancel()
	}
	assert settled, 'cooperative convergence failed: g1=${asg[1]} g2=${asg[2]}'
	mut seen := map[int]bool{}
	for p in asg[1] {
		seen[p] = true
	}
	for p in asg[2] {
		assert p !in seen
		seen[p] = true
	}
	assert seen.len == 4
	// sticky proof: g1 retained its lowest owned partitions
	for p in asg[1] {
		assert p in [0, 1], 'sticky keep should retain lowest owned, got ${p}'
	}

	// continuity: new record per partition; without any commits, g1 must
	// yield only new records (cursors preserved); g2 re-reads its history
	mut fresh := []Record{}
	for p in 0 .. 4 {
		fresh << Record{
			partition: p
			value:     'new-p${p}'.bytes()
		}
	}
	c1.produce('events', mut fresh) or {
		stop.cancel()
		assert false, '${err}'
		return
	}
	mut done := false
	for _ in 0 .. 600 {
		st := <-status
		asg[st.who] = st.assigned.clone()
		got[st.who] << st.records
		if got[1].len >= 2 && got[2].len >= 4 {
			done = true
			break
		}
	}
	stop.cancel()
	assert done, 'continuity phase timed out: g1=${got[1].len} g2=${got[2].len}'
	assert got[1].len == 2, 'g1 expected 2 new records, got ${got[1].len}'
	for r in got[1] {
		v := r.value or { []u8{} }
		assert v.bytestr().starts_with('new-'), 'g1 re-consumed ${v.bytestr()}'
	}
	assert got[2].len == 4, 'g2 expected 4 (2 old + 2 new), got ${got[2].len}'
}

fn test_group_first_poll_waits_for_a_leader_that_is_not_ready() {
	// A member's first poll resolves start offsets for its uncommitted
	// partitions; a leader briefly refusing ListOffsets, as just after
	// the topic was created, must not fail the poll. (A failed first
	// poll leaves the member to rejoin on the next one, which can stall
	// other members of the group polled from the same thread.)
	mut cl := kfake.start(1, kfake.ClusterCfg{
		partitions_per_topic:    2
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
	mut records := []Record{}
	for i in 0 .. 4 {
		records << Record{
			partition: i % 2
			value:     'v${i}'.bytes()
		}
	}
	c.produce('events', mut records) or {
		assert false, '${err}'
		return
	}
	mut g := c.new_group_consumer('starters', ['events'], fast) or {
		assert false, '${err}'
		return
	}
	recs := g.poll() or {
		assert false, 'first poll: ${err}'
		return
	}
	assert recs.len == 4
	g.close()
}
