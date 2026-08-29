module kgo

import kfake
import time

fn txn_client(cl &kfake.Cluster, iso IsolationLevel) &Client {
	return new_client(Config{
		seed_brokers:    [cl.seed_addr()]
		fetch_max_wait:  100 * time.millisecond
		isolation_level: iso
	}) or { panic('client: ${err}') }
}

fn test_txn_commit_and_abort_isolation() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := txn_client(cl, .read_uncommitted)
	defer {
		c.close()
	}
	mut cc := txn_client(cl, .read_committed)
	defer {
		cc.close()
	}

	mut t := c.new_txn_producer('tx-1') or {
		assert false, '${err}'
		return
	}
	pid, epoch := t.producer()
	assert pid >= 1000
	assert epoch == 0

	// ---- commit visibility ----
	t.begin() or {
		assert false, '${err}'
		return
	}
	mut committed := [
		Record{
			key:   'c1'.bytes()
			value: 'committed one'.bytes()
		},
		Record{
			value: 'committed two'.bytes()
		},
	]
	t.produce('events', mut committed) or {
		assert false, 'txn produce: ${err}'
		return
	}
	assert committed[0].offset == 0
	t.commit() or {
		assert false, 'commit: ${err}'
		return
	}

	mut co_c := cc.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	got := co_c.poll() or {
		assert false, '${err}'
		return
	}
	assert got.len == 2 // marker not returned
	v0 := got[0].value or { []u8{} }
	assert v0.bytestr() == 'committed one'

	// ---- abort isolation ----
	t.begin() or {
		assert false, '${err}'
		return
	}
	mut doomed := [
		Record{
			value: 'aborted one'.bytes()
		},
		Record{
			value: 'aborted two'.bytes()
		},
	]
	t.produce('events', mut doomed) or {
		assert false, '${err}'
		return
	}
	t.abort() or {
		assert false, 'abort: ${err}'
		return
	}
	// read_committed: nothing new
	hidden := co_c.poll() or {
		assert false, '${err}'
		return
	}
	assert hidden.len == 0, 'aborted records leaked to read_committed'

	// read_uncommitted from earliest: sees committed 2 + aborted 2
	mut cu := txn_client(cl, .read_uncommitted)
	defer {
		cu.close()
	}
	mut co_u := cu.new_consumer(['events'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	all := co_u.poll() or {
		assert false, '${err}'
		return
	}
	assert all.len == 4

	// ---- skip-over-aborted to the next commit ----
	t.begin() or {
		assert false, '${err}'
		return
	}
	mut third := [
		Record{
			value: 'committed three'.bytes()
		},
	]
	t.produce('events', mut third) or {
		assert false, '${err}'
		return
	}
	t.commit() or {
		assert false, '${err}'
		return
	}
	next := co_c.poll() or {
		assert false, '${err}'
		return
	}
	assert next.len == 1
	nv := next[0].value or { []u8{} }
	assert nv.bytestr() == 'committed three'
	// sequences continued across transactions of one producer session
	assert third[0].offset > committed[1].offset
}

fn test_txn_zombie_fencing() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c1 := txn_client(cl, .read_uncommitted)
	defer {
		c1.close()
	}
	mut c2 := txn_client(cl, .read_uncommitted)
	defer {
		c2.close()
	}

	mut old := c1.new_txn_producer('tx-fence') or {
		assert false, '${err}'
		return
	}
	_, epoch1 := old.producer()

	// re-init of the same transactional id bumps the epoch
	mut fresh := c2.new_txn_producer('tx-fence') or {
		assert false, '${err}'
		return
	}
	_, epoch2 := fresh.producer()
	assert epoch2 == epoch1 + 1

	old.begin() or {
		assert false, '${err}'
		return
	}
	mut zombie := [
		Record{
			value: 'from the zombie'.bytes()
		},
	]
	mut fenced := false
	old.produce('events', mut zombie) or {
		fenced = true
		assert err.msg().contains('PRODUCER_FENCED')
	}
	assert fenced, 'stale producer must be fenced'

	// the fresh producer works
	fresh.begin() or {
		assert false, '${err}'
		return
	}
	mut ok := [
		Record{
			value: 'from the fresh'.bytes()
		},
	]
	fresh.produce('events', mut ok) or {
		assert false, '${err}'
		return
	}
	fresh.commit() or {
		assert false, '${err}'
		return
	}
}

fn test_txn_eos_commit_and_abort() {
	mut cl := kfake.start(1, kfake.ClusterCfg{})
	mut c := txn_client(cl, .read_uncommitted)
	defer {
		c.close()
	}
	gopts := GroupOpts{
		heartbeat_interval: 30 * time.millisecond
	}

	// input records
	mut input := []Record{}
	for i in 0 .. 4 {
		input << Record{
			value: 'in-${i}'.bytes()
		}
	}
	c.produce('eos-in', mut input) or {
		assert false, '${err}'
		return
	}

	// consume via group (own client), transform, produce + offsets in txn
	mut gc := txn_client(cl, .read_committed)
	defer {
		gc.close()
	}
	mut g := gc.new_group_consumer('eos-grp', ['eos-in'], gopts) or {
		assert false, '${err}'
		return
	}
	mut consumed := []Record{}
	for _ in 0 .. 40 {
		consumed << g.poll() or {
			assert false, '${err}'
			return
		}
		if consumed.len >= 4 {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	assert consumed.len == 4

	mut tc := txn_client(cl, .read_uncommitted)
	defer {
		tc.close()
	}
	mut t := tc.new_txn_producer('eos-tx') or {
		assert false, '${err}'
		return
	}
	t.begin() or {
		assert false, '${err}'
		return
	}
	mut out := []Record{}
	for r in consumed {
		v := r.value or { []u8{} }
		out << Record{
			value: 'OUT(${v.bytestr()})'.bytes()
		}
	}
	t.produce('eos-out', mut out) or {
		assert false, '${err}'
		return
	}
	t.send_offsets('eos-grp', g.positions()) or {
		assert false, 'send offsets: ${err}'
		return
	}
	t.commit() or {
		assert false, '${err}'
		return
	}

	// output visible to read_committed
	mut cc := txn_client(cl, .read_committed)
	defer {
		cc.close()
	}
	mut co := cc.new_consumer(['eos-out'], ConsumerOpts{}) or {
		assert false, '${err}'
		return
	}
	outs := co.poll() or {
		assert false, '${err}'
		return
	}
	assert outs.len == 4
	ov := outs[0].value or { []u8{} }
	assert ov.bytestr() == 'OUT(in-0)'

	// offsets applied atomically: a fresh member finds nothing to replay
	g.close()
	mut gc2 := txn_client(cl, .read_committed)
	defer {
		gc2.close()
	}
	mut g2 := gc2.new_group_consumer('eos-grp', ['eos-in'], gopts) or {
		assert false, '${err}'
		return
	}
	leftovers := g2.poll() or {
		assert false, '${err}'
		return
	}
	assert leftovers.len == 0, 'EOS offsets not applied: replayed ${leftovers.len}'

	// ---- abort path: output hidden AND offsets rolled back ----
	mut late := [
		Record{
			value: 'in-late'.bytes()
		},
	]
	c.produce('eos-in', mut late) or {
		assert false, '${err}'
		return
	}
	mut late_got := []Record{}
	for _ in 0 .. 40 {
		late_got << g2.poll() or {
			assert false, '${err}'
			return
		}
		if late_got.len >= 1 {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	assert late_got.len == 1

	t.begin() or {
		assert false, '${err}'
		return
	}
	mut doomed_out := [
		Record{
			value: 'OUT(in-late)'.bytes()
		},
	]
	t.produce('eos-out', mut doomed_out) or {
		assert false, '${err}'
		return
	}
	t.send_offsets('eos-grp', g2.positions()) or {
		assert false, '${err}'
		return
	}
	t.abort() or {
		assert false, '${err}'
		return
	}

	extra := co.poll() or {
		assert false, '${err}'
		return
	}
	assert extra.len == 0, 'aborted EOS output leaked'
	g2.close()
	mut gc3 := txn_client(cl, .read_committed)
	defer {
		gc3.close()
	}
	mut g3 := gc3.new_group_consumer('eos-grp', ['eos-in'], gopts) or {
		assert false, '${err}'
		return
	}
	mut replayed := []Record{}
	for _ in 0 .. 40 {
		replayed << g3.poll() or {
			assert false, '${err}'
			return
		}
		if replayed.len >= 1 {
			break
		}
		time.sleep(20 * time.millisecond)
	}
	assert replayed.len == 1, 'aborted offsets were applied anyway'
	rv := replayed[0].value or { []u8{} }
	assert rv.bytestr() == 'in-late'
	g3.close()
}
