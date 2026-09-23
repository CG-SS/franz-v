module krec

fn test_txn_attributes() {
	recs := [
		Record{
			value:     'v'.bytes()
			timestamp: 5
		},
	]
	plain := build_record_batch(recs, BatchOpts{}) or {
		assert false, '${err}'
		return
	}
	pb, _ := parse_record_batch(plain) or {
		assert false, '${err}'
		return
	}
	assert !is_transactional(pb)
	assert !is_control(pb)

	txn := build_record_batch(recs, BatchOpts{
		producer_id:    77
		producer_epoch: 2
		base_sequence:  10
		transactional:  true
	}) or {
		assert false, '${err}'
		return
	}
	tb, _ := parse_record_batch(txn) or {
		assert false, '${err}'
		return
	}
	assert is_transactional(tb)
	assert !is_control(tb)
	assert tb.producer_id == 77
	assert tb.producer_epoch == 2
	assert tb.first_sequence == 10
}

fn test_control_marker_roundtrip() {
	for commit in [true, false] {
		buf := control_marker_batch(42, 77, 3, commit, 1700000000000) or {
			assert false, '${err}'
			return
		}
		b, recs := parse_record_batch(buf) or {
			assert false, '${err}'
			return
		}
		assert is_control(b)
		assert is_transactional(b)
		assert b.first_offset == 42
		assert b.producer_id == 77
		assert recs.len == 1
		got := control_marker_is_commit(recs[0]) or {
			assert false, 'marker type expected'
			return
		}
		assert got == commit
	}
}

fn test_parse_record_batches_skips_control() {
	data := build_record_batch([
		Record{
			value:     'user data'.bytes()
			timestamp: 1
		},
	], BatchOpts{
		producer_id:    9
		producer_epoch: 0
		transactional:  true
	}) or {
		assert false, '${err}'
		return
	}
	marker := control_marker_batch(1, 9, 0, true, 2) or {
		assert false, '${err}'
		return
	}
	mut stream := data.clone()
	stream << marker

	// meta parse sees both batches
	metas := parse_batches_meta(stream) or {
		assert false, '${err}'
		return
	}
	assert metas.len == 2
	assert is_control(metas[1].batch)

	// flat parse hides the control record
	flat := parse_record_batches(stream) or {
		assert false, '${err}'
		return
	}
	assert flat.len == 1
	v := flat[0].value or { []u8{} }
	assert v.bytestr() == 'user data'
}

fn test_next_sequence_wraps_like_kafka() {
	assert next_sequence(0, 3) == 3
	assert next_sequence(max_i32 - 3, 3) == max_i32
	assert next_sequence(max_i32 - 2, 3) == 0
	assert next_sequence(max_i32 - 1, 3) == 1
	assert next_sequence(max_i32, 1) == 0
	assert next_sequence(max_i32, 5) == 4
}
