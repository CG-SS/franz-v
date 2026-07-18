module kerr

fn test_error_for_code_basics() {
	// code 0 means no error
	if _ := error_for_code(0) {
		assert false, 'code 0 must return none'
	}

	e1 := error_for_code(1) or { KafkaError{} }
	assert e1.message == 'OFFSET_OUT_OF_RANGE'
	assert e1.error_code == 1
	assert !e1.retriable

	e3 := error_for_code(3) or { KafkaError{} }
	assert e3.message == 'UNKNOWN_TOPIC_OR_PARTITION'
	assert e3.retriable

	em1 := error_for_code(-1) or { KafkaError{} }
	assert em1.message == 'UNKNOWN_SERVER_ERROR'

	// unknown codes map to unknown_server_error
	eunk := error_for_code(30000) or { KafkaError{} }
	assert eunk.message == 'UNKNOWN_SERVER_ERROR'
}

fn test_is_retriable() {
	assert !is_retriable(0)
	assert !is_retriable(-1) // UNKNOWN_SERVER_ERROR is not retriable
	assert is_retriable(2) // CORRUPT_MESSAGE
	assert is_retriable(3) // UNKNOWN_TOPIC_OR_PARTITION
	assert !is_retriable(1) // OFFSET_OUT_OF_RANGE
	assert is_retriable(7) // REQUEST_TIMED_OUT
	assert !is_retriable(29) // TOPIC_AUTHORIZATION_FAILED
	assert is_retriable(89) // THROTTLING_QUOTA_EXCEEDED
	assert !is_retriable(30000) // unknown codes are not retriable
}

fn test_ierror_conformance() {
	// KafkaError works as a V error, keeping the typed code
	e := not_leader_for_partition
	assert e.msg() == 'NOT_LEADER_FOR_PARTITION: This server is not the leader for that topic-partition.'
	assert e.code() == 6

	// returning it through V's error channel
	res := failing_fn() or {
		assert err.msg().starts_with('REBALANCE_IN_PROGRESS:')
		assert err.code() == 27
		return
	}
	assert false, 'should have errored, got ${res}'
}

fn failing_fn() !int {
	return rebalance_in_progress
}

fn test_table_integrity() {
	// franz-go currently defines codes -1 and 1..133 (0 is "no error")
	assert all_errors.len == 134

	// codes are unique and messages are unique
	mut seen_codes := map[int]bool{}
	mut seen_msgs := map[string]bool{}
	for e in all_errors {
		assert !seen_codes[int(e.error_code)], 'duplicate code ${e.error_code}'
		seen_codes[int(e.error_code)] = true
		assert !seen_msgs[e.message], 'duplicate message ${e.message}'
		seen_msgs[e.message] = true
		assert e.message.len > 0
		assert e.description.len > 0
		assert e.error_code != 0, 'code 0 must not be in the table'
	}

	// contiguous coverage: -1, then 1..133 present
	assert seen_codes[-1]
	for c in 1 .. 134 {
		assert seen_codes[c], 'missing code ${c}'
	}

	// every code in the table round-trips through error_for_code
	for e in all_errors {
		got := error_for_code(e.error_code) or { KafkaError{} }
		assert got.message == e.message
		assert got.retriable == e.retriable
	}
}

fn test_spot_check_late_additions() {
	// spot-check entries near the end of the table (most likely to be
	// mangled by generation)
	e120 := error_for_code(120) or { KafkaError{} }
	assert e120.message == 'TRANSACTION_ABORTABLE'
	e133 := error_for_code(133) or { KafkaError{} }
	assert e133.message == 'SHARE_SESSION_LIMIT_REACHED'
	assert e133.retriable
	e100 := error_for_code(100) or { KafkaError{} }
	assert e100.message == 'UNKNOWN_TOPIC_ID'
	assert e100.retriable
}
