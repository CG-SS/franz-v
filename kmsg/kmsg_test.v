module kmsg

import kbin

// roundtrip encodes req, decodes into a fresh struct, re-encodes, and
// requires byte equality — the strongest version-independent invariant.
fn roundtrip[T](orig T) T {
	mut w := kbin.Writer{}
	orig.write_to(mut w)
	mut got := T{
		version: orig.version
	}
	mut r := kbin.Reader{
		src: w.buf
	}
	got.read_from(mut r) or {
		assert false, '${T.name} v${orig.version} decode failed: ${err}'
		return got
	}
	assert r.remaining() == 0, '${T.name} v${orig.version} left ${r.remaining()} bytes'
	mut w2 := kbin.Writer{}
	got.write_to(mut w2)
	assert w2.buf == w.buf, '${T.name} v${orig.version} re-encode mismatch'
	return got
}

// ---------------------------------------------------------------------------
// Spec-pinned wire bytes (hand-derived from the Kafka protocol spec)
// ---------------------------------------------------------------------------

fn test_metadata_request_v0_wire_bytes() {
	// v0: array<string> topics. ["a", "bc"] =>
	// int32(2), int16(1) 'a', int16(2) 'b' 'c'
	mut req := MetadataRequest{
		version: 0
		topics:  [
			MetadataRequestTopic{
				topic: 'a'
			},
			MetadataRequestTopic{
				topic: 'bc'
			},
		]
	}
	mut w := kbin.Writer{}
	req.write_to(mut w)
	assert w.buf == [u8(0x00), 0x00, 0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x02, 0x62, 0x63]
}

fn test_metadata_request_null_topics_wire_bytes() {
	// v1+: null topics (= all topics) encodes as int32(-1)
	mut req := MetadataRequest{
		version: 1
	}
	mut w := kbin.Writer{}
	req.write_to(mut w)
	assert w.buf == [u8(0xff), 0xff, 0xff, 0xff]

	// flexible v9: null topics encodes as uvarint 0, then bools, then 0 tags
	mut req9 := MetadataRequest{
		version:                               9
		allow_auto_topic_creation:             true
		include_cluster_authorized_operations: true
		include_topic_authorized_operations:   false
	}
	mut w9 := kbin.Writer{}
	req9.write_to(mut w9)
	assert w9.buf == [u8(0x00), 0x01, 0x01, 0x00, 0x00]
}

fn test_api_versions_request_wire_bytes() {
	// v0: empty body
	mut req0 := ApiVersionsRequest{
		version: 0
	}
	mut w0 := kbin.Writer{}
	req0.write_to(mut w0)
	assert w0.buf == []u8{}

	// v3 (flexible): compact strings 'me', '1.0', empty tag section
	mut req3 := ApiVersionsRequest{
		version:                 3
		client_software_name:    'me'
		client_software_version: '1.0'
	}
	mut w3 := kbin.Writer{}
	req3.write_to(mut w3)
	assert w3.buf == [u8(0x03), 0x6d, 0x65, 0x04, 0x31, 0x2e, 0x30, 0x00]
}

// ---------------------------------------------------------------------------
// Round-trips across every version
// ---------------------------------------------------------------------------

fn test_metadata_request_all_versions() {
	mut uuid := [16]u8{}
	for i in 0 .. 16 {
		uuid[i] = u8(i + 1)
	}
	for version in i16(0) .. i16(14) {
		mut req := MetadataRequest{
			version:                               version
			topics:                                [
				MetadataRequestTopic{
					topic_id: uuid
					topic:    'topic-a'
				},
				MetadataRequestTopic{
					topic: 'topic-b'
				},
			]
			allow_auto_topic_creation:             true
			include_cluster_authorized_operations: true
			include_topic_authorized_operations:   true
		}
		got := roundtrip(req)
		topics := got.topics or { []MetadataRequestTopic{} }
		assert topics.len == 2
		if version >= 10 {
			assert topics[0].topic_id == uuid
			t0 := topics[0].topic or { '<none>' }
			assert t0 == 'topic-a'
		} else {
			// below v10 topic is a plain string on the wire
			t1 := topics[1].topic or { '<none>' }
			assert t1 == 'topic-b'
		}
		if version >= 4 {
			assert got.allow_auto_topic_creation
		} else {
			assert !got.allow_auto_topic_creation // not on the wire below v4
		}
	}
}

fn test_metadata_request_null_vs_empty_topics() {
	for version in [i16(1), 5, 9, 12] {
		// null topics survives a round-trip as null
		mut null_req := MetadataRequest{
			version: version
		}
		got_null := roundtrip(null_req)
		assert got_null.topics == none

		// empty (but present) topics survives as empty, not null
		mut empty_req := MetadataRequest{
			version: version
			topics:  []MetadataRequestTopic{}
		}
		got_empty := roundtrip(empty_req)
		topics := got_empty.topics or {
			assert false, 'v${version}: empty topics decoded as null'
			return
		}

		assert topics.len == 0
	}
}

fn test_metadata_response_all_versions() {
	for version in i16(0) .. i16(14) {
		mut resp := MetadataResponse{
			version:               version
			throttle_millis:       50
			brokers:               [
				MetadataResponseBroker{
					node_id: 1
					host:    'broker-1.example'
					port:    9092
					rack:    'rack-a'
				},
				MetadataResponseBroker{
					node_id: 2
					host:    'broker-2.example'
					port:    9093
				},
			]
			cluster_id:            'test-cluster'
			controller_id:         1
			topics:                [
				MetadataResponseTopic{
					error_code:            0
					topic:                 'events'
					is_internal:           false
					partitions:            [
						MetadataResponseTopicPartition{
							error_code:       0
							partition:        0
							leader:           1
							leader_epoch:     5
							replicas:         [1, 2]
							isr:              [1, 2]
							offline_replicas: [3]
						},
						MetadataResponseTopicPartition{
							partition: 1
							leader:    2
							replicas:  [2, 1]
							isr:       [2]
						},
					]
					authorized_operations: 4095
				},
			]
			authorized_operations: 2047
			error_code:            0
		}
		got := roundtrip(resp)
		assert got.brokers.len == 2
		assert got.brokers[0].host == 'broker-1.example'
		assert got.topics.len == 1
		assert got.topics[0].partitions.len == 2
		assert got.topics[0].partitions[0].replicas == [1, 2]
		if version >= 1 {
			rack := got.brokers[0].rack or { '<none>' }
			assert rack == 'rack-a'
			assert got.brokers[1].rack == none
		}
		if version >= 3 {
			assert got.throttle_millis == 50
		}
		if version >= 7 {
			assert got.topics[0].partitions[0].leader_epoch == 5
		} else {
			assert got.topics[0].partitions[0].leader_epoch == -1 // struct default
		}
		if version >= 8 && version <= 10 {
			assert got.authorized_operations == 2047
		} else {
			assert got.authorized_operations == -2147483648 // struct default
		}
	}
}

fn test_api_versions_response_all_versions() {
	for version in i16(0) .. i16(5) {
		mut resp := ApiVersionsResponse{
			version:         version
			error_code:      0
			api_keys:        [
				ApiVersionsResponseApiKey{
					api_key:     0
					min_version: 0
					max_version: 13
				},
				ApiVersionsResponseApiKey{
					api_key:     3
					min_version: 0
					max_version: 13
				},
			]
			throttle_millis: 25
		}
		got := roundtrip(resp)
		assert got.api_keys.len == 2
		assert got.api_keys[1].api_key == 3
		if version >= 1 {
			assert got.throttle_millis == 25
		}
	}
}

// ---------------------------------------------------------------------------
// Flexible-version tagged fields
// ---------------------------------------------------------------------------

fn test_tags_omitted_when_default() {
	// all tagged fields at defaults: tag section must be a single 0x00
	mut resp := ApiVersionsResponse{
		version: 3
	}
	mut w := kbin.Writer{}
	resp.write_to(mut w)
	// body: error_code int16(0), compact array len 1 (empty), throttle
	// int32(0), tag count 0
	assert w.buf == [u8(0x00), 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]
	assert w.buf.last() == 0x00
}

fn test_tags_roundtrip_when_set() {
	mut resp := ApiVersionsResponse{
		version:                  3
		api_keys:                 [
			ApiVersionsResponseApiKey{
				api_key:     18
				max_version: 4
			},
		]
		supported_features:       [
			ApiVersionsResponseSupportedFeature{
				name:        'metadata.version'
				min_version: 1
				max_version: 7
			},
		]
		finalized_features_epoch: 42
		finalized_features:       [
			ApiVersionsResponseFinalizedFeature{
				name:              'metadata.version'
				max_version_level: 7
				min_version_level: 1
			},
		]
		zk_migration_ready:       true
	}
	got := roundtrip(resp)
	assert got.supported_features.len == 1
	assert got.supported_features[0].name == 'metadata.version'
	assert got.supported_features[0].max_version == 7
	assert got.finalized_features_epoch == 42
	assert got.finalized_features.len == 1
	assert got.finalized_features[0].max_version_level == 7
	assert got.zk_migration_ready
	assert got.unknown_tags.len == 0
}

fn test_tags_not_written_below_flexible() {
	// v2 is not flexible: tagged fields must not appear at all
	mut resp := ApiVersionsResponse{
		version:                  2
		finalized_features_epoch: 42
		zk_migration_ready:       true
	}
	mut w := kbin.Writer{}
	resp.write_to(mut w)
	// error_code int16, array len int32(0), throttle int32 — 10 bytes, no tags
	assert w.buf.len == 10
	got := roundtrip(resp)
	// tagged values were never on the wire, so decode keeps struct defaults
	assert got.finalized_features_epoch == -1
	assert !got.zk_migration_ready
}

fn test_unknown_tag_preserved_and_reencoded() {
	// craft an ApiVersionsResponse v3 that carries tag 1 (known:
	// finalized_features_epoch) followed by tag 9 (unknown to this client)
	mut w := kbin.Writer{}
	w.write_int16(0) // error_code
	w.write_compact_array_len(0) // api_keys
	w.write_int32(0) // throttle_millis
	w.write_uvarint(2) // two tags
	w.write_uvarint(1) // tag 1: finalized_features_epoch
	w.write_uvarint(8)
	w.write_int64(42)
	w.write_uvarint(9) // tag 9: unknown
	w.write_uvarint(3)
	w.buf << [u8(0xaa), 0xbb, 0xcc]

	mut resp := ApiVersionsResponse{
		version: 3
	}
	mut r := kbin.Reader{
		src: w.buf
	}
	resp.read_from(mut r) or {
		assert false, 'decode failed: ${err}'
		return
	}
	assert resp.finalized_features_epoch == 42
	assert resp.unknown_tags.len == 1
	assert resp.unknown_tags[0].tag == 9
	assert resp.unknown_tags[0].data == [u8(0xaa), 0xbb, 0xcc]

	// re-encoding reproduces the original bytes exactly (lossless)
	mut w2 := kbin.Writer{}
	resp.write_to(mut w2)
	assert w2.buf == w.buf
}

// ---------------------------------------------------------------------------
// Interface conformance and error paths
// ---------------------------------------------------------------------------

fn test_request_interface() {
	reqs := [
		Request(MetadataRequest{
			version: 12
		}),
		Request(ApiVersionsRequest{
			version: 3
		}),
	]
	assert reqs[0].key() == 3
	assert reqs[0].max_version() == 13
	assert reqs[0].is_flexible()
	assert reqs[1].key() == 18
	assert reqs[1].max_version() == 4
	mut w := kbin.Writer{}
	reqs[0].write_to(mut w)
	assert w.buf.len > 0
}

fn test_truncated_decode_errors() {
	mut req := MetadataRequest{
		version: 12
		topics:  [
			MetadataRequestTopic{
				topic: 'a-topic'
			},
		]
	}
	mut w := kbin.Writer{}
	req.write_to(mut w)
	// every strict prefix of a valid message must fail to decode, never panic
	for cut in 0 .. w.buf.len {
		mut partial := MetadataRequest{
			version: 12
		}
		mut r := kbin.Reader{
			src: w.buf[..cut]
		}
		partial.read_from(mut r) or {
			assert err is kbin.NotEnoughDataError
			continue
		}
		assert false, 'decode of ${cut}/${w.buf.len} bytes should have failed'
	}
}

// ---------------------------------------------------------------------------
// Full-coverage features: produce/fetch, enums, nullable structs,
// length-field-minus record batches, version-prefixed records
// ---------------------------------------------------------------------------

fn test_produce_request_roundtrip() {
	records := [u8(0x01), 0x02, 0x03, 0xfe, 0xff]
	for version in i16(0) .. i16(13) {
		mut req := ProduceRequest{
			version:        version
			transaction_id: 'txn-1'
			acks:           -1
			timeout_millis: 30000
			topics:         [
				ProduceRequestTopic{
					topic:      'events'
					partitions: [
						ProduceRequestTopicPartition{
							partition: 3
							records:   records
						},
					]
				},
			]
		}
		got := roundtrip(req)
		assert got.topics.len == 1
		assert got.topics[0].partitions[0].partition == 3
		rec := got.topics[0].partitions[0].records or { []u8{} }
		assert rec == records
		assert got.timeout_millis == 30000
		if version >= 3 {
			tid := got.transaction_id or { '<none>' }
			assert tid == 'txn-1'
		}
	}
}

fn test_fetch_roundtrip() {
	batch_bytes := [u8(0xaa), 0xbb, 0xcc, 0xdd]
	for version in i16(0) .. i16(18) {
		mut req := FetchRequest{
			version:         version
			max_wait_millis: 500
			min_bytes:       1
			max_bytes:       1048576
			topics:          [
				FetchRequestTopic{
					topic:      'events'
					partitions: [
						FetchRequestTopicPartition{
							partition:           0
							fetch_offset:        42
							partition_max_bytes: 65536
						},
					]
				},
			]
		}
		roundtrip(req)

		mut resp := FetchResponse{
			version: version
			topics:  [
				FetchResponseTopic{
					topic:      'events'
					partitions: [
						FetchResponseTopicPartition{
							partition:      0
							high_watermark: 100
							record_batches: batch_bytes
						},
					]
				},
			]
		}
		got := roundtrip(resp)
		rb := got.topics[0].partitions[0].record_batches or { []u8{} }
		assert rb == batch_bytes
	}
}

fn test_enum_fields() {
	// fresh struct carries the DSL default ACLResourcePatternType(3) = LITERAL
	mut req := DescribeACLsRequest{
		version: 2
	}
	assert req.resource_pattern_type == acl_resource_pattern_type_literal
	assert req.resource_pattern_type == ACLResourcePatternType(3)

	req = DescribeACLsRequest{
		version:               2
		resource_type:         acl_resource_type_topic
		resource_name:         'events'
		resource_pattern_type: acl_resource_pattern_type_prefixed
		operation:             acl_operation_write
		permission_type:       acl_permission_type_allow
	}
	got := roundtrip(req)
	assert got.resource_type == acl_resource_type_topic
	assert got.resource_pattern_type == acl_resource_pattern_type_prefixed
	// unknown future enum values survive a round-trip (open alias)
	mut unk := DescribeACLsRequest{
		version:       2
		resource_type: ACLResourceType(120)
	}
	got2 := roundtrip(unk)
	assert got2.resource_type == ACLResourceType(120)
}

fn test_nullable_struct_field() {
	// ConsumerGroupHeartbeatResponse.Assignment is `nullable=>`:
	// wire carries int8 -1 when absent, 1 + fields when present
	mut absent := ConsumerGroupHeartbeatResponse{
		version: 0
	}
	mut w := kbin.Writer{}
	absent.write_to(mut w)
	assert w.buf.last() == 0x00 // empty tag section
	got_absent := roundtrip(absent)
	assert got_absent.assignment == none

	mut uuid := [16]u8{}
	uuid[0] = 7
	mut present := ConsumerGroupHeartbeatResponse{
		version:    0
		member_id:  'm-1'
		assignment: ConsumerGroupHeartbeatResponseAssignment{
			topics: [
				ConsumerGroupHeartbeatResponseAssignmentTopic{
					topic_id:   uuid
					partitions: [0, 1, 2]
				},
			]
		}
	}
	got := roundtrip(present)
	asg := got.assignment or {
		assert false, 'assignment should be present'
		return
	}

	assert asg.topics.len == 1
	assert asg.topics[0].topic_id == uuid
	assert asg.topics[0].partitions == [0, 1, 2]
}

fn test_record_batch_length_field_minus() {
	// RecordBatch.Records length = Length field - 49
	records := [u8(0x10), 0x20, 0x30, 0x40, 0x50]
	mut batch := RecordBatch{
		first_offset:           0
		length:                 49 + records.len
		partition_leader_epoch: -1
		magic:                  2
		crc:                    0x1234
		attributes:             0
		last_offset_delta:      4
		first_timestamp:        1700000000000
		max_timestamp:          1700000000004
		producer_id:            -1
		producer_epoch:         -1
		first_sequence:         0
		num_records:            5
		records:                records
	}
	mut w := kbin.Writer{}
	batch.write_to(mut w)
	// fixed header is 61 bytes: 8+4+4+1+4+2+4+8+8+8+2+4+4
	assert w.buf.len == 61 + records.len

	mut got := RecordBatch{}
	mut r := kbin.Reader{
		src: w.buf
	}
	got.read_from(mut r) or {
		assert false, 'decode failed: ${err}'
		return
	}
	assert r.remaining() == 0
	assert got.records == records
	assert got.num_records == 5
	assert got.first_timestamp == 1700000000000

	// a length field that overruns the buffer must error, not panic
	mut bad := RecordBatch{}
	mut wbad := kbin.Writer{}
	batch.write_to(mut wbad)
	mut rbad := kbin.Reader{
		src: wbad.buf[..40]
	}
	bad.read_from(mut rbad) or { assert err is kbin.NotEnoughDataError }
}

fn test_offset_commit_key_version_prefix() {
	// `with version field` types carry a wire-encoded int16 version prefix
	mut key := OffsetCommitKey{
		version:   1
		group:     'my-group'
		topic:     'events'
		partition: 7
	}
	mut w := kbin.Writer{}
	key.write_to(mut w)
	assert w.buf[0] == 0x00 && w.buf[1] == 0x01 // int16 version = 1 leads

	mut got := OffsetCommitKey{}
	mut r := kbin.Reader{
		src: w.buf
	}
	got.read_from(mut r) or {
		assert false, 'decode failed: ${err}'
		return
	}
	assert got.version == 1
	assert got.group == 'my-group'
	assert got.topic == 'events'
	assert got.partition == 7
	assert r.remaining() == 0
}
