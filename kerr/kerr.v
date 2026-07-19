// Module kerr contains Kafka errors; a V implementation of franz-go's
// pkg/kerr error table.
//
// The error descriptions duplicate the official ones at
// https://kafka.apache.org/protocol.html#protocol_error_codes
//
// This file is generated from the upstream Go table; regenerate rather
// than hand-editing the entries.
module kerr

// KafkaError is a Kafka error. It implements V's IError, so it can be
// returned anywhere an error is expected while retaining the typed code.
pub struct KafkaError {
pub:
	// message is the string form of a Kafka error code (UNKNOWN_SERVER_ERROR, etc).
	message string
	// error_code is the int16 Kafka error code. (Named error_code because V
	// does not allow a field and a method to share the name `code`.)
	error_code i16
	// retriable is whether the error is considered retriable by Kafka.
	retriable bool
	// description is a succinct description of what this error means.
	description string
}

// msg implements IError.
pub fn (e &KafkaError) msg() string {
	return '${e.message}: ${e.description}'
}

// code implements IError, returning the Kafka error code.
pub fn (e &KafkaError) code() int {
	return int(e.error_code)
}

pub const unknown_server_error = KafkaError{'UNKNOWN_SERVER_ERROR', -1, false, 'The server experienced an unexpected error when processing the request.'}
pub const offset_out_of_range = KafkaError{'OFFSET_OUT_OF_RANGE', 1, false, 'The requested offset is not within the range of offsets maintained by the server.'}
pub const corrupt_message = KafkaError{'CORRUPT_MESSAGE', 2, true, 'This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.'}
pub const unknown_topic_or_partition = KafkaError{'UNKNOWN_TOPIC_OR_PARTITION', 3, true, 'This server does not host this topic-partition.'}
pub const invalid_fetch_size = KafkaError{'INVALID_FETCH_SIZE', 4, false, 'The requested fetch size is invalid.'}
pub const leader_not_available = KafkaError{'LEADER_NOT_AVAILABLE', 5, true, 'There is no leader for this topic-partition as we are in the middle of a leadership election.'}
pub const not_leader_for_partition = KafkaError{'NOT_LEADER_FOR_PARTITION', 6, true, 'This server is not the leader for that topic-partition.'}
pub const request_timed_out = KafkaError{'REQUEST_TIMED_OUT', 7, true, 'The request timed out.'}
pub const broker_not_available = KafkaError{'BROKER_NOT_AVAILABLE', 8, true, 'The broker is not available.'}
pub const replica_not_available = KafkaError{'REPLICA_NOT_AVAILABLE', 9, true, 'The replica is not available for the requested topic-partition.'}
pub const message_too_large = KafkaError{'MESSAGE_TOO_LARGE', 10, false, 'The request included a message larger than the max message size the server will accept'}
pub const stale_controller_epoch = KafkaError{'STALE_CONTROLLER_EPOCH', 11, false, 'The controller moved to another broker.'}
pub const offset_metadata_too_large = KafkaError{'OFFSET_METADATA_TOO_LARGE', 12, false, 'The metadata field of the offset request was too large.'}
pub const network_exception = KafkaError{'NETWORK_EXCEPTION', 13, true, 'The server disconnected before a response was received.'}
pub const coordinator_load_in_progress = KafkaError{'COORDINATOR_LOAD_IN_PROGRESS', 14, true, "The coordinator is loading and hence can't process requests."}
pub const coordinator_not_available = KafkaError{'COORDINATOR_NOT_AVAILABLE', 15, true, 'The coordinator is not available.'}
pub const not_coordinator = KafkaError{'NOT_COORDINATOR', 16, true, 'This is not the correct coordinator.'}
pub const invalid_topic_exception = KafkaError{'INVALID_TOPIC_EXCEPTION', 17, false, 'The request attempted to perform an operation on an invalid topic.'}
pub const record_list_too_large = KafkaError{'RECORD_LIST_TOO_LARGE', 18, false, 'The request included message batch larger than the configured segment size on the server.'}
pub const not_enough_replicas = KafkaError{'NOT_ENOUGH_REPLICAS', 19, true, 'Messages are rejected since there are fewer in-sync replicas than required.'}
pub const not_enough_replicas_after_append = KafkaError{'NOT_ENOUGH_REPLICAS_AFTER_APPEND', 20, true, 'Messages are written to the log, but to fewer in-sync replicas than required.'}
pub const invalid_required_acks = KafkaError{'INVALID_REQUIRED_ACKS', 21, false, 'Produce request specified an invalid value for required acks.'}
pub const illegal_generation = KafkaError{'ILLEGAL_GENERATION', 22, false, 'Specified group generation id is not valid.'}
pub const inconsistent_group_protocol = KafkaError{'INCONSISTENT_GROUP_PROTOCOL', 23, false, "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."}
pub const invalid_group_id = KafkaError{'INVALID_GROUP_ID', 24, false, 'The configured groupID is invalid.'}
pub const unknown_member_id = KafkaError{'UNKNOWN_MEMBER_ID', 25, false, 'The coordinator is not aware of this member.'}
pub const invalid_session_timeout = KafkaError{'INVALID_SESSION_TIMEOUT', 26, false, 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).'}
pub const rebalance_in_progress = KafkaError{'REBALANCE_IN_PROGRESS', 27, false, 'The group is rebalancing, so a rejoin is needed.'}
pub const invalid_commit_offset_size = KafkaError{'INVALID_COMMIT_OFFSET_SIZE', 28, false, 'The committing offset data size is not valid.'}
pub const topic_authorization_failed = KafkaError{'TOPIC_AUTHORIZATION_FAILED', 29, false, 'Not authorized to access topics: [Topic authorization failed.]'}
pub const group_authorization_failed = KafkaError{'GROUP_AUTHORIZATION_FAILED', 30, false, 'Not authorized to access group: Group authorization failed.'}
pub const cluster_authorization_failed = KafkaError{'CLUSTER_AUTHORIZATION_FAILED', 31, false, 'Cluster authorization failed.'}
pub const invalid_timestamp = KafkaError{'INVALID_TIMESTAMP', 32, false, 'The timestamp of the message is out of acceptable range.'}
pub const unsupported_sasl_mechanism = KafkaError{'UNSUPPORTED_SASL_MECHANISM', 33, false, 'The broker does not support the requested SASL mechanism.'}
pub const illegal_sasl_state = KafkaError{'ILLEGAL_SASL_STATE', 34, false, 'Request is not valid given the current SASL state.'}
pub const unsupported_version = KafkaError{'UNSUPPORTED_VERSION', 35, false, 'The version of API is not supported.'}
pub const topic_already_exists = KafkaError{'TOPIC_ALREADY_EXISTS', 36, false, 'Topic with this name already exists.'}
pub const invalid_partitions = KafkaError{'INVALID_PARTITIONS', 37, false, 'Number of partitions is below 1.'}
pub const invalid_replication_factor = KafkaError{'INVALID_REPLICATION_FACTOR', 38, false, 'Replication factor is below 1 or larger than the number of available brokers.'}
pub const invalid_replica_assignment = KafkaError{'INVALID_REPLICA_ASSIGNMENT', 39, false, 'Replica assignment is invalid.'}
pub const invalid_config = KafkaError{'INVALID_CONFIG', 40, false, 'Configuration is invalid.'}
pub const not_controller = KafkaError{'NOT_CONTROLLER', 41, true, 'This is not the correct controller for this cluster.'}
pub const invalid_request = KafkaError{'INVALID_REQUEST', 42, false, 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.'}
pub const unsupported_for_message_format = KafkaError{'UNSUPPORTED_FOR_MESSAGE_FORMAT', 43, false, 'The message format version on the broker does not support the request.'}
pub const policy_violation = KafkaError{'POLICY_VIOLATION', 44, false, 'Request parameters do not satisfy the configured policy.'}
pub const out_of_order_sequence_number = KafkaError{'OUT_OF_ORDER_SEQUENCE_NUMBER', 45, false, 'The broker received an out of order sequence number.'}
pub const duplicate_sequence_number = KafkaError{'DUPLICATE_SEQUENCE_NUMBER', 46, false, 'The broker received a duplicate sequence number.'}
pub const invalid_producer_epoch = KafkaError{'INVALID_PRODUCER_EPOCH', 47, false, 'Producer attempted an operation with an old epoch.'}
pub const invalid_txn_state = KafkaError{'INVALID_TXN_STATE', 48, false, 'The producer attempted a transactional operation in an invalid state.'}
pub const invalid_producer_id_mapping = KafkaError{'INVALID_PRODUCER_ID_MAPPING', 49, false, 'The producer attempted to use a producer id which is not currently assigned to its transactional id.'}
pub const invalid_transaction_timeout = KafkaError{'INVALID_TRANSACTION_TIMEOUT', 50, false, 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).'}
pub const concurrent_transactions = KafkaError{'CONCURRENT_TRANSACTIONS', 51, false, 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.'}
pub const transaction_coordinator_fenced = KafkaError{'TRANSACTION_COORDINATOR_FENCED', 52, false, 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.'}
pub const transactional_id_authorization_failed = KafkaError{'TRANSACTIONAL_ID_AUTHORIZATION_FAILED', 53, false, 'Transactional ID authorization failed.'}
pub const security_disabled = KafkaError{'SECURITY_DISABLED', 54, false, 'Security features are disabled.'}
pub const operation_not_attempted = KafkaError{'OPERATION_NOT_ATTEMPTED', 55, false, 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.'}
pub const kafka_storage_error = KafkaError{'KAFKA_STORAGE_ERROR', 56, true, 'Disk error when trying to access log file on the disk.'}
pub const log_dir_not_found = KafkaError{'LOG_DIR_NOT_FOUND', 57, false, 'The user-specified log directory is not found in the broker config.'}
pub const sasl_authentication_failed = KafkaError{'SASL_AUTHENTICATION_FAILED', 58, false, 'SASL Authentication failed.'}
pub const unknown_producer_id = KafkaError{'UNKNOWN_PRODUCER_ID', 59, false, "This exception is raised by the broker if it could not locate the producer metadata associated with the producerID in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerID are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."}
pub const reassignment_in_progress = KafkaError{'REASSIGNMENT_IN_PROGRESS', 60, false, 'A partition reassignment is in progress.'}
pub const delegation_token_auth_disabled = KafkaError{'DELEGATION_TOKEN_AUTH_DISABLED', 61, false, 'Delegation Token feature is not enabled.'}
pub const delegation_token_not_found = KafkaError{'DELEGATION_TOKEN_NOT_FOUND', 62, false, 'Delegation Token is not found on server.'}
pub const delegation_token_owner_mismatch = KafkaError{'DELEGATION_TOKEN_OWNER_MISMATCH', 63, false, 'Specified Principal is not valid Owner/Renewer.'}
pub const delegation_token_request_not_allowed = KafkaError{'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED', 64, false, 'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.'}
pub const delegation_token_authorization_failed = KafkaError{'DELEGATION_TOKEN_AUTHORIZATION_FAILED', 65, false, 'Delegation Token authorization failed.'}
pub const delegation_token_expired = KafkaError{'DELEGATION_TOKEN_EXPIRED', 66, false, 'Delegation Token is expired.'}
pub const invalid_principal_type = KafkaError{'INVALID_PRINCIPAL_TYPE', 67, false, 'Supplied principalType is not supported.'}
pub const non_empty_group = KafkaError{'NON_EMPTY_GROUP', 68, false, 'The group is not empty.'}
pub const group_id_not_found = KafkaError{'GROUP_ID_NOT_FOUND', 69, false, 'The group id does not exist.'}
pub const fetch_session_id_not_found = KafkaError{'FETCH_SESSION_ID_NOT_FOUND', 70, true, 'The fetch session ID was not found.'}
pub const invalid_fetch_session_epoch = KafkaError{'INVALID_FETCH_SESSION_EPOCH', 71, true, 'The fetch session epoch is invalid.'}
pub const listener_not_found = KafkaError{'LISTENER_NOT_FOUND', 72, true, 'There is no listener on the leader broker that matches the listener on which metadata request was processed.'}
pub const topic_deletion_disabled = KafkaError{'TOPIC_DELETION_DISABLED', 73, false, 'Topic deletion is disabled.'}
pub const fenced_leader_epoch = KafkaError{'FENCED_LEADER_EPOCH', 74, true, 'The leader epoch in the request is older than the epoch on the broker'}
pub const unknown_leader_epoch = KafkaError{'UNKNOWN_LEADER_EPOCH', 75, true, 'The leader epoch in the request is newer than the epoch on the broker'}
pub const unsupported_compression_type = KafkaError{'UNSUPPORTED_COMPRESSION_TYPE', 76, false, 'The requesting client does not support the compression type of given partition.'}
pub const stale_broker_epoch = KafkaError{'STALE_BROKER_EPOCH', 77, false, 'Broker epoch has changed'}
pub const offset_not_available = KafkaError{'OFFSET_NOT_AVAILABLE', 78, true, 'The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing'}
pub const member_id_required = KafkaError{'MEMBER_ID_REQUIRED', 79, false, 'The group member needs to have a valid member id before actually entering a consumer group'}
pub const preferred_leader_not_available = KafkaError{'PREFERRED_LEADER_NOT_AVAILABLE', 80, true, 'The preferred leader was not available'}
pub const group_max_size_reached = KafkaError{'GROUP_MAX_SIZE_REACHED', 81, false, 'The consumer group has reached its max size'}
pub const fenced_instance_id = KafkaError{'FENCED_INSTANCE_ID', 82, false, 'The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.'}
pub const eligible_leaders_not_available = KafkaError{'ELIGIBLE_LEADERS_NOT_AVAILABLE', 83, true, 'Eligible topic partition leaders are not available'}
pub const election_not_needed = KafkaError{'ELECTION_NOT_NEEDED', 84, true, 'Leader election not needed for topic partition'}
pub const no_reassignment_in_progress = KafkaError{'NO_REASSIGNMENT_IN_PROGRESS', 85, false, 'No partition reassignment is in progress.'}
pub const group_subscribed_to_topic = KafkaError{'GROUP_SUBSCRIBED_TO_TOPIC', 86, false, 'Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.'}
pub const invalid_record = KafkaError{'INVALID_RECORD', 87, false, 'This record has failed the validation on the broker and hence been rejected.'}
pub const unstable_offset_commit = KafkaError{'UNSTABLE_OFFSET_COMMIT', 88, true, 'There are unstable offsets that need to be cleared.'}
pub const throttling_quota_exceeded = KafkaError{'THROTTLING_QUOTA_EXCEEDED', 89, true, 'The throttling quota has been exceeded.'}
pub const producer_fenced = KafkaError{'PRODUCER_FENCED', 90, false, 'There is a newer producer with the same transactionalId which fences the current one.'}
pub const resource_not_found = KafkaError{'RESOURCE_NOT_FOUND', 91, false, 'A request illegally referred to a resource that does not exist.'}
pub const duplicate_resource = KafkaError{'DUPLICATE_RESOURCE', 92, false, 'A request illegally referred to the same resource twice.'}
pub const unacceptable_credential = KafkaError{'UNACCEPTABLE_CREDENTIAL', 93, false, 'Requested credential would not meet criteria for acceptability.'}
pub const inconsistent_voter_set = KafkaError{'INCONSISTENT_VOTER_SET', 94, false, 'Indicates that either the sender or recipient of a voter-only request is not one of the expected voters.'}
pub const invalid_update_version = KafkaError{'INVALID_UPDATE_VERSION', 95, false, 'The given update version was invalid.'}
pub const feature_update_failed = KafkaError{'FEATURE_UPDATE_FAILED', 96, false, 'Unable to update finalized features due to an unexpected server error.'}
pub const principal_deserialization_failure = KafkaError{'PRINCIPAL_DESERIALIZATION_FAILURE', 97, false, 'Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.'}
pub const snapshot_not_found = KafkaError{'SNAPSHOT_NOT_FOUND', 98, false, 'Requested snapshot was not found.'}
pub const position_out_of_range = KafkaError{'POSITION_OUT_OF_RANGE', 99, false, 'Requested position is not greater than or equal to zero, and less than the size of the snapshot.'}
pub const unknown_topic_id = KafkaError{'UNKNOWN_TOPIC_ID', 100, true, 'This server does not host this topic ID.'}
pub const duplicate_broker_registration = KafkaError{'DUPLICATE_BROKER_REGISTRATION', 101, false, 'This broker ID is already in use.'}
pub const broker_id_not_registered = KafkaError{'BROKER_ID_NOT_REGISTERED', 102, false, 'The given broker ID was not registered.'}
pub const inconsistent_topic_id = KafkaError{'INCONSISTENT_TOPIC_ID', 103, true, "The log's topic ID did not match the topic ID in the request."}
pub const inconsistent_cluster_id = KafkaError{'INCONSISTENT_CLUSTER_ID', 104, false, 'The clusterId in the request does not match that found on the server.'}
pub const transactional_id_not_found = KafkaError{'TRANSACTIONAL_ID_NOT_FOUND', 105, false, 'The transactionalId could not be found.'}
pub const fetch_session_topic_id_error = KafkaError{'FETCH_SESSION_TOPIC_ID_ERROR', 106, true, 'The fetch session encountered inconsistent topic ID usage.'}
pub const ineligible_replica = KafkaError{'INELIGIBLE_REPLICA', 107, false, 'The new ISR contains at least one ineligible replica.'}
pub const new_leader_elected = KafkaError{'NEW_LEADER_ELECTED', 108, false, 'The AlterPartition request successfully updated the partition state but the leader has changed.'}
pub const offset_moved_to_tiered_storage = KafkaError{'OFFSET_MOVED_TO_TIERED_STORAGE', 109, false, 'The requested offset is moved to tiered storage.'}
pub const fenced_member_epoch = KafkaError{'FENCED_MEMBER_EPOCH', 110, false, 'The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.'}
pub const unreleased_instance_id = KafkaError{'UNRELEASED_INSTANCE_ID', 111, false, 'The instance ID is still used by another member in the consumer group. That member must leave first.'}
pub const unsupported_assignor = KafkaError{'UNSUPPORTED_ASSIGNOR', 112, false, 'The assignor or its version range is not supported by the consumer group.'}
pub const stale_member_epoch = KafkaError{'STALE_MEMBER_EPOCH', 113, false, 'The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.'}
pub const mismatched_endpoint_type = KafkaError{'MISMATCHED_ENDPOINT_TYPE', 114, false, 'The request was sent to an endpoint of the wrong type.'}
pub const unsupported_endpoint_type = KafkaError{'UNSUPPORTED_ENDPOINT_TYPE', 115, false, 'This endpoint type is not supported yet.'}
pub const unknown_controller_id = KafkaError{'UNKNOWN_CONTROLLER_ID', 116, false, 'This controller ID is not known'}
pub const unknown_subscription_id = KafkaError{'UNKNOWN_SUBSCRIPTION_ID', 117, false, 'Client sent a push telemetry request with an invalid or outdated subscription ID.'}
pub const telemetry_too_large = KafkaError{'TELEMETRY_TOO_LARGE', 118, false, 'Client sent a push telemetry request larger than the maximum size the broker will accept.'}
pub const invalid_registration = KafkaError{'INVALID_REGISTRATION', 119, false, 'The controller has considered the broker registration to be invalid.'}
pub const transaction_abortable = KafkaError{'TRANSACTION_ABORTABLE', 120, false, 'The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.'}
pub const invalid_record_state = KafkaError{'INVALID_RECORD_STATE', 121, false, 'The record state is invalid. The acknowledgement of delivery could not be completed.'}
pub const share_session_not_found = KafkaError{'SHARE_SESSION_NOT_FOUND', 122, false, 'The share session was not found.'}
pub const invalid_share_session_epoch = KafkaError{'INVALID_SHARE_SESSION_EPOCH', 123, false, 'The share session epoch is invalid.'}
pub const fenced_state_epoch = KafkaError{'FENCED_STATE_EPOCH', 124, false, 'The share coordinator rejected the request because the share-group state epoch did not match.'}
pub const invalid_voter_key = KafkaError{'INVALID_VOTER_KEY', 125, false, "The voter key doesn't match the receiving replica's key."}
pub const duplicate_voter = KafkaError{'DUPLICATE_VOTER', 126, false, 'The voter is already part of the set of voters.'}
pub const voter_not_found = KafkaError{'VOTER_NOT_FOUND', 127, false, 'The voter is not part of the set of voters.'}
pub const invalid_regular_expression = KafkaError{'INVALID_REGULAR_EXPRESSION', 128, false, 'The regular expression is not valid.'}
pub const rebootstrap_required = KafkaError{'REBOOTSTRAP_REQUIRED', 129, false, 'Client metadata is stale. The client should rebootstrap to obtain new metadata.'}
pub const streams_invalid_topology = KafkaError{'STREAMS_INVALID_TOPOLOGY', 130, false, 'The supplied topology is invalid.'}
pub const streams_invalid_topology_epoch = KafkaError{'STREAMS_INVALID_TOPOLOGY_EPOCH', 131, false, 'The supplied topology epoch is invalid.'}
pub const streams_topology_fenced = KafkaError{'STREAMS_TOPOLOGY_FENCED', 132, false, 'The supplied topology epoch is outdated.'}
pub const share_session_limit_reached = KafkaError{'SHARE_SESSION_LIMIT_REACHED', 133, true, 'The limit of share sessions has been reached.'}

// all_errors lists every known Kafka error, ordered by code (code -1 first).
pub const all_errors = [unknown_server_error, offset_out_of_range, corrupt_message,
	unknown_topic_or_partition, invalid_fetch_size, leader_not_available, not_leader_for_partition,
	request_timed_out, broker_not_available, replica_not_available, message_too_large,
	stale_controller_epoch, offset_metadata_too_large, network_exception,
	coordinator_load_in_progress, coordinator_not_available, not_coordinator, invalid_topic_exception,
	record_list_too_large, not_enough_replicas, not_enough_replicas_after_append,
	invalid_required_acks, illegal_generation, inconsistent_group_protocol, invalid_group_id,
	unknown_member_id, invalid_session_timeout, rebalance_in_progress, invalid_commit_offset_size,
	topic_authorization_failed, group_authorization_failed, cluster_authorization_failed,
	invalid_timestamp, unsupported_sasl_mechanism, illegal_sasl_state, unsupported_version,
	topic_already_exists, invalid_partitions, invalid_replication_factor, invalid_replica_assignment,
	invalid_config, not_controller, invalid_request, unsupported_for_message_format, policy_violation,
	out_of_order_sequence_number, duplicate_sequence_number, invalid_producer_epoch,
	invalid_txn_state, invalid_producer_id_mapping, invalid_transaction_timeout,
	concurrent_transactions, transaction_coordinator_fenced, transactional_id_authorization_failed,
	security_disabled, operation_not_attempted, kafka_storage_error, log_dir_not_found,
	sasl_authentication_failed, unknown_producer_id, reassignment_in_progress,
	delegation_token_auth_disabled, delegation_token_not_found, delegation_token_owner_mismatch,
	delegation_token_request_not_allowed, delegation_token_authorization_failed,
	delegation_token_expired, invalid_principal_type, non_empty_group, group_id_not_found,
	fetch_session_id_not_found, invalid_fetch_session_epoch, listener_not_found,
	topic_deletion_disabled, fenced_leader_epoch, unknown_leader_epoch, unsupported_compression_type,
	stale_broker_epoch, offset_not_available, member_id_required, preferred_leader_not_available,
	group_max_size_reached, fenced_instance_id, eligible_leaders_not_available, election_not_needed,
	no_reassignment_in_progress, group_subscribed_to_topic, invalid_record, unstable_offset_commit,
	throttling_quota_exceeded, producer_fenced, resource_not_found, duplicate_resource,
	unacceptable_credential, inconsistent_voter_set, invalid_update_version, feature_update_failed,
	principal_deserialization_failure, snapshot_not_found, position_out_of_range, unknown_topic_id,
	duplicate_broker_registration, broker_id_not_registered, inconsistent_topic_id,
	inconsistent_cluster_id, transactional_id_not_found, fetch_session_topic_id_error,
	ineligible_replica, new_leader_elected, offset_moved_to_tiered_storage, fenced_member_epoch,
	unreleased_instance_id, unsupported_assignor, stale_member_epoch, mismatched_endpoint_type,
	unsupported_endpoint_type, unknown_controller_id, unknown_subscription_id, telemetry_too_large,
	invalid_registration, transaction_abortable, invalid_record_state, share_session_not_found,
	invalid_share_session_epoch, fenced_state_epoch, invalid_voter_key, duplicate_voter,
	voter_not_found, invalid_regular_expression, rebootstrap_required, streams_invalid_topology,
	streams_invalid_topology_epoch, streams_topology_fenced, share_session_limit_reached]!

// error_for_code returns the KafkaError corresponding to the given error code.
//
// If the code is 0 (no error), this returns none.
// If the code is unknown, this returns unknown_server_error.
pub fn error_for_code(code i16) ?KafkaError {
	return match code {
		0 { none }
		1 { offset_out_of_range }
		2 { corrupt_message }
		3 { unknown_topic_or_partition }
		4 { invalid_fetch_size }
		5 { leader_not_available }
		6 { not_leader_for_partition }
		7 { request_timed_out }
		8 { broker_not_available }
		9 { replica_not_available }
		10 { message_too_large }
		11 { stale_controller_epoch }
		12 { offset_metadata_too_large }
		13 { network_exception }
		14 { coordinator_load_in_progress }
		15 { coordinator_not_available }
		16 { not_coordinator }
		17 { invalid_topic_exception }
		18 { record_list_too_large }
		19 { not_enough_replicas }
		20 { not_enough_replicas_after_append }
		21 { invalid_required_acks }
		22 { illegal_generation }
		23 { inconsistent_group_protocol }
		24 { invalid_group_id }
		25 { unknown_member_id }
		26 { invalid_session_timeout }
		27 { rebalance_in_progress }
		28 { invalid_commit_offset_size }
		29 { topic_authorization_failed }
		30 { group_authorization_failed }
		31 { cluster_authorization_failed }
		32 { invalid_timestamp }
		33 { unsupported_sasl_mechanism }
		34 { illegal_sasl_state }
		35 { unsupported_version }
		36 { topic_already_exists }
		37 { invalid_partitions }
		38 { invalid_replication_factor }
		39 { invalid_replica_assignment }
		40 { invalid_config }
		41 { not_controller }
		42 { invalid_request }
		43 { unsupported_for_message_format }
		44 { policy_violation }
		45 { out_of_order_sequence_number }
		46 { duplicate_sequence_number }
		47 { invalid_producer_epoch }
		48 { invalid_txn_state }
		49 { invalid_producer_id_mapping }
		50 { invalid_transaction_timeout }
		51 { concurrent_transactions }
		52 { transaction_coordinator_fenced }
		53 { transactional_id_authorization_failed }
		54 { security_disabled }
		55 { operation_not_attempted }
		56 { kafka_storage_error }
		57 { log_dir_not_found }
		58 { sasl_authentication_failed }
		59 { unknown_producer_id }
		60 { reassignment_in_progress }
		61 { delegation_token_auth_disabled }
		62 { delegation_token_not_found }
		63 { delegation_token_owner_mismatch }
		64 { delegation_token_request_not_allowed }
		65 { delegation_token_authorization_failed }
		66 { delegation_token_expired }
		67 { invalid_principal_type }
		68 { non_empty_group }
		69 { group_id_not_found }
		70 { fetch_session_id_not_found }
		71 { invalid_fetch_session_epoch }
		72 { listener_not_found }
		73 { topic_deletion_disabled }
		74 { fenced_leader_epoch }
		75 { unknown_leader_epoch }
		76 { unsupported_compression_type }
		77 { stale_broker_epoch }
		78 { offset_not_available }
		79 { member_id_required }
		80 { preferred_leader_not_available }
		81 { group_max_size_reached }
		82 { fenced_instance_id }
		83 { eligible_leaders_not_available }
		84 { election_not_needed }
		85 { no_reassignment_in_progress }
		86 { group_subscribed_to_topic }
		87 { invalid_record }
		88 { unstable_offset_commit }
		89 { throttling_quota_exceeded }
		90 { producer_fenced }
		91 { resource_not_found }
		92 { duplicate_resource }
		93 { unacceptable_credential }
		94 { inconsistent_voter_set }
		95 { invalid_update_version }
		96 { feature_update_failed }
		97 { principal_deserialization_failure }
		98 { snapshot_not_found }
		99 { position_out_of_range }
		100 { unknown_topic_id }
		101 { duplicate_broker_registration }
		102 { broker_id_not_registered }
		103 { inconsistent_topic_id }
		104 { inconsistent_cluster_id }
		105 { transactional_id_not_found }
		106 { fetch_session_topic_id_error }
		107 { ineligible_replica }
		108 { new_leader_elected }
		109 { offset_moved_to_tiered_storage }
		110 { fenced_member_epoch }
		111 { unreleased_instance_id }
		112 { unsupported_assignor }
		113 { stale_member_epoch }
		114 { mismatched_endpoint_type }
		115 { unsupported_endpoint_type }
		116 { unknown_controller_id }
		117 { unknown_subscription_id }
		118 { telemetry_too_large }
		119 { invalid_registration }
		120 { transaction_abortable }
		121 { invalid_record_state }
		122 { share_session_not_found }
		123 { invalid_share_session_epoch }
		124 { fenced_state_epoch }
		125 { invalid_voter_key }
		126 { duplicate_voter }
		127 { voter_not_found }
		128 { invalid_regular_expression }
		129 { rebootstrap_required }
		130 { streams_invalid_topology }
		131 { streams_invalid_topology_epoch }
		132 { streams_topology_fenced }
		133 { share_session_limit_reached }
		else { unknown_server_error }
	}
}

// is_retriable returns whether a Kafka error code is considered retriable.
// Code 0 (no error) and unknown codes are not retriable.
pub fn is_retriable(code i16) bool {
	e := error_for_code(code) or { return false }
	return e.retriable
}
