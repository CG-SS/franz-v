// Generated smoke test: round-trips every kmsg type at every wire
// version with default (zero-value) structs, catching any asymmetry
// between write_to and read_from version gating.
module kmsg

import kbin

// Each V _test.v file compiles as its own program, so the roundtrip
// helper is duplicated here rather than shared from kmsg_test.v.
fn smoke_roundtrip[T](orig T) {
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
		return
	}
	assert r.remaining() == 0, '${T.name} v${orig.version} trailing bytes'
	mut w2 := kbin.Writer{}
	got.write_to(mut w2)
	assert w2.buf == w.buf, '${T.name} v${orig.version} re-encode mismatch'
}

fn test_all_messages_all_versions_default_roundtrip() {
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(AddOffsetsToTxnRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(AddOffsetsToTxnResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(AddPartitionsToTxnRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(AddPartitionsToTxnResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AddRaftVoterRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AddRaftVoterResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AllocateProducerIDsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AllocateProducerIDsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AlterClientQuotasRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AlterClientQuotasResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(AlterConfigsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(AlterConfigsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(AlterPartitionRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(AlterPartitionResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AlterPartitionAssignmentsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(AlterPartitionAssignmentsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(AlterReplicaLogDirsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(AlterReplicaLogDirsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AlterShareGroupOffsetsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AlterShareGroupOffsetsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AlterUserSCRAMCredentialsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AlterUserSCRAMCredentialsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(ApiVersionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(ApiVersionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AssignReplicasToDirsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(AssignReplicasToDirsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(BeginQuorumEpochRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(BeginQuorumEpochResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(BrokerHeartbeatRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(BrokerHeartbeatResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(BrokerRegistrationRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(BrokerRegistrationResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ConsumerGroupDescribeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ConsumerGroupDescribeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ConsumerGroupHeartbeatRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ConsumerGroupHeartbeatResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(ControlledShutdownRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(ControlledShutdownResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ControllerRegistrationRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ControllerRegistrationResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreateACLsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreateACLsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreateDelegationTokenRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreateDelegationTokenResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreatePartitionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(CreatePartitionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(8) {
		smoke_roundtrip(CreateTopicsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(8) {
		smoke_roundtrip(CreateTopicsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DeleteACLsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DeleteACLsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DeleteGroupsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DeleteGroupsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DeleteRecordsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DeleteRecordsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DeleteShareGroupOffsetsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DeleteShareGroupOffsetsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DeleteShareGroupStateRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DeleteShareGroupStateResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(DeleteTopicsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(DeleteTopicsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DescribeACLsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DescribeACLsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(DescribeClientQuotasRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(DescribeClientQuotasResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DescribeClusterRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DescribeClusterResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(DescribeConfigsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(DescribeConfigsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DescribeDelegationTokenRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(4) {
		smoke_roundtrip(DescribeDelegationTokenResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(DescribeGroupsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(DescribeGroupsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(DescribeLogDirsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(DescribeLogDirsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeProducersRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeProducersResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DescribeQuorumRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(DescribeQuorumResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(DescribeShareGroupOffsetsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(DescribeShareGroupOffsetsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeTopicPartitionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeTopicPartitionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeTransactionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeTransactionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeUserSCRAMCredentialsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(DescribeUserSCRAMCredentialsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ElectLeadersRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ElectLeadersResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(EndQuorumEpochRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(EndQuorumEpochResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(EndTxnRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(EndTxnResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(EnvelopeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(EnvelopeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ExpireDelegationTokenRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ExpireDelegationTokenResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(19) {
		smoke_roundtrip(FetchRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(19) {
		smoke_roundtrip(FetchResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(FetchSnapshotRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(FetchSnapshotResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(FindCoordinatorRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(7) {
		smoke_roundtrip(FindCoordinatorResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(GetTelemetrySubscriptionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(GetTelemetrySubscriptionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(HeartbeatRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(HeartbeatResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(IncrementalAlterConfigsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(IncrementalAlterConfigsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(InitProducerIDRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(InitProducerIDResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(InitializeShareGroupStateRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(InitializeShareGroupStateResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(10) {
		smoke_roundtrip(JoinGroupRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(10) {
		smoke_roundtrip(JoinGroupResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(8) {
		smoke_roundtrip(LeaderAndISRRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(8) {
		smoke_roundtrip(LeaderAndISRResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(LeaveGroupRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(LeaveGroupResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ListConfigResourcesRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ListConfigResourcesResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(ListGroupsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(ListGroupsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(12) {
		smoke_roundtrip(ListOffsetsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(12) {
		smoke_roundtrip(ListOffsetsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ListPartitionReassignmentsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ListPartitionReassignmentsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ListTransactionsRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ListTransactionsResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(14) {
		smoke_roundtrip(MetadataRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(14) {
		smoke_roundtrip(MetadataResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(11) {
		smoke_roundtrip(OffsetCommitRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(11) {
		smoke_roundtrip(OffsetCommitResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(OffsetDeleteRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(OffsetDeleteResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(11) {
		smoke_roundtrip(OffsetFetchRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(11) {
		smoke_roundtrip(OffsetFetchResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(OffsetForLeaderEpochRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(OffsetForLeaderEpochResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(14) {
		smoke_roundtrip(ProduceRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(14) {
		smoke_roundtrip(ProduceResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(PushTelemetryRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(PushTelemetryResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ReadShareGroupStateRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(ReadShareGroupStateResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ReadShareGroupStateSummaryRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ReadShareGroupStateSummaryResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(RemoveRaftVoterRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(RemoveRaftVoterResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(RenewDelegationTokenRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(RenewDelegationTokenResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(SASLAuthenticateRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(SASLAuthenticateResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(SASLHandshakeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(SASLHandshakeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ShareAcknowledgeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ShareAcknowledgeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ShareFetchRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(ShareFetchResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ShareGroupDescribeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ShareGroupDescribeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ShareGroupHeartbeatRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(ShareGroupHeartbeatResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(StopReplicaRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(5) {
		smoke_roundtrip(StopReplicaResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(StreamsGroupDescribeRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(StreamsGroupDescribeResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(StreamsGroupHeartbeatRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(StreamsGroupHeartbeatResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(SyncGroupRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(SyncGroupResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(TxnOffsetCommitRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(6) {
		smoke_roundtrip(TxnOffsetCommitResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(UnregisterBrokerRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(UnregisterBrokerResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(UpdateFeaturesRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(UpdateFeaturesResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(9) {
		smoke_roundtrip(UpdateMetadataRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(9) {
		smoke_roundtrip(UpdateMetadataResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(UpdateRaftVoterRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(1) {
		smoke_roundtrip(UpdateRaftVoterResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(VoteRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(VoteResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(WriteShareGroupStateRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(2) {
		smoke_roundtrip(WriteShareGroupStateResponse{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(WriteTxnMarkersRequest{
			version: version
		})
	}
	for version in i16(0) .. i16(3) {
		smoke_roundtrip(WriteTxnMarkersResponse{
			version: version
		})
	}
}
