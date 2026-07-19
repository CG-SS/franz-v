// Module kadm is the admin client: typed helpers for topic lifecycle,
// configuration, consumer-group inspection, offsets, and record deletion,
// built on the kgo request path. Wide but shallow, like its franz-go
// namesake.
module kadm

import kbin
import kerr
import kgo
import kmsg

// Admin wraps a kgo.Client with admin conveniences.
pub struct Admin {
pub mut:
	client &kmsg.Client
}

// new builds an Admin over an existing client.
pub fn new(c &Client) &Admin {
	return &kmsg.Admin{
		client: c
	}
}

// OpResult is the outcome of one per-entity admin operation.
pub struct OpResult {
pub:
	name    string
	code    i16
	err_msg string
}

// ok returns an error if the operation failed (TOPIC_ALREADY_EXISTS is
// reported, not swallowed).
pub fn (r OpResult) ok() ! {
	if r.code != 0 {
		return error('${r.name}: ${r.err_msg}')
	}
}

fn code_result(name string, code i16) OpResult {
	if code == 0 {
		return kmsg.OpResult{
			name: name
		}
	}
	e := kmsg.error_for_code(code) or { kmsg.unknown_server_error }
	return kmsg.OpResult{
		name:    name
		code:    code
		err_msg: e.msg()
	}
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

// TopicSpec describes a topic to create.
pub struct TopicSpec {
pub mut:
	topic       string
	partitions  int = 1
	replication i16 = 1
	configs     map[string]string
}

// create_topics creates topics, returning one result per topic, sorted.
pub fn (mut a Admin) create_topics(specs []TopicSpec) ![]OpResult {
	mut c := a.client
	mut req := kmsg.CreateTopicsRequest{
		timeout_millis: 15000
	}
	for spec in specs {
		mut rt := kmsg.CreateTopicsRequestTopic{
			topic:              spec.topic
			num_partitions:     spec.partitions
			replication_factor: spec.replication
		}
		mut names := spec.configs.keys()
		names.sort()
		for name in names {
			rt.configs << kmsg.CreateTopicsRequestTopicConfig{
				name:  name
				value: spec.configs[name]
			}
		}
		req.topics << rt
	}
	body := c.request(mut req)!
	mut resp := kmsg.CreateTopicsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	mut out := []kmsg.OpResult{cap: resp.topics.len}
	for t in resp.topics {
		out << code_result(t.topic, t.error_code)
	}
	out.sort(a.name < b.name)
	return out
}

// delete_topics deletes topics by name.
pub fn (mut a Admin) delete_topics(names []string) ![]OpResult {
	mut c := a.client
	mut req := kmsg.DeleteTopicsRequest{
		timeout_millis: 15000
		topic_names:    names.clone()
	}
	// v6+ switches to (name, uuid) pairs; stay on the names shape
	body := c.request_capped(mut req, 5)!
	mut resp := kmsg.DeleteTopicsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	mut out := []kmsg.OpResult{cap: resp.topics.len}
	for t in resp.topics {
		out << code_result(t.topic or { '' }, t.error_code)
	}
	out.sort(a.name < b.name)
	return out
}

// PartitionDetail is one partition's placement.
pub struct PartitionDetail {
pub:
	partition int
	leader    int
	replicas  []int
	isr       []int
}

// TopicDetail is one topic's layout.
pub struct TopicDetail {
pub:
	topic      string
	topic_id   [16]u8
	internal   bool
	partitions []kmsg.PartitionDetail
}

// list_topics lists all topics with partition placement; internal topics
// (names starting with __) are excluded unless include_internal.
pub fn (mut a Admin) list_topics(include_internal bool) ![]TopicDetail {
	mut c := a.client
	resp := c.metadata([])!
	mut out := []kmsg.TopicDetail{}
	for t in resp.topics {
		name := t.topic or { continue }
		if !include_internal && name.starts_with('__') {
			continue
		}
		mut parts := []kmsg.PartitionDetail{cap: t.partitions.len}
		for p in t.partitions {
			parts << kmsg.PartitionDetail{
				partition: p.partition
				leader:    p.leader
				replicas:  p.replicas.clone()
				isr:       p.isr.clone()
			}
		}
		parts.sort(a.partition < b.partition)
		out << kmsg.TopicDetail{
			topic:      name
			topic_id:   t.topic_id
			internal:   t.is_internal
			partitions: parts
		}
	}
	out.sort(a.topic < b.topic)
	return out
}

// create_partitions grows a topic to total partitions.
pub fn (mut a Admin) create_partitions(topic string, total int) ! {
	mut c := a.client
	mut req := kmsg.CreatePartitionsRequest{
		timeout_millis: 15000
		topics:         [
			kmsg.CreatePartitionsRequestTopic{
				topic: topic
				count: total
			},
		]
	}
	body := c.request(mut req)!
	mut resp := kmsg.CreatePartitionsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	for t in resp.topics {
		if t.error_code != 0 {
			e := kmsg.error_for_code(t.error_code) or { kmsg.unknown_server_error }
			return error('create partitions ${t.topic}: ${e.msg()}')
		}
	}
}

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

// ConfigEntry is one configuration key/value.
pub struct ConfigEntry {
pub:
	name       string
	value      ?string
	read_only  bool
	is_default bool
}

const resource_type_topic = i8(2)

// describe_topic_configs returns a topic's configuration, sorted by name.
pub fn (mut a Admin) describe_topic_configs(topic string) ![]ConfigEntry {
	mut c := a.client
	mut req := kmsg.DescribeConfigsRequest{
		resources: [
			kmsg.DescribeConfigsRequestResource{
				resource_type: resource_type_topic
				resource_name: topic
			},
		]
	}
	body := c.request(mut req)!
	mut resp := kmsg.DescribeConfigsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	mut out := []kmsg.ConfigEntry{}
	for res in resp.resources {
		if res.error_code != 0 {
			e := kmsg.error_for_code(res.error_code) or { kmsg.unknown_server_error }
			return error('describe configs ${topic}: ${e.msg()}')
		}
		for cfg in res.configs {
			out << kmsg.ConfigEntry{
				name:       cfg.name
				value:      cfg.value
				read_only:  cfg.read_only
				is_default: cfg.is_default
			}
		}
	}
	out.sort(a.name < b.name)
	return out
}

// alter_topic_configs incrementally sets and deletes topic configs.
pub fn (mut a Admin) alter_topic_configs(topic string, set map[string]string, del []string) ! {
	mut c := a.client
	mut res := kmsg.IncrementalAlterConfigsRequestResource{
		resource_type: resource_type_topic
		resource_name: topic
	}
	mut names := set.keys()
	names.sort()
	for name in names {
		res.configs << kmsg.IncrementalAlterConfigsRequestResourceConfig{
			name:  name
			op:    kmsg.incremental_alter_config_op_set
			value: set[name]
		}
	}
	for name in del {
		res.configs << kmsg.IncrementalAlterConfigsRequestResourceConfig{
			name: name
			op:   kmsg.incremental_alter_config_op_delete
		}
	}
	mut req := kmsg.IncrementalAlterConfigsRequest{
		resources: [res]
	}
	body := c.request(mut req)!
	mut resp := kmsg.IncrementalAlterConfigsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	for rres in resp.resources {
		if rres.error_code != 0 {
			e := kmsg.error_for_code(rres.error_code) or { kmsg.unknown_server_error }
			return error('alter configs ${topic}: ${e.msg()}')
		}
	}
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

// find_group_coordinator locates a group's coordinator node.
fn (mut a Admin) find_group_coordinator(group string) !int {
	mut c := a.client
	mut req := kmsg.FindCoordinatorRequest{
		coordinator_key:  group
		coordinator_type: 0
	}
	body := c.request_capped(mut req, 3)!
	mut resp := kmsg.FindCoordinatorResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	if resp.error_code != 0 {
		e := kmsg.error_for_code(resp.error_code) or { kmsg.unknown_server_error }
		return error('find coordinator: ${e.msg()}')
	}
	return resp.node_id
}

// GroupListing is one group from list_groups.
pub struct GroupListing {
pub:
	group         string
	protocol_type string
	state         string
}

// list_groups lists groups across all known brokers, sorted.
pub fn (mut a Admin) list_groups() ![]GroupListing {
	mut c := a.client
	if c.known_brokers().len == 0 {
		c.metadata([])!
	}
	mut seen := map[string]bool{}
	mut out := []kmsg.GroupListing{}
	for node in c.known_brokers() {
		mut req := kmsg.ListGroupsRequest{}
		body := c.request_broker(node, mut req)!
		mut resp := kmsg.ListGroupsResponse{
			version: req.version
		}
		mut r := kmsg.Reader{
			src: body
		}
		resp.read_from(mut r)!
		for g in resp.groups {
			if g.group in seen {
				continue
			}
			seen[g.group] = true
			out << kmsg.GroupListing{
				group:         g.group
				protocol_type: g.protocol_type
				state:         g.group_state
			}
		}
	}
	out.sort(a.group < b.group)
	return out
}

// GroupMember is one member of a described group.
pub struct GroupMember {
pub:
	member_id   string
	client_id   string
	client_host string
	assigned    map[string][]int // topic -> partitions
}

// GroupDescription is one group's full state.
pub struct GroupDescription {
pub:
	group         string
	state         string
	protocol_type string
	protocol      string
	members       []kmsg.GroupMember
}

// describe_group describes one group, decoding member assignments.
pub fn (mut a Admin) describe_group(group string) !GroupDescription {
	mut c := a.client
	coordinator := a.find_group_coordinator(group)!
	mut req := kmsg.DescribeGroupsRequest{
		groups: [group]
	}
	body := c.request_broker(coordinator, mut req)!
	mut resp := kmsg.DescribeGroupsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	if resp.groups.len == 0 {
		return error('group ${group}: not found')
	}
	g := resp.groups[0]
	if g.error_code != 0 {
		e := kmsg.error_for_code(g.error_code) or { kmsg.unknown_server_error }
		return error('describe group ${group}: ${e.msg()}')
	}
	mut members := []kmsg.GroupMember{cap: g.members.len}
	for m in g.members {
		mut assigned := map[string][]int{}
		if m.member_assignment.len > 0 {
			mut asg := kmsg.ConsumerMemberAssignment{}
			mut ar := kmsg.Reader{
				src: m.member_assignment
			}
			asg.read_from(mut ar) or { kmsg.ConsumerMemberAssignment{} }
			for t in asg.topics {
				assigned[t.topic] = t.partitions.clone()
			}
		}
		members << kmsg.GroupMember{
			member_id:   m.member_id
			client_id:   m.client_id
			client_host: m.client_host
			assigned:    assigned
		}
	}
	members.sort(a.member_id < b.member_id)
	return kmsg.GroupDescription{
		group:         g.group
		state:         g.state
		protocol_type: g.protocol_type
		protocol:      g.protocol
		members:       members
	}
}

// delete_groups deletes groups (they must be empty), one result each.
pub fn (mut a Admin) delete_groups(groups []string) ![]OpResult {
	mut c := a.client
	mut out := []kmsg.OpResult{}
	for group in groups {
		coordinator := a.find_group_coordinator(group)!
		mut req := kmsg.DeleteGroupsRequest{
			groups: [group]
		}
		body := c.request_broker(coordinator, mut req)!
		mut resp := kmsg.DeleteGroupsResponse{
			version: req.version
		}
		mut r := kmsg.Reader{
			src: body
		}
		resp.read_from(mut r)!
		for g in resp.groups {
			out << code_result(g.group, g.error_code)
		}
	}
	out.sort(a.name < b.name)
	return out
}

// ---------------------------------------------------------------------------
// Offsets and lag
// ---------------------------------------------------------------------------

// fetch_group_offsets returns a group's committed offsets as
// 'topic/partition' -> offset (only committed entries).
pub fn (mut a Admin) fetch_group_offsets(group string) !map[string]i64 {
	mut c := a.client
	coordinator := a.find_group_coordinator(group)!
	mut req := kmsg.OffsetFetchRequest{
		group: group
	}
	// none topics = all topics; v8+ switches to the multi-group shape
	body := c.request_broker_capped(coordinator, mut req, 7)!
	mut resp := kmsg.OffsetFetchResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	mut out := map[string]i64{}
	for t in resp.topics {
		for p in t.partitions {
			if p.offset >= 0 {
				out['${t.topic}/${p.partition}'] = p.offset
			}
		}
	}
	return out
}

// list_offsets_at resolves every partition of the topics at ts (-2
// earliest, -1 latest) as 'topic/partition' -> offset.
fn (mut a Admin) list_offsets_at(topics []string, ts i64) !map[string]i64 {
	mut c := a.client
	c.metadata(topics)!
	mut out := map[string]i64{}
	for topic in topics {
		nparts := c.partition_count(topic) or { continue }
		mut by_leader := map[int][]int{}
		for p in 0 .. nparts {
			leader := c.partition_leader(topic, p) or { continue }
			by_leader[leader] << p
		}
		for leader, parts in by_leader {
			offsets := c.list_offsets(leader, topic, parts, ts)!
			for p, off in offsets {
				out['${topic}/${p}'] = off
			}
		}
	}
	return out
}

// list_end_offsets returns each partition's high watermark.
pub fn (mut a Admin) list_end_offsets(topics []string) !map[string]i64 {
	return a.list_offsets_at(topics, -1)
}

// list_start_offsets returns each partition's log start offset.
pub fn (mut a Admin) list_start_offsets(topics []string) !map[string]i64 {
	return a.list_offsets_at(topics, -2)
}

// LagEntry is one partition's consumer-group lag.
pub struct LagEntry {
pub:
	topic     string
	partition int
	committed i64 // -1 if nothing committed
	end       i64
	lag       i64
}

// group_lag computes lag for every partition the group has offsets or
// subscriptions for: end offset minus committed (uncommitted partitions
// lag by the full log).
pub fn (mut a Admin) group_lag(group string) ![]LagEntry {
	committed := a.fetch_group_offsets(group)!
	// topics involved: from committed keys plus described assignments
	mut topics := map[string]bool{}
	for key, _ in committed {
		idx := key.last_index('/') or { continue }
		topics[key[..idx]] = true
	}
	desc := a.describe_group(group) or { kmsg.GroupDescription{} }
	for m in desc.members {
		for topic, _ in m.assigned {
			topics[topic] = true
		}
	}
	mut names := topics.keys()
	names.sort()
	ends := a.list_end_offsets(names)!
	mut out := []kmsg.LagEntry{}
	for key, end in ends {
		idx := key.last_index('/') or { continue }
		committed_off := committed[key] or { i64(-1) }
		base := if committed_off >= 0 { committed_off } else { i64(0) }
		out << kmsg.LagEntry{
			topic:     key[..idx]
			partition: key[idx + 1..].int()
			committed: committed_off
			end:       end
			lag:       end - base
		}
	}
	out.sort_with_compare(fn (a &LagEntry, b &LagEntry) int {
		if a.topic != b.topic {
			return compare_strings(a.topic, b.topic)
		}
		return a.partition - b.partition
	})
	return out
}

// delete_records truncates a partition: records below before_offset are
// deleted. Returns the new low watermark.
pub fn (mut a Admin) delete_records(topic string, partition int, before_offset i64) !i64 {
	mut c := a.client
	c.metadata([topic])!
	leader := c.partition_leader(topic, partition) or {
		return error('no leader for ${topic}[${partition}]')
	}
	mut req := kmsg.DeleteRecordsRequest{
		timeout_millis: 15000
		topics:         [
			kmsg.DeleteRecordsRequestTopic{
				topic:      topic
				partitions: [
					kmsg.DeleteRecordsRequestTopicPartition{
						partition: partition
						offset:    before_offset
					},
				]
			},
		]
	}
	body := c.request_broker(leader, mut req)!
	mut resp := kmsg.DeleteRecordsResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	for t in resp.topics {
		for p in t.partitions {
			if p.error_code != 0 {
				e := kmsg.error_for_code(p.error_code) or { kmsg.unknown_server_error }
				return error('delete records ${topic}[${p.partition}]: ${e.msg()}')
			}
			return p.low_watermark
		}
	}
	return error('delete records: empty response')
}
