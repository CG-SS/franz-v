// Consumer groups: the classic (eager) group protocol. A GroupConsumer
// finds its coordinator, joins with balancer preferences, syncs (the
// elected leader computes assignments), heartbeats inside poll, fetches
// through an inner direct Consumer positioned at committed offsets, and
// rejoins whenever the coordinator signals a rebalance.
module kgo

import kbin
import kerr
import kmsg
import time

// Kafka error codes the group protocol reacts to by name.
const ec_offset_out_of_range = i16(1)
const ec_coordinator_load = i16(14)
const ec_coordinator_not_available = i16(15)
const ec_not_coordinator = i16(16)
const ec_illegal_generation = i16(22)
const ec_unknown_member = i16(25)
const ec_rebalance_in_progress = i16(27)
const ec_member_id_required = i16(79)

// GroupOpts configures a GroupConsumer.
pub struct GroupOpts {
pub mut:
	// balancers is the assignment-strategy preference order offered on
	// join; the coordinator picks the first supported by all members.
	balancers []kmsg.BalancerKind = [kmsg.BalancerKind.range_bal, .round_robin]
	// session_timeout is how long the coordinator waits for a heartbeat
	// before evicting the member.
	session_timeout time.Duration = 45 * time.second
	// rebalance_timeout is how long the coordinator waits for members to
	// rejoin during a rebalance.
	rebalance_timeout time.Duration = 60 * time.second
	// heartbeat_interval is how often poll sends heartbeats.
	heartbeat_interval time.Duration = 3 * time.second
	// start is where partitions without a committed offset begin.
	start kmsg.StartOffset = .earliest
}

// GroupConsumer consumes topics as a member of a consumer group. Obtain
// one from Client.new_group_consumer; call poll in a loop, commit when
// appropriate, and close to leave the group cleanly.
//
// Give each group member its own Client: JoinGroup is a long-poll the
// coordinator can hold for the whole rebalance window, and franz-v issues
// requests strictly in order per broker connection (no pipelining yet), so
// two members sharing a client can serialize behind each other's joins.
@[heap]
pub struct GroupConsumer {
pub mut:
	client &kmsg.Client
	group  string
	topics []string
	opts   kmsg.GroupOpts
mut:
	coordinator    int = -2147483648 // none
	member_id      string
	generation     int = -1
	assignment     map[string][]int // topic -> partitions
	inner          ?&kmsg.Consumer
	needs_rejoin   bool = true
	last_heartbeat i64 // unix microseconds
}

// new_group_consumer creates a group member for the given group and
// topics. The group is joined lazily on the first poll.
pub fn (mut c Client) new_group_consumer(group string, topics []string, opts GroupOpts) !&GroupConsumer {
	if group == '' {
		return error('group id must be non-empty')
	}
	if topics.len == 0 {
		return error('at least one topic is required')
	}
	c.metadata(topics)!
	return &kmsg.GroupConsumer{
		client: c
		group:  group
		topics: topics
		opts:   opts
	}
}

// assigned returns the currently assigned partitions per topic (empty
// before the first successful join).
pub fn (mut g GroupConsumer) assigned() map[string][]int {
	return g.assignment.clone()
}

// member returns the coordinator-assigned member id (empty before join).
pub fn (mut g GroupConsumer) member() string {
	return g.member_id
}

// ---------------------------------------------------------------------------
// Coordinator discovery
// ---------------------------------------------------------------------------

fn (mut g GroupConsumer) find_coordinator() ! {
	mut c := g.client
	mut req := kmsg.FindCoordinatorRequest{
		coordinator_key:  g.group
		coordinator_type: 0 // group
	}
	// v4+ batches coordinator keys; stay on the single-key shape
	// the coordinator's backing topic is created lazily; retry while the
	// broker reports it not yet available
	for _ in 0 .. 40 {
		body := c.request_capped(mut req, 3)!
		mut resp := kmsg.FindCoordinatorResponse{
			version: req.version
		}
		mut r := kmsg.Reader{
			src: body
		}
		resp.read_from(mut r)!
		match resp.error_code {
			0 {
				g.coordinator = resp.node_id
				c.cfg.log(.debug, 'group ${g.group}: coordinator is node ${resp.node_id}')
				return
			}
			ec_coordinator_not_available, ec_coordinator_load {
				time.sleep(250 * time.millisecond)
			}
			else {
				e := kmsg.error_for_code(resp.error_code) or { kmsg.unknown_server_error }
				return error('find coordinator: ${e.msg()}')
			}
		}
	}
	return error('find coordinator: not available after retries')
}

// request_capped is request() with a version cap, for shape-changing APIs
// (coordinator lookups, raw fetches to brokers without known topic ids).
pub fn (mut c Client) request_capped(mut req Request, cap i16) ![]u8 {
	mut attempt := 0
	for {
		mut b := c.any_broker() or { return kmsg.NoBrokersError{} }
		res := c.request_on(mut b, mut req, cap)
		if body := res.body {
			return body
		}
		if !res.retriable || attempt >= c.cfg.request_retries {
			return error(res.err_msg)
		}
		attempt++
		time.sleep(c.cfg.backoff_for(attempt - 1))
		if c.cancel.is_done() {
			return kmsg.ClientClosedError{}
		}
	}
	return kmsg.NoBrokersError{}
}

// ---------------------------------------------------------------------------
// Join / Sync
// ---------------------------------------------------------------------------

fn (g &GroupConsumer) member_metadata() []u8 {
	mut meta := kmsg.ConsumerMemberMetadata{
		version:    2 // v1 adds owned_partitions, v2 the generation
		topics:     g.topics.clone()
		generation: g.generation
	}
	mut owned_topics := g.assignment.keys()
	owned_topics.sort()
	for t in owned_topics {
		meta.owned_partitions << kmsg.ConsumerMemberMetadataOwnedPartition{
			topic:      t
			partitions: g.assignment[t].clone()
		}
	}
	mut w := kmsg.Writer{}
	meta.write_to(mut w)
	return w.buf
}

// join_and_sync runs the JoinGroup / SyncGroup cycle until membership is
// established, then positions the inner consumer at committed offsets.
fn (mut g GroupConsumer) join_and_sync() ! {
	if g.coordinator == -2147483648 {
		g.find_coordinator()!
	}
	mut c := g.client
	mut tries := 0
	for {
		tries++
		if tries > 30 {
			return error('group ${g.group}: could not join after ${tries - 1} attempts')
		}
		mut protocols := []kmsg.JoinGroupRequestProtocol{}
		for b in g.opts.balancers {
			protocols << kmsg.JoinGroupRequestProtocol{
				name:     b.protocol_name()
				metadata: g.member_metadata()
			}
		}
		mut jreq := kmsg.JoinGroupRequest{
			group:                    g.group
			session_timeout_millis:   int(i64(g.opts.session_timeout) / 1000000)
			rebalance_timeout_millis: int(i64(g.opts.rebalance_timeout) / 1000000)
			member_id:                g.member_id
			protocol_type:            'consumer'
			protocols:                protocols
		}
		jbody := c.request_broker(g.coordinator, mut jreq) or {
			g.coordinator = -2147483648
			g.find_coordinator()!
			continue
		}
		mut jresp := kmsg.JoinGroupResponse{
			version: jreq.version
		}
		mut jr := kmsg.Reader{
			src: jbody
		}
		jresp.read_from(mut jr)!

		match jresp.error_code {
			0 {}
			ec_member_id_required {
				g.member_id = jresp.member_id
				continue
			}
			ec_unknown_member {
				g.member_id = ''
				continue
			}
			ec_coordinator_load {
				time.sleep(100 * time.millisecond)
				continue
			}
			ec_coordinator_not_available, ec_not_coordinator {
				g.coordinator = -2147483648
				g.find_coordinator()!
				continue
			}
			ec_rebalance_in_progress {
				continue
			}
			else {
				e := kmsg.error_for_code(jresp.error_code) or { kmsg.unknown_server_error }
				return error('join group: ${e.msg()}')
			}
		}

		g.generation = jresp.generation
		g.member_id = jresp.member_id
		chosen_name := jresp.protocol or { g.opts.balancers[0].protocol_name() }
		is_leader := jresp.leader_id == g.member_id
		c.cfg.log(.info,
			'group ${g.group}: joined generation ${g.generation} as ${g.member_id} (leader: ${is_leader}, protocol: ${chosen_name})')

		mut sreq := kmsg.SyncGroupRequest{
			group:      g.group
			generation: g.generation
			member_id:  g.member_id
			// v5+ validates these against the group's chosen protocol;
			// Kafka 4.x's coordinator requires them
			protocol_type: 'consumer'
			protocol:      chosen_name
		}
		if is_leader {
			sreq.group_assignment = g.lead_assignments(chosen_name, jresp.members)!
		}
		sbody := c.request_broker(g.coordinator, mut sreq) or {
			g.coordinator = -2147483648
			g.find_coordinator()!
			continue
		}
		mut sresp := kmsg.SyncGroupResponse{
			version: sreq.version
		}
		mut sr := kmsg.Reader{
			src: sbody
		}
		sresp.read_from(mut sr)!
		match sresp.error_code {
			0 {}
			ec_rebalance_in_progress, ec_illegal_generation {
				continue
			}
			ec_unknown_member {
				g.member_id = ''
				continue
			}
			else {
				e := kmsg.error_for_code(sresp.error_code) or { kmsg.unknown_server_error }
				return error('sync group: ${e.msg()}')
			}
		}

		mut assigned := kmsg.ConsumerMemberAssignment{}
		mut ar := kmsg.Reader{
			src: sresp.member_assignment
		}
		assigned.read_from(mut ar)!
		mut new_assignment := map[string][]int{}
		for t in assigned.topics {
			new_assignment[t.topic] = t.partitions.clone()
		}
		c.cfg.log(.info, 'group ${g.group}: assigned ${new_assignment}')

		if chosen_name == 'cooperative-sticky' {
			// keep cursors of retained partitions: the sticky payoff is
			// no offset refetch and no duplicate consumption
			mut preserve := map[string]i64{}
			mut revoked := 0
			if mut inner := g.inner {
				for topic, parts in g.assignment {
					for p in parts {
						key := cursor_key(topic, p)
						retained := p in (new_assignment[topic] or { []int{} })
						if retained {
							if cur := inner.cursors[key] {
								preserve[key] = cur
							}
						} else {
							revoked++
						}
					}
				}
			}
			g.assignment = new_assignment.clone()
			g.position_at_committed(preserve)!
			g.needs_rejoin = false
			g.last_heartbeat = time.now().unix_micro()
			if revoked > 0 {
				// cooperative constraint: revoked partitions were assigned
				// to nobody this generation; rejoin (advertising reduced
				// ownership) so the next rebalance can place them
				c.cfg.log(.info,
					'group ${g.group}: cooperative revoke of ${revoked} partition(s), rejoining')
				continue
			}
			return
		}
		g.assignment = new_assignment.clone()
		g.position_at_committed(map[string]i64{})!
		g.needs_rejoin = false
		g.last_heartbeat = time.now().unix_micro()
		return
	}
}

// lead_assignments runs the chosen balancer over all members' decoded
// subscriptions (leader only).
fn (mut g GroupConsumer) lead_assignments(chosen_name string, members []JoinGroupResponseMember) ![]SyncGroupRequestGroupAssignment {
	kind := balancer_for_name(chosen_name) or {
		return error('coordinator chose unsupported protocol ${chosen_name}')
	}
	mut c := g.client
	mut subs := []kmsg.MemberSubscription{cap: members.len}
	mut all_topics := map[string]bool{}
	for m in members {
		mut meta := kmsg.ConsumerMemberMetadata{}
		mut r := kmsg.Reader{
			src: m.protocol_metadata
		}
		meta.read_from(mut r)!
		mut owned := map[string][]int{}
		for op in meta.owned_partitions {
			owned[op.topic] = op.partitions.clone()
		}
		subs << kmsg.MemberSubscription{
			member_id:  m.member_id
			topics:     meta.topics.clone()
			owned:      owned
			generation: meta.generation
		}
		for t in meta.topics {
			all_topics[t] = true
		}
	}
	mut topics := all_topics.keys()
	topics.sort()
	c.metadata(topics)!
	mut counts := map[string]int{}
	for t in topics {
		counts[t] = c.partition_count(t) or { 0 }
	}

	plan := balance(kind, subs, counts)
	mut out := []kmsg.SyncGroupRequestGroupAssignment{cap: subs.len}
	for member_id, per_topic in plan {
		mut asg := kmsg.ConsumerMemberAssignment{}
		mut ts := per_topic.keys()
		ts.sort()
		for t in ts {
			asg.topics << kmsg.ConsumerMemberAssignmentTopic{
				topic:      t
				partitions: per_topic[t].clone()
			}
		}
		mut w := kmsg.Writer{}
		asg.write_to(mut w)
		out << kmsg.SyncGroupRequestGroupAssignment{
			member_id:         member_id
			member_assignment: w.buf
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------

// position_at_committed builds the inner consumer with cursors at the
// group's committed offsets, falling back to opts.start where none exist.
// Cursors in preserve win over committed offsets (cooperative retention).
fn (mut g GroupConsumer) position_at_committed(preserve map[string]i64) ! {
	mut c := g.client
	mut req := kmsg.OffsetFetchRequest{
		group: g.group
	}
	mut req_topics := []kmsg.OffsetFetchRequestTopic{}
	mut topics := g.assignment.keys()
	topics.sort()
	for t in topics {
		req_topics << kmsg.OffsetFetchRequestTopic{
			topic:      t
			partitions: g.assignment[t].clone()
		}
	}
	req.topics = req_topics
	// v8+ switches to a per-group array shape; stay on the single-group form
	body := c.request_broker_capped(g.coordinator, mut req, 7)!
	mut resp := kmsg.OffsetFetchResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!

	mut cursors := map[string]i64{}
	mut missing := map[int][]int{} // leader -> partitions per topic handled below
	_ = missing
	for t in resp.topics {
		for p in t.partitions {
			if p.error_code != 0 {
				e := kmsg.error_for_code(p.error_code) or { kmsg.unknown_server_error }
				return error('offset fetch ${t.topic}[${p.partition}]: ${e.msg()}')
			}
			if p.offset >= 0 {
				cursors[cursor_key(t.topic, p.partition)] = p.offset
			}
		}
	}
	// resolve uncommitted partitions per start policy
	ts := if g.opts.start == .earliest { i64(-2) } else { i64(-1) }
	for topic, parts in g.assignment {
		mut by_leader := map[int][]int{}
		for p in parts {
			if cursor_key(topic, p) in cursors {
				continue
			}
			leader := c.leader_for(topic, p)!
			by_leader[leader] << p
		}
		for leader, ps in by_leader {
			offsets := c.list_offsets(leader, topic, ps, ts)!
			for p, off in offsets {
				cursors[cursor_key(topic, p)] = off
			}
		}
	}
	for k, v in preserve {
		cursors[k] = v
	}
	g.inner = &kmsg.Consumer{
		client:  c
		cursors: cursors
	}
}

// positions returns the current per-partition cursor positions
// ('topic/partition' -> next offset), e.g. for TxnProducer.send_offsets.
pub fn (mut g GroupConsumer) positions() map[string]i64 {
	mut inner := g.inner or { return map[string]i64{} }
	return inner.cursors.clone()
}

// commit commits the inner consumer's current positions to the group.
pub fn (mut g GroupConsumer) commit() ! {
	mut inner := g.inner or { return error('not joined; nothing to commit') }
	mut c := g.client
	mut per_topic := map[string][]kmsg.OffsetCommitRequestTopicPartition{}
	for key, next in inner.cursors {
		idx := key.last_index('/') or { continue }
		topic := key[..idx]
		per_topic[topic] << kmsg.OffsetCommitRequestTopicPartition{
			partition: key[idx + 1..].int()
			offset:    next
		}
	}
	mut req := kmsg.OffsetCommitRequest{
		group:      g.group
		generation: g.generation
		member_id:  g.member_id
	}
	mut topics := per_topic.keys()
	topics.sort()
	// OffsetCommit v10+ addresses topics by uuid (KIP-516): usable once
	// every topic's id is known, else stay on v9 names
	mut cap := i16(-1)
	for t in topics {
		mut rt := kmsg.OffsetCommitRequestTopic{
			topic:      t
			partitions: per_topic[t]
		}
		if id := c.topic_id(t) {
			rt.topic_id = id
		} else {
			cap = 9
		}
		req.topics << rt
	}
	body := c.request_broker_capped(g.coordinator, mut req, cap)!
	mut resp := kmsg.OffsetCommitResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	for t in resp.topics {
		for p in t.partitions {
			if p.error_code != 0 {
				if p.error_code in [ec_illegal_generation, ec_unknown_member,
					ec_rebalance_in_progress] {
					g.needs_rejoin = true
				}
				e := kmsg.error_for_code(p.error_code) or { kmsg.unknown_server_error }
				return error('offset commit ${t.topic}[${p.partition}]: ${e.msg()}')
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Heartbeat / poll / close
// ---------------------------------------------------------------------------

fn (mut g GroupConsumer) maybe_heartbeat() ! {
	now := time.now().unix_micro()
	if now - g.last_heartbeat < i64(g.opts.heartbeat_interval) / 1000 {
		return
	}
	g.last_heartbeat = now
	mut c := g.client
	mut req := kmsg.HeartbeatRequest{
		group:      g.group
		generation: g.generation
		member_id:  g.member_id
	}
	body := c.request_broker(g.coordinator, mut req) or {
		g.coordinator = -2147483648
		g.needs_rejoin = true
		return
	}
	mut resp := kmsg.HeartbeatResponse{
		version: req.version
	}
	mut r := kmsg.Reader{
		src: body
	}
	resp.read_from(mut r)!
	match resp.error_code {
		0 {}
		ec_rebalance_in_progress, ec_illegal_generation {
			c.cfg.log(.info, 'group ${g.group}: rebalance signaled, rejoining')
			g.needs_rejoin = true
		}
		ec_unknown_member {
			g.member_id = ''
			g.needs_rejoin = true
		}
		ec_coordinator_not_available, ec_not_coordinator {
			g.coordinator = -2147483648
			g.needs_rejoin = true
		}
		else {
			e := kmsg.error_for_code(resp.error_code) or { kmsg.unknown_server_error }
			return error('heartbeat: ${e.msg()}')
		}
	}
}

// poll joins/rejoins the group as needed, heartbeats, and fetches one
// round from the assigned partitions.
pub fn (mut g GroupConsumer) poll() ![]Record {
	if g.needs_rejoin {
		g.join_and_sync()!
	}
	g.maybe_heartbeat()!
	if g.needs_rejoin {
		// a rebalance was signaled: rejoin now so this poll fetches with
		// the new assignment
		g.join_and_sync()!
	}
	mut inner := g.inner or { return error('group consumer has no assignment') }
	return inner.poll()
}

// close leaves the group so partitions rebalance to remaining members
// promptly.
pub fn (mut g GroupConsumer) close() {
	if g.member_id == '' {
		return
	}
	mut c := g.client
	mut req := kmsg.LeaveGroupRequest{
		group:     g.group
		member_id: g.member_id
	}
	// v3+ switches to a members array; the single-member shape suffices
	c.request_broker_capped(g.coordinator, mut req, 2) or {}
	g.member_id = ''
	g.needs_rejoin = true
}
