// Group balancers: compute partition assignments from member
// subscriptions. Range and RoundRobin match Kafka's classic eager
// assignors; cooperative-sticky comes later per the port plan.
module kgo

// BalancerKind names a supported assignment strategy; the order in
// GroupOpts.balancers is the preference order offered on JoinGroup.
pub enum BalancerKind {
	range_bal
	round_robin
}

// protocol_name is the strategy name on the wire (must match other
// clients' names for mixed-language groups).
fn (b BalancerKind) protocol_name() string {
	return match b {
		.range_bal { 'range' }
		.round_robin { 'roundrobin' }
	}
}

fn balancer_for_name(name string) ?BalancerKind {
	return match name {
		'range' { BalancerKind.range_bal }
		'roundrobin' { BalancerKind.round_robin }
		else { none }
	}
}

// MemberSubscription is one member's topic interests, as decoded from its
// JoinGroup metadata.
pub struct MemberSubscription {
pub mut:
	member_id string
	topics    []string
}

// balance computes member -> topic -> partitions for the given
// subscriptions and per-topic partition counts.
fn balance(kind BalancerKind, members_ []MemberSubscription, partitions map[string]int) map[string]map[string][]int {
	mut members := members_.clone()
	members.sort(a.member_id < b.member_id)
	mut out := map[string]map[string][]int{}
	for m in members {
		out[m.member_id] = map[string][]int{}
	}

	mut topics := partitions.keys()
	topics.sort()

	match kind {
		.range_bal {
			// per topic: sorted subscribers; the first npart % nsub
			// members get one extra partition (Kafka range semantics)
			for topic in topics {
				mut subs := []string{}
				for m in members {
					if topic in m.topics {
						subs << m.member_id
					}
				}
				if subs.len == 0 {
					continue
				}
				npart := partitions[topic]
				base := npart / subs.len
				extra := npart % subs.len
				mut p := 0
				for i, sub in subs {
					mut count := base
					if i < extra {
						count++
					}
					for _ in 0 .. count {
						out[sub][topic] << p
						p++
					}
				}
			}
		}
		.round_robin {
			// all (topic, partition) pairs in order, dealt circularly to
			// the sorted members subscribed to each pair's topic
			mut cursor := 0
			for topic in topics {
				for p in 0 .. partitions[topic] {
					mut placed := false
					for try in 0 .. members.len {
						m := members[(cursor + try) % members.len]
						if topic in m.topics {
							out[m.member_id][topic] << p
							cursor = (cursor + try + 1) % members.len
							placed = true
							break
						}
					}
					if !placed {
						continue
					}
				}
			}
		}
	}

	return out
}
