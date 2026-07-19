// Group balancers: compute partition assignments from member
// subscriptions. Range and RoundRobin match Kafka's classic eager
// assignors; cooperative-sticky comes later per the port plan.
module kgo

// BalancerKind names a supported assignment strategy; the order in
// GroupOpts.balancers is the preference order offered on JoinGroup.
pub enum BalancerKind {
	range_bal
	round_robin
	cooperative_sticky
}

// protocol_name is the strategy name on the wire (must match other
// clients' names for mixed-language groups).
fn (b BalancerKind) protocol_name() string {
	return match b {
		.range_bal {
			'range'
		}
		.round_robin {
			'roundrobin'
		}
		.cooperative_sticky {
			'cooperative-sticky'
		}
	}
}

fn balancer_for_name(name string) ?BalancerKind {
	return match name {
		'range' { BalancerKind.range_bal }
		'roundrobin' { BalancerKind.round_robin }
		'cooperative-sticky' { BalancerKind.cooperative_sticky }
		else { none }
	}
}

// MemberSubscription is one member's topic interests, as decoded from its
// JoinGroup metadata.
pub struct MemberSubscription {
pub mut:
	member_id string
	topics    []string
	// owned are the partitions the member currently holds (cooperative
	// protocols), from its join metadata.
	owned map[string][]int
	// generation the member last joined in; resolves duplicate ownership
	// claims (higher wins).
	generation int = -1
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
		.cooperative_sticky {
			cooperative_sticky_balance(members, partitions, mut out)
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

// cooperative_sticky_balance: a deterministic sticky assignor honoring the
// KIP-429 cooperative constraint — a partition never transfers directly
// between members in one generation. Simplifications vs the Java
// implementation (which is ~1500 lines): targets come from a greedy ideal
// distribution, kept partitions are the lowest-numbered owned ones, and
// tie-breaks are lexicographic. The invariants that matter hold: sticky
// where balance allows, max/min spread converges to <= 1 across
// generations, and moving partitions sit out exactly one generation.
fn cooperative_sticky_balance(members []MemberSubscription, partitions map[string]int, mut out map[string]map[string][]int) {
	mut topics := partitions.keys()
	topics.sort()

	// 1. ideal target count per member: deal every partition to the
	// least-loaded subscriber (stable order) as if from scratch
	mut target := map[string]int{}
	for m in members {
		target[m.member_id] = 0
	}
	for topic in topics {
		for _ in 0 .. partitions[topic] {
			mut best := ''
			for m in members {
				if topic !in m.topics {
					continue
				}
				if best == '' || target[m.member_id] < target[best] {
					best = m.member_id
				}
			}
			if best != '' {
				target[best]++
			}
		}
	}

	// 2. resolve ownership claims; higher generation wins a contested
	// partition, then earlier member id
	mut claim := map[string]string{}
	mut claim_gen := map[string]int{}
	for m in members {
		mut owned_topics := m.owned.keys()
		owned_topics.sort()
		for topic in owned_topics {
			if topic !in partitions || topic !in m.topics {
				continue
			}
			for p in m.owned[topic] {
				if p < 0 || p >= partitions[topic] {
					continue
				}
				key := '${topic}/${p}'
				g := claim_gen[key] or { -2 }
				if key !in claim || m.generation > g {
					claim[key] = m.member_id
					claim_gen[key] = m.generation
				}
			}
		}
	}

	// 3. sticky keep, capped at target: lowest partition numbers stay,
	// the excess is revoked (assigned to nobody this generation)
	mut counts := map[string]int{}
	for m in members {
		counts[m.member_id] = 0
	}
	for m in members {
		mut kept := 0
		for topic in topics {
			for p in 0 .. partitions[topic] {
				if claim['${topic}/${p}'] or { '' } != m.member_id {
					continue
				}
				if kept >= target[m.member_id] {
					continue // revoked: cools for one generation
				}
				out[m.member_id][topic] << p
				kept++
			}
		}
		counts[m.member_id] = kept
	}

	// 4. unowned partitions go to subscribers below target
	for topic in topics {
		for p in 0 .. partitions[topic] {
			if '${topic}/${p}' in claim {
				continue
			}
			mut best := ''
			for m in members {
				if topic !in m.topics || counts[m.member_id] >= target[m.member_id] {
					continue
				}
				if best == '' || counts[m.member_id] < counts[best] {
					best = m.member_id
				}
			}
			if best != '' {
				out[best][topic] << p
				counts[best]++
			}
		}
	}
}
