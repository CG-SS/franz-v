// Module kversion specifies versions for Kafka request keys; a V
// implementation of franz-go's pkg/kversion.
//
// Kafka technically has internal broker versions that bump multiple times
// per release; this package only defines releases and tip. Release history
// lives in tables.v as delta data extracted from the franz-go source.
module kversion

import kmsg

// Req is one API key's supported version range.
pub struct Req {
pub mut:
	key  i16
	vmin i16
	vmax i16
}

// Versions is a list of versions, with each item corresponding to a Kafka
// key and each item's value corresponding to the max version supported.
pub struct Versions {
pub mut:
	reqs map[i16]Req
}

struct Release {
	name  string
	major int
	reqs  map[i16]Req
}

// build_chain folds each release's deltas over its predecessor, oldest
// first. Delta triples are (op, key, ver); see tables.v.
fn build_chain(specs []RelSpec) []Release {
	mut out := []Release{cap: specs.len}
	mut reqs := map[i16]Req{}
	for spec in specs {
		mut i := 0
		for i + 2 < spec.deltas.len + 1 {
			op := spec.deltas[i]
			key := spec.deltas[i + 1]
			ver := spec.deltas[i + 2]
			i += 3
			match op {
				0 { // add key at max ver
					reqs[key] = Req{
						key:  key
						vmax: ver
					}
				}
				1 { // bump key max to ver
					mut r := reqs[key]
					r.vmax = ver
					reqs[key] = r
				}
				2 { // set key min ver
					mut r := reqs[key]
					r.vmin = ver
					reqs[key] = r
				}
				3 { // delete key
					reqs.delete(key)
				}
				else {}
			}
		}
		out << Release{
			name:  spec.name
			major: spec.major
			reqs:  reqs.clone()
		}
	}
	return out
}

const zk_chain = build_chain(zk_specs[..])
const broker_chain = build_chain(broker_specs[..])
const controller_chain = build_chain(controller_specs[..])

fn chain_versions(chain []Release, name string) ?Versions {
	for rel in chain {
		if rel.name == name {
			return Versions{
				reqs: rel.reqs.clone()
			}
		}
	}
	return none
}

// merged returns a Versions merging the given releases in order: any key
// already present is kept, any missing key is added. Kafka 4.0+ merges the
// KRaft broker, KRaft controller, and ZooKeeper-era request sets this way.
fn merged(rels []Release) Versions {
	mut reqs := map[i16]Req{}
	for rel in rels {
		for k, req in rel.reqs {
			if k !in reqs {
				reqs[k] = req
			}
		}
	}
	return Versions{
		reqs: reqs
	}
}

// stable returns the latest released Kafka key versions. This is the
// default used by the client to avoid breaking tip changes.
pub fn stable() Versions {
	return merged([broker_chain.last(), controller_chain.last(),
		zk_chain.last()])
}

// tip returns the latest defined ZooKeeper-lineage key versions; this may
// be slightly out of date (matches franz-go's Tip).
pub fn tip() Versions {
	return merged([zk_chain.last()])
}

// version_strings returns all recognized release names usable as input to
// from_string: 4.x+ broker releases, then every ZooKeeper-era release,
// newest first.
pub fn version_strings() []string {
	mut out := []string{}
	for i := broker_chain.len - 1; i >= 0; i-- {
		if broker_chain[i].major >= 4 {
			out << broker_chain[i].name
		}
	}
	for i := zk_chain.len - 1; i >= 0; i-- {
		out << zk_chain[i].name
	}
	return out
}

// from_string returns the Versions for a release string:
// v0.#.# or v0.#.#.# for 0.x, v#.# or v#.#.# for 1.0+; the "v" is optional
// and patch versions are ignored.
pub fn from_string(v_ string) ?Versions {
	mut v := v_
	if v.starts_with('v') {
		v = v[1..]
	}
	parts := v.split('.')
	for p in parts {
		if !all_digits(p) {
			return none
		}
	}
	mut name := ''
	if parts.len >= 3 && parts[0] == '0' {
		// v0.#.# with optional trailing patch
		if parts.len > 4 {
			return none
		}
		name = 'v${parts[0]}.${parts[1]}.${parts[2]}'
	} else if parts.len >= 2 && parts[0] != '0' {
		// v#.# with optional trailing patch
		if parts.len > 3 {
			return none
		}
		name = 'v${parts[0]}.${parts[1]}'
	} else {
		return none
	}
	for chain in [broker_chain, zk_chain] {
		if vs := chain_versions(chain, name) {
			return vs
		}
	}
	return none
}

fn all_digits(s string) bool {
	if s == '' {
		return false
	}
	for c in s {
		if c < `0` || c > `9` {
			return false
		}
	}
	return true
}

// from_api_versions_response returns a Versions from a broker's
// ApiVersions response.
pub fn from_api_versions_response(r &kmsg.ApiVersionsResponse) Versions {
	mut reqs := map[i16]Req{}
	for k in r.api_keys {
		reqs[k.api_key] = Req{
			key:  k.api_key
			vmin: k.min_version
			vmax: k.max_version
		}
	}
	return Versions{
		reqs: reqs
	}
}

// has_key returns true if the versions contain the given key.
pub fn (vs &Versions) has_key(k i16) bool {
	return k in vs.reqs
}

// lookup_max_key_version returns the max version for the given key, or
// none if the key does not exist.
pub fn (vs &Versions) lookup_max_key_version(k i16) ?i16 {
	req := vs.reqs[k] or { return none }
	return req.vmax
}

// set_max_key_version sets the max version for the given key. Setting a
// version to -1 (or using a negative key) removes the key entirely.
pub fn (mut vs Versions) set_max_key_version(k i16, v i16) {
	if k < 0 || v < 0 {
		vs.reqs.delete(k)
		return
	}
	mut req := vs.reqs[k] or { Req{} }
	req.vmax = v
	req.key = k // in case the key did not exist
	vs.reqs[k] = req
}

// equal returns whether two versions contain exactly the same keys and
// version ranges.
pub fn (vs &Versions) equal(other &Versions) bool {
	if vs.reqs.len != other.reqs.len {
		return false
	}
	for k, req in vs.reqs {
		oreq := other.reqs[k] or { return false }
		if req != oreq {
			return false
		}
	}
	return true
}

// sorted_reqs returns every key's version range, sorted by key. (This
// replaces franz-go's callback-based EachMaxKeyVersion: V closures capture
// by value, so returning data is both safer and more idiomatic.)
pub fn (vs &Versions) sorted_reqs() []Req {
	mut keys := vs.reqs.keys()
	keys.sort()
	mut out := []Req{cap: keys.len}
	for k in keys {
		out << vs.reqs[k]
	}
	return out
}

// str returns a listing of API names and their max versions.
pub fn (vs &Versions) str() string {
	mut keys := vs.reqs.keys()
	keys.sort()
	mut lines := []string{cap: keys.len}
	for k in keys {
		lines << '${kmsg.name_for_key(k)}\t${vs.reqs[k].vmax}'
	}
	return lines.join('\n')
}

// GuessConfig configures version guessing; a plain struct with defaults
// replaces franz-go's functional options.
pub struct GuessConfig {
pub mut:
	// skip_keys are ignored while guessing. The defaults are
	// broker-to-broker or optional keys that most non-Kafka
	// implementations (and telemetry-disabled brokers) do not advertise:
	// LeaderAndISR(4), StopReplica(5), UpdateMetadata(6),
	// ControlledShutdown(7), WriteTxnMarkers(27), AlterISR(56),
	// UpdateFeatures(57), Envelope(58), AllocateProducerIDs(67),
	// GetTelemetrySubscriptions(71), PushTelemetry(72).
	skip_keys []i16 = [i16(4), 5, 6, 7, 27, 56, 57, 58, 67, 71, 72]
}

const guess_exact = u8(0)
const guess_at_least = u8(1)
const guess_custom_unknown = u8(2)
const guess_between = u8(3)
const guess_not_even = u8(4)

struct Guess {
	v1  string
	v2  string // for between
	how u8
}

fn (g Guess) str() string {
	return match g.how {
		guess_exact { g.v1 }
		guess_at_least { 'at least ${g.v1}' }
		guess_custom_unknown { 'unknown custom version' }
		guess_between { 'between ${g.v1} and ${g.v2}' }
		guess_not_even { 'not even ${g.v1}' }
		else { g.v1 }
	}
}

// version_guess attempts to guess which Kafka release these versions
// belong to, returning e.g. 'v0.8.0' or 'v2.7'. Guessing is done against
// the ZooKeeper, KRaft broker, and KRaft controller lineages and the most
// exact match wins. Unlike franz-go, this never mutates the receiver.
pub fn (vs &Versions) version_guess() string {
	return vs.version_guess_with(GuessConfig{})
}

// version_guess_with is version_guess with explicit configuration.
pub fn (vs &Versions) version_guess_with(cfg GuessConfig) string {
	zk := vs.guess_against(zk_chain, cfg)
	broker := vs.guess_against(broker_chain, cfg)
	controller := vs.guess_against(controller_chain, cfg)

	ord := [broker, zk, controller]
	for g in ord {
		if g.how == guess_exact {
			return g.str()
		}
	}
	for g in ord {
		if g.how == guess_at_least {
			return g.str()
		}
	}
	// A custom version: return the ZooKeeper-lineage guess, matching
	// franz-go (KRaft lineages miss requests and can guess too high).
	return zk.str()
}

fn (vs &Versions) guess_against(chain []Release, cfg GuessConfig) Guess {
	// Only max key versions are compared.
	mut mine := map[i16]Req{}
	for k, req in vs.reqs {
		if k !in cfg.skip_keys {
			mine[k] = req
		}
	}

	mut higher := ?string(none)
	for i := chain.len - 1; i >= 0; i-- {
		rel := chain[i]
		mut cmp := map[i16]Req{}
		for k, req in rel.reqs {
			if k !in cfg.skip_keys {
				cmp[k] = req
			}
		}

		mut under := false
		mut equal := false
		mut over := false
		for k, req in mine {
			if cmpreq := cmp[k] {
				if req.vmax < cmpreq.vmax {
					under = true
				} else if req.vmax > cmpreq.vmax {
					over = true
				} else {
					equal = true
				}
				cmp.delete(k)
			} else {
				// a key this release does not know: the broker is
				// higher than this release by definition
				over = true
			}
		}
		// keys of this release we did not have at all
		if cmp.len > 0 {
			under = true
		}

		is_oldest := i == 0
		if under && over {
			if is_oldest {
				return Guess{
					how: guess_custom_unknown
				}
			}
		} else if under {
			if is_oldest {
				return Guess{
					v1:  rel.name
					how: guess_not_even
				}
			}
		} else if over {
			if h := higher {
				return Guess{
					v1:  rel.name
					v2:  h
					how: guess_between
				}
			}
			return Guess{
				v1:  rel.name
				how: guess_at_least
			}
		} else if equal {
			return Guess{
				v1:  rel.name
				how: guess_exact
			}
		}
		higher = rel.name
	}
	return Guess{
		how: guess_custom_unknown
	}
}
