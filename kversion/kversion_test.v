module kversion

// Test cases ported from franz-go's kversion_test.go plus additions.

fn test_chains_built() {
	assert zk_chain.len == 29
	assert zk_chain[0].name == 'v0.8.0'
	assert zk_chain.last().name == 'v3.9'
	assert broker_chain.last().name == 'v4.2'
	assert controller_chain.last().name == 'v4.2'
}

fn test_set_max_key_version() {
	mut vs := Versions{}
	for i in 0 .. 100 {
		vs.set_max_key_version(i16(i), i16(i))
	}
	for k, req in vs.reqs {
		assert k == req.vmax
	}
	// negative version removes the key
	vs.set_max_key_version(5, -1)
	assert !vs.has_key(5)
	assert vs.has_key(6)
}

fn test_lookup() {
	vs := from_string('v3.5') or {
		assert false, 'v3.5 should resolve'
		return
	}
	produce := vs.lookup_max_key_version(0) or {
		assert false, 'produce must exist'
		return
	}
	assert produce == 9
	if _ := vs.lookup_max_key_version(30000) {
		assert false, 'key 30000 must not exist'
	}
}

fn test_version_guess_v0_8_0() {
	// exact -> not even -> between -> unknown custom, as in franz-go
	mut v := from_string('v0.8.0') or {
		assert false, 'v0.8.0 should resolve'
		return
	}
	assert v.version_guess() == 'v0.8.0'
	v.set_max_key_version(0, -1)
	assert v.version_guess() == 'not even v0.8.0'
	v.set_max_key_version(0, 100)
	assert v.version_guess() == 'between v0.8.0 and v0.8.1'
	v.set_max_key_version(1, -1)
	assert v.version_guess() == 'unknown custom version'
}

fn test_version_guess_between_next() {
	mut v := from_string('v0.9.0') or {
		assert false, 'v0.9.0 should resolve'
		return
	}
	assert v.version_guess() == 'v0.9.0'
	v.set_max_key_version(17, 0)
	assert v.version_guess() == 'between v0.9.0 and v0.10.0'
	v.set_max_key_version(0, 2)
	v.set_max_key_version(1, 2)
	v.set_max_key_version(3, 1)
	v.set_max_key_version(6, 2)
	v.set_max_key_version(18, 0)
	assert v.version_guess() == 'v0.10.0'
}

fn test_version_guess_skip_keys() {
	// 4, 5, 6, 7 are skipped by default: unsetting them changes nothing
	mut v := from_string('v2.7') or {
		assert false, 'v2.7 should resolve'
		return
	}
	assert v.version_guess() == 'v2.7'
	v.set_max_key_version(4, -1)
	v.set_max_key_version(5, -1)
	v.set_max_key_version(6, -1)
	v.set_max_key_version(7, -1)
	assert v.version_guess() == 'v2.7'
	// unknown keys with -1 version are no-ops
	v.set_max_key_version(100, -1)
	assert v.version_guess() == 'v2.7'
	// but with a custom skip config that skips nothing, it degrades
	custom := v.version_guess_with(GuessConfig{
		skip_keys: []
	})
	assert custom != 'v2.7'
}

fn test_version_guess_every_zk_release() {
	// Every release in the zk lineage guesses itself — except releases
	// whose successor changed no API versions at all (v3.3/v3.4 and
	// v3.7/v3.8 are wire-identical), where the newer name wins since
	// guessing scans newest-first.
	skips := GuessConfig{}.skip_keys
	for i, rel in zk_chain {
		vs := Versions{
			reqs: rel.reqs.clone()
		}
		mut expect := rel.name
		// releases wire-identical to their successor after default skips
		// (e.g. v3.3/v3.4 differ only in skipped broker-to-broker keys)
		// guess as the newer name, since guessing scans newest-first
		if i + 1 < zk_chain.len {
			next := zk_chain[i + 1]
			mut identical := true
			for k, req in rel.reqs {
				if k in skips {
					continue
				}
				nreq := next.reqs[k] or {
					identical = false
					break
				}
				if nreq.vmax != req.vmax {
					identical = false
					break
				}
			}
			for k in next.reqs.keys() {
				if k !in skips && k !in rel.reqs {
					identical = false
				}
			}
			if identical {
				expect = next.name
			}
		}
		assert vs.version_guess() == expect, 'guess for ${rel.name}'
	}
}

fn test_version_guess_is_pure() {
	// unlike franz-go, guessing must not mutate the receiver
	vs := from_string('v2.7') or {
		assert false, 'v2.7 should resolve'
		return
	}
	before := vs.reqs.len
	vs.version_guess()
	assert vs.reqs.len == before
	assert vs.has_key(4) // a default-skipped key survives
}

fn test_from_string_forms() {
	// patch versions are accepted and stripped; v prefix optional
	for input in ['v0.8.0', '0.8.0', '0.8.0.1', 'v3.5', '3.5', '3.5.1'] {
		if _ := from_string(input) {
		} else {
			assert false, '${input} should resolve'
		}
	}
	for input in ['', 'v', 'abc', '0.8', 'v3', '3.5.1.1', '0.8.0.0.0'] {
		if _ := from_string(input) {
			assert false, '${input} should not resolve'
		}
	}
}

fn test_version_strings() {
	names := version_strings()
	assert 'v4.2' in names
	assert 'v0.8.0' in names
	assert 'v3.9' in names
	assert names.first() == 'v4.2'
	assert names.last() == 'v0.8.0'
}

fn test_stable_and_equal() {
	a := stable()
	b := stable()
	assert a.equal(b)
	assert a.has_key(0) // produce
	assert a.has_key(18) // api versions
	mut c := stable()
	c.set_max_key_version(0, 0)
	assert !a.equal(c)
}

fn test_sorted_reqs() {
	vs := from_string('v0.8.1') or {
		assert false, 'v0.8.1 should resolve'
		return
	}
	reqs := vs.sorted_reqs()
	assert reqs.len == vs.reqs.len
	assert reqs.len == 10 // keys 0..9 in v0.8.1
	for i in 1 .. reqs.len {
		assert reqs[i].key > reqs[i - 1].key
	}
}

fn test_str_uses_names() {
	vs := from_string('v0.8.0') or {
		assert false, 'v0.8.0 should resolve'
		return
	}
	s := vs.str()
	assert s.contains('Produce')
	assert s.contains('Fetch')
	assert s.contains('Metadata')
}
