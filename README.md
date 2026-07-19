# franz-v

A native V port of [franz-go](https://github.com/twmb/franz-go), an Apache
Kafka client. Pinned toolchain: **V 0.5.2 (commit 7647ce1) + the
const-fixed-array cgen patch** (see the PR in this project's history: fixed
`check_expr_is_const` so fixed-array consts referencing struct consts are
runtime-initialized instead of emitting invalid static C initializers).
`kerr.all_errors` relies on the patched behavior.

## Status: Phase 1 complete (kerr + kbin)

| Module | Ported from | Status |
|---|---|---|
| `kerr/` | `pkg/kerr` | done — all 134 error codes (-1, 1..133), typed `KafkaError` implementing `IError` |
| `kbin/` | `pkg/kbin` | done — full primitive set, `Writer`/`Reader`, tested against independent golden vectors |
| `kmsg/` | `pkg/kmsg` | **complete: all 95 definition files** — 93 message families (186 request/response types) plus `enums` (11 open-alias enums) and `misc` (RecordBatch, MessageV0/V1, Header, `__consumer_offsets` key/value records) |
| `kversion/` | `pkg/kversion` | done — release tables for 57 releases across ZooKeeper/KRaft-broker/KRaft-controller lineages (extracted to data, folded at init), version guessing, from_string/stable/tip. `version_guess` is pure (never mutates the receiver, unlike upstream) and the callback-based EachMaxKeyVersion became `sorted_reqs()` since V closures capture by value |
| `sasl/` | `pkg/sasl` | done — Mechanism/Session interfaces with named-result structs, PLAIN, SCRAM-SHA-256/512 on vlib crypto; SCRAM-SHA-256 validated byte-for-byte against the RFC 7677 test vector. OAUTH/AWS/GSSAPI deferred |
| `kgo/` | `pkg/kgo` | **phases 4a + 4b**: Client with seed bootstrap, cluster discovery via Metadata (auto-starting workers for discovered brokers), per-broker ApiVersions negotiation incl. the KIP-511 v0 fallback for old brokers, automatic version selection (min of client/config/broker) written back through the mutable `Request.version` interface field, metadata cache with partition-leader lookup, and retries with exponential backoff routed by a retriable classification carried on PromisedResp. Plus the 4a foundations: Cancel (context replacement: done channel + deadline, child derivation), Logger interface (Nop/Basic), typed client errors with retriability, mut hook interfaces with per-interface slices, Config as a plain struct with defaults (replacing functional options), wire framing (request header v1/v2, ApiVersions header-v0 exception), and the channel-fed one-worker-per-broker loop with correlation checking, on-demand dial, poisoned-connection teardown, and cancel-safe synchronous request(). Integration-tested against an in-process fake Kafka broker over loopback TCP |
| `generate/` | `generate/` | dev-time generator, **self-hosted in V** (`v run generate/`); parses the upstream definitions DSL, emits V. Its output was diff-validated byte-for-byte against the retired bootstrap generator before the switch |

Regenerate kmsg (dev-time only; requires the franz-go checkout for its
definitions directory). The generator is itself a V program:

```
v run generate/ <franz-go>/generate/definitions kmsg <families...>
```

The `enums` and `misc` definition files are parsed first automatically
since families reference their types. The generator emits one `.v` file per
input plus `kmsg/smoke_test.v`, the all-types/all-versions round-trip test.
Run `v fmt -w kmsg/*.v` after regenerating.

Generator DSL coverage: version-gated fields, flexible/compact encodings,
tagged fields (with default-omission and unknown-tag preservation), inline
and named `not top level` structs, nullable structs (int8 presence marker),
enums (open aliases so unknown future wire values decode losslessly),
varint strings/bytes, `length-field-minus` blobs (RecordBatch.Records), and
`with version field` records that carry their own wire-encoded version.

Run the tests:

```
v test .
v fmt -verify kbin/primitives.v kbin/primitives_test.v kerr/kerr.v kerr/kerr_test.v
```

## V-first design conventions (used by all later phases)

This is an implementation in V, not a translation of Go:

No constructors — `kbin.Writer{}` and `kbin.Reader{src: data}` are plain
structs with defaults. Encoding is a `Writer` with `write_*` methods
(following vlib's `strings.Builder` pattern) rather than Go's
`dst = Append(dst, ...)` free functions. No `unsafe` anywhere; franz-go's
zero-copy `UnsafeString` variants were deliberately not carried over.
Nullability is expressed with Options (`?string`, `?[]u8`), never sentinel
values or pointers. Fallible functions return `!T`, and failures are typed
structs implementing `IError` (`kbin.NotEnoughDataError`,
`kerr.KafkaError`), checkable with `err is`, never compared as strings.
Lookups are `match` expressions, not table scans.

Kafka type mapping: `int8`→`i8`, `int16`→`i16`, `int32`→`int` (V int is
always 32 bits), `int64`→`i64`, `uint16`→`u16`, `uint32`→`u32`,
`float64`→`f64`, `uuid`→`[16]u8`.

One deliberate exception to "Results everywhere": `kbin.Reader` methods are
infallibly typed (invalidate-once + `complete()`/`ok()` at the end), because
V forbids combining Result and Option in one return type (`!?string` does
not compile in 0.5.2) and Kafka decoding needs nullable reads that can also
fail. A Result-based reader would therefore have inconsistent semantics
across methods; the single-validity-check design is coherent, and it is what
generated decoders want anyway.

Deliberate quirk kept from upstream: non-nullable `read_bytes` /
`read_compact_bytes` treat a `-1` length as empty rather than erroring
(Microsoft EventHubs workaround, documented in franz-go).

## Style-guide entries (learned the hard way)

`ch <- x * 2` parses as `(ch <- x) * 2` in V 0.5.2 — always send a named
temporary. `code` cannot be both a field and a method name on one struct, so
`KafkaError` stores `error_code` and exposes `code()` for `IError`.

## Test strategy

Wire-format tests use golden vectors generated by an independent Python
implementation of the Kafka/protobuf zig-zag varint spec, so the V code is
validated against the spec rather than against itself; plus deterministic
xorshift-driven random round-trips for every primitive, boundary tests at
every varint length transition, overflow/truncation decode cases, and reader
invalidation semantics.
