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
| `krec/` | (parts of `pkg/kgo`) | protocol-level record layer split out so `kfake` can validate batches without importing the client: Record/RecordHeader, magic-2 record wire codec, RecordBatch build/parse with CRC-32C verification, and compression (gzip/snappy/zstd via vlib; **lz4 deliberately unsupported, empirically verified** — vlib's `compress.lz` emits a custom 'VLZ1' container (magic `56 4c 5a 31`), not LZ4 frames (`04 22 4d 18`); wired in experimentally, batches round-trip locally but Apache Kafka 4.3.1 rejects them in LogValidator with `Lz4Compression.wrapForInput: invalid magic bytes` → UNKNOWN_SERVER_ERROR. Details in `krec/compression.v`) |
| `kfake/` | `pkg/kfake` (seed) | in-process fake cluster module: ApiVersions (incl. KIP-511 fallback), Metadata (multi-node, multi-partition, real loopback ports), Produce with full batch verification (CRC, decompress, decode) and record storage, and Fetch (re-serves stored records as rebuilt batches) |
| `kadm/` | `pkg/kadm` | admin client over the kgo request path: topic lifecycle (create with configs, delete, list with placement + uuid, create_partitions), configs (describe + KIP-339 incremental alter), groups (list across brokers, describe with decoded member assignments, delete with NON_EMPTY_GROUP protection), offsets (committed via OffsetFetch, start/end via ListOffsets, **group_lag**), and delete_records truncation. Hermetic suite over kfake (which grew a topic registry, log-start offsets, null-topics=all semantics, and nine admin handlers) plus `examples/admin_smoke.v` validated end-to-end against Kafka 4.3.1 on the first run. ACLs deferred (no authorizer to validate against). DeleteTopics capped v5 (v6+ switches to uuid pairs). |
| `kgo/` | `pkg/kgo` | **phases 4a + 4b**: Client with seed bootstrap, cluster discovery via Metadata (auto-starting workers for discovered brokers), per-broker ApiVersions negotiation incl. the KIP-511 v0 fallback for old brokers, automatic version selection (min of client/config/broker) written back through the mutable `Request.version` interface field, metadata cache with partition-leader lookup, and retries with exponential backoff routed by a retriable classification carried on PromisedResp. **Phase 5 — kfake transactions**: kfake's storage went batch-granular (StoredBatch preserves producer id/epoch/sequence, transactional and control flags; fetch re-serves faithful batches), enabling the full transaction coordinator: InitProducerId with epoch bumps (zombie fencing), AddPartitionsToTxn, staged TxnOffsetCommit applied to group offsets only on commit, EndTxn writing real control-marker batches, produce-side epoch fencing (PRODUCER_FENCED) and sequence-continuity validation (OUT_OF_ORDER_SEQUENCE_NUMBER), and fetch responses carrying aborted-transaction ranges — correctly bounded to the served window by marker offset, matching real broker behavior (returning stale ranges would make correct clients skip later committed batches of the same producer, a bug the hermetic suite caught). The entire EOS story — commit visibility, abort isolation under both isolation levels, skip-over-aborted, fencing, and consume-transform-produce with both commit (offsets+output atomic) and abort (both rolled back, replay proven) — now runs hermetically in kgo/txn_test.v in ~40ms, mirroring the real-broker txn smoke. **QoL — request pipelining + connection classes**: brokers now run a single writer plus a reader per connection; requests are written back-to-back (up to `max_inflight` = 64 per connection, config-tunable) and an in-flight queue resolves promises against Kafka's guaranteed in-order responses, with correlation verified per entry. Any transport or correlation failure fails the current and all queued in-flight requests retriably, and the writer reaps dead connections and redials. Because Kafka processes each *connection* serially, pipelining is paired with three connection classes per broker — normal, fetch, and coordinator — so fetch long-polls and coordinator-held joins cannot head-of-line-block metadata, produce, or heartbeats. Hermetic proofs (kfake grew connection counters, fetch delay, and mid-stream kill injection): 80 concurrent requests through one client pipeline over exactly one connection; a 500ms fetch long-poll leaves concurrent metadata under 300ms; killed connections fail in-flights retriably and recover on fresh connections. The one-client-per-group-member guidance stands — it is Kafka's own per-connection serial semantics, and franz-go's model too — but a single member's heartbeats/commits are no longer delayed by its own fetch long-polls. All four real-broker smokes pass over the pipelined transport; the hermetic suite got ~2.6x faster wall-clock as a side effect. **QoL — cooperative-sticky balancing (KIP-429)**: `BalancerKind.cooperative_sticky` ('cooperative-sticky' on the wire). Members advertise owned partitions and their join generation via ConsumerMemberMetadata v2; the leader-side assignor is sticky (owners keep partitions up to a greedy-ideal target, duplicate claims resolved by generation) and honors the cooperative constraint — a moving partition is revoked to nobody for one generation, the revoking member rejoins advertising reduced ownership, and the next rebalance places it. Client-side, retained partitions keep their cursors across rebalances. Validated hermetically and against Kafka 4.3.1's coordinator (`examples/cooperative_smoke.v`): incremental two-round convergence, sticky retention of lowest-owned, and the continuity proof — zero duplicate consumption without any commits, while the joiner recovers its partitions' history. Because joins are coordinator-held barriers and franz-v has no pipelining yet, concurrent members must each be polled from their own thread (the smoke shows the worker pattern). Producer robustness hardened along the way: retriable per-partition produce errors (NOT_LEADER etc.) refresh metadata and retry only the failed partitions, and successful partitions' offsets are recorded before any error surfaces so retries can never duplicate accepted records. **Phase 4d transactions/EOS**: TxnProducer (own Client per producer) implementing InitProducerId (epoch bump = zombie fencing), AddPartitionsToTxn (capped v3, classic shape), transactional record batches with per-partition idempotent sequences, AddOffsetsToTxn + TxnOffsetCommit (capped v2) for consume-transform-produce, and EndTxn commit/abort (capped v4, pre-KIP-890 semantics). Consumers gained isolation levels: read_committed filters aborted transactions client-side via the fetch aborted-transactions list plus control-marker batches (krec grew transactional/control attributes, metadata-preserving batch parsing, and marker build/decode); cursors now advance by batch bounds so aborted/control ranges never wedge a poll. Coordinator discovery retries the lazily-created coordinator topics (COORDINATOR_NOT_AVAILABLE / LOAD_IN_PROGRESS). Validated end-to-end against Kafka 4.3.1 (`examples/txn_smoke.v`): commit visibility, abort isolation under both isolation levels, skip-over-aborted-range, PRODUCER_FENCED zombie fencing, and EOS with both outcomes — commit applies output+offsets atomically, abort rolls both back (verified by replay). kfake transaction support is Phase 5 backlog. **Phase 4d consumer groups**: GroupConsumer implementing the classic eager protocol — FindCoordinator (capped v3), JoinGroup with MEMBER_ID_REQUIRED handling, SyncGroup (v5+ protocol fields filled, required by Kafka 4.x's KRaft coordinator), leader-side range/roundrobin balancing over decoded member subscriptions, heartbeats inside poll with REBALANCE_IN_PROGRESS/ILLEGAL_GENERATION/UNKNOWN_MEMBER rejoin handling, OffsetFetch (capped v7) falling back to ListOffsets, OffsetCommit (capped v9; v10+ is uuid-addressed), and LeaveGroup (capped v2). One Client per member until pipelining lands (joins are coordinator-held long-polls; franz-v sends strictly in order per connection). Validated against Kafka 4.3.1's real coordinator: two-member split, disjoint consumption, commits, leave + survivor reclaim, commit persistence across rebalances (`examples/group_smoke.v`). kfake grew a group coordinator (generations, rejoin signaling, sync barriers, offsets) for hermetic tests. **Phase 4d direct consumer**: Consumer with per-partition cursors, start positions resolved via ListOffsets (earliest/latest), per-leader fetch rounds (Fetch capped v12 pending topic-id support), batch-boundary offset filtering, seek/position, and internal recovery — OFFSET_OUT_OF_RANGE resets per `offset_reset` policy, leadership changes refresh metadata for the next poll. Consumer groups are next. **Phase 4c producer**: synchronous produce with per-partition batching routed to partition leaders, murmur2 partitioner matching Kafka's Java goldens (plus round-robin), timestamp defaulting, per-partition offset assignment, and kerr-typed partition errors. Plus the 4a foundations: Cancel (context replacement: done channel + deadline, child derivation), Logger interface (Nop/Basic), typed client errors with retriability, mut hook interfaces with per-interface slices, Config as a plain struct with defaults (replacing functional options), wire framing (request header v1/v2, ApiVersions header-v0 exception), and the channel-fed one-worker-per-broker loop with correlation checking, on-demand dial, poisoned-connection teardown, and cancel-safe synchronous request(). Integration-tested against an in-process fake Kafka broker over loopback TCP |
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
