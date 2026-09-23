// RecordBatch building and parsing (magic 2). The CRC-32C covers all bytes
// after the crc field; the batch is serialized via kmsg.RecordBatch and
// the crc patched in afterwards.
module krec

import hash.crc32
import kbin
import kmsg

// batch_header_before_crc is first_offset(8) + length(4) +
// partition_leader_epoch(4) + magic(1).
const batch_header_before_crc = 17

// batch_len_after_length_field is everything from partition_leader_epoch
// through num_records: 4+1+4+2+4+8+8+8+2+4+4.
const batch_len_after_length_field = 49

// next_sequence returns the producer sequence number that follows seq after
// n records. Sequences are int32 values that wrap from max_i32 to 0, exactly
// like Kafka's DefaultRecordBatch.incrementSequence: an idempotent producer
// sends the result as the next batch's base sequence, and brokers expect it.
pub fn next_sequence(seq int, n int) int {
	if seq > max_i32 - n {
		return n - (max_i32 - seq) - 1
	}
	return seq + n
}

// BatchOpts configures build_record_batch; the defaults produce a
// non-idempotent, non-transactional batch.
pub struct BatchOpts {
pub mut:
	codec          Codec
	base_offset    i64 // the batch's first offset (brokers rewrite on produce)
	producer_id    i64 = -1
	producer_epoch i16 = -1
	base_sequence  int = -1
	// transactional sets attribute bit 4: the batch belongs to an open
	// transaction of producer_id/producer_epoch.
	transactional bool
	// control sets attribute bit 5: the batch carries a transaction
	// marker rather than user records.
	control bool
}

// build_record_batch renders records into one magic-2 RecordBatch. All
// records must belong to the same partition. Timestamp deltas are taken
// against the first record's timestamp.
pub fn build_record_batch(records []Record, opts BatchOpts) ![]u8 {
	if records.len == 0 {
		return error('cannot build an empty record batch')
	}
	base_ts := records[0].timestamp
	mut max_ts := base_ts
	mut payload := kbin.Writer{}
	for i, r in records {
		if r.timestamp > max_ts {
			max_ts = r.timestamp
		}
		encode_record(mut payload, &r, i, r.timestamp - base_ts)
	}
	compressed := compress_payload(opts.codec, payload.buf)!

	mut batch := kmsg.RecordBatch{
		first_offset:           opts.base_offset
		length:                 batch_len_after_length_field + compressed.len
		partition_leader_epoch: -1
		magic:                  2
		crc:                    0 // patched below
		attributes:             batch_attributes(opts)
		last_offset_delta:      records.len - 1
		first_timestamp:        base_ts
		max_timestamp:          max_ts
		producer_id:            opts.producer_id
		producer_epoch:         opts.producer_epoch
		first_sequence:         opts.base_sequence
		num_records:            records.len
		records:                compressed
	}
	mut w := kbin.Writer{}
	batch.write_to(mut w)
	// crc32c over everything after the crc field
	crc := crc32.sum_crc32c(w.buf[batch_header_before_crc + 4..])
	mut cw := kbin.Writer{}
	cw.write_uint32(crc)
	for i in 0 .. 4 {
		w.buf[batch_header_before_crc + i] = cw.buf[i]
	}
	return w.buf
}

fn batch_attributes(opts BatchOpts) i16 {
	mut a := i16(int(opts.codec))
	if opts.transactional {
		a |= i16(0x10)
	}
	if opts.control {
		a |= i16(0x20)
	}
	return a
}

// is_transactional reports attribute bit 4 of a parsed batch.
pub fn is_transactional(batch kmsg.RecordBatch) bool {
	return int(batch.attributes) & (1 << 4) != 0
}

// is_control reports attribute bit 5 of a parsed batch: a transaction
// marker batch.
pub fn is_control(batch kmsg.RecordBatch) bool {
	return int(batch.attributes) & (1 << 5) != 0
}

// ParsedBatch is one decoded batch with its metadata preserved, for
// callers that need transactional context (isolation filtering).
pub struct ParsedBatch {
pub mut:
	batch   kmsg.RecordBatch
	records []Record
}

// parse_batches_meta splits and parses a Fetch payload into batches with
// metadata. A trailing partial batch is ignored, per protocol.
pub fn parse_batches_meta(buf []u8) ![]ParsedBatch {
	mut out := []ParsedBatch{}
	mut off := 0
	for buf.len - off >= 12 {
		mut hr := kbin.Reader{
			src: buf[off + 8..off + 12].clone()
		}
		total := 12 + hr.read_int32()
		if total <= 12 || off + total > buf.len {
			break
		}
		batch, recs := parse_record_batch(buf[off..off + total].clone())!
		out << ParsedBatch{
			batch:   batch
			records: recs
		}
		off += total
	}
	return out
}

// control_marker_batch builds a transaction control batch holding one
// commit (true) or abort (false) marker, as brokers write on EndTxn.
pub fn control_marker_batch(base_offset i64, producer_id i64, producer_epoch i16, commit bool, timestamp i64) ![]u8 {
	marker_type := if commit { u8(1) } else { u8(0) }
	// key: version int16 0, type int16; value: version int16 0,
	// coordinator epoch int32 0
	key := [u8(0), 0, 0, marker_type]
	value := [u8(0), 0, 0, 0, 0, 0]
	rec := Record{
		key:       key
		value:     value
		timestamp: timestamp
	}
	return build_record_batch([rec], BatchOpts{
		base_offset:    base_offset
		producer_id:    producer_id
		producer_epoch: producer_epoch
		transactional:  true
		control:        true
	})
}

// control_marker_is_commit decodes a control record's marker type.
pub fn control_marker_is_commit(rec Record) ?bool {
	key := rec.key or { return none }
	if key.len < 4 {
		return none
	}
	return key[2] == 0 && key[3] == 1
}

// BatchCrcError reports a record batch whose CRC-32C did not match.
pub struct BatchCrcError {
	Error
pub:
	want u32
	got  u32
}

// msg implements IError.
pub fn (e BatchCrcError) msg() string {
	return 'record batch crc mismatch: batch says ${e.want:08x}, computed ${e.got:08x}'
}

// parse_record_batch decodes and verifies one RecordBatch: CRC first, then
// decompression, then records. Returns the batch metadata and records with
// absolute offsets/timestamps.
pub fn parse_record_batch(buf []u8) !(kmsg.RecordBatch, []Record) {
	mut batch := kmsg.RecordBatch{}
	mut r := kbin.Reader{
		src: buf
	}
	batch.read_from(mut r)!
	if batch.magic != 2 {
		return error('unsupported record batch magic ${batch.magic}')
	}
	want := u32(batch.crc)
	got := crc32.sum_crc32c(buf[batch_header_before_crc + 4..r.off])
	if want != got {
		return BatchCrcError{
			want: want
			got:  got
		}
	}
	codec_bits := int(batch.attributes) & 0x07
	codec := match codec_bits {
		0 { Codec.uncompressed }
		1 { Codec.gzip }
		2 { Codec.snappy }
		3 { Codec.lz4 }
		4 { Codec.zstd }
		else { return error('unknown compression bits ${codec_bits}') }
	}

	payload := decompress_payload(codec, batch.records)!

	mut pr := kbin.Reader{
		src: payload
	}
	mut records := []Record{cap: batch.num_records}
	for _ in 0 .. batch.num_records {
		records << decode_record(mut pr, batch.first_offset, batch.first_timestamp)!
	}
	if pr.remaining() != 0 {
		return error('record batch has ${pr.remaining()} trailing bytes')
	}
	return batch, records
}

// parse_record_batches splits and parses a Fetch payload of one or more
// concatenated record batches, skipping control batches. A trailing
// partial batch (brokers may cut responses mid-batch) is ignored.
pub fn parse_record_batches(buf []u8) ![]Record {
	mut out := []Record{}
	for pb in parse_batches_meta(buf)! {
		if is_control(pb.batch) {
			continue
		}
		out << pb.records
	}
	return out
}
