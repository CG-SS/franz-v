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

// BatchOpts configures build_record_batch; the defaults produce a
// non-idempotent, non-transactional batch.
pub struct BatchOpts {
pub mut:
	codec          kmsg.Codec
	base_offset    i64 // the batch's first offset (brokers rewrite on produce)
	producer_id    i64 = -1
	producer_epoch i16 = -1
	base_sequence  int = -1
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
	mut payload := kmsg.Writer{}
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
		attributes:             i16(int(opts.codec))
		last_offset_delta:      records.len - 1
		first_timestamp:        base_ts
		max_timestamp:          max_ts
		producer_id:            opts.producer_id
		producer_epoch:         opts.producer_epoch
		first_sequence:         opts.base_sequence
		num_records:            records.len
		records:                compressed
	}
	mut w := kmsg.Writer{}
	batch.write_to(mut w)
	// crc32c over everything after the crc field
	crc := crc32.sum_crc32c(w.buf[batch_header_before_crc + 4..])
	mut cw := kmsg.Writer{}
	cw.write_uint32(crc)
	for i in 0 .. 4 {
		w.buf[batch_header_before_crc + i] = cw.buf[i]
	}
	return w.buf
}

// BatchCrcError reports a record batch whose CRC-32C did not match.
pub struct BatchCrcError {
	kmsg.Error
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
pub fn parse_record_batch(buf []u8) !(RecordBatch, []Record) {
	mut batch := kmsg.RecordBatch{}
	mut r := kmsg.Reader{
		src: buf
	}
	batch.read_from(mut r)!
	if batch.magic != 2 {
		return error('unsupported record batch magic ${batch.magic}')
	}
	want := u32(batch.crc)
	got := crc32.sum_crc32c(buf[batch_header_before_crc + 4..r.off])
	if want != got {
		return kmsg.BatchCrcError{
			want: want
			got:  got
		}
	}
	codec_bits := int(batch.attributes) & 0x07
	codec := match codec_bits {
		0 { kmsg.Codec.uncompressed }
		1 { kmsg.Codec.gzip }
		2 { kmsg.Codec.snappy }
		3 { kmsg.Codec.lz4 }
		4 { kmsg.Codec.zstd }
		else { return error('unknown compression bits ${codec_bits}') }
	}

	payload := decompress_payload(codec, batch.records)!

	mut pr := kmsg.Reader{
		src: payload
	}
	mut records := []kmsg.Record{cap: batch.num_records}
	for _ in 0 .. batch.num_records {
		records << decode_record(mut pr, batch.first_offset, batch.first_timestamp)!
	}
	if pr.remaining() != 0 {
		return error('record batch has ${pr.remaining()} trailing bytes')
	}
	return batch, records
}

// parse_record_batches splits and parses a Fetch payload of one or more
// concatenated record batches. A trailing partial batch (brokers may cut
// responses mid-batch) is ignored, per protocol.
pub fn parse_record_batches(buf []u8) ![]Record {
	mut out := []kmsg.Record{}
	mut off := 0
	for buf.len - off >= 12 {
		mut hr := kmsg.Reader{
			src: buf[off + 8..off + 12].clone()
		}
		total := 12 + hr.read_int32()
		if total <= 12 || off + total > buf.len {
			break // partial trailing batch
		}
		_, recs := parse_record_batch(buf[off..off + total])!
		out << recs
		off += total
	}
	return out
}
