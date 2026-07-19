// Record is a single Kafka record (magic-2 format), plus its wire
// encode/decode. Kafka records are varint-heavy: franz-go keeps this type
// hand-maintained rather than generated, and so does franz-v.
module krec

import kbin

// RecordHeader is one record header.
pub struct RecordHeader {
pub mut:
	key   string
	value ?[]u8
}

// Record is a producible/consumable Kafka record. partition may be preset
// to pin a partition; otherwise the configured partitioner assigns one.
// After a successful produce, topic, partition and offset are filled in.
pub struct Record {
pub mut:
	key       ?[]u8
	value     ?[]u8
	headers   []kbin.RecordHeader
	timestamp i64 // unix milliseconds; 0 means "now" at produce time
	topic     string
	partition int = -1
	offset    i64 = -1
}

// encode_record appends the magic-2 wire form of r to w, expressed as
// deltas against the batch's base offset/timestamp.
fn encode_record(mut w Writer, r &Record, offset_delta int, timestamp_delta i64) {
	mut body := kbin.Writer{}
	body.write_int8(0) // attributes: unused
	body.write_varlong(timestamp_delta)
	body.write_varint(offset_delta)
	body.write_varint_bytes(r.key)
	body.write_varint_bytes(r.value)
	body.write_varint(r.headers.len)
	for h in r.headers {
		body.write_varint_string(h.key)
		body.write_varint_bytes(h.value)
	}
	w.write_varint(body.buf.len)
	w.buf << body.buf
}

// decode_record reads one record from r, resolving deltas against the
// batch base offset/timestamp.
fn decode_record(mut r Reader, base_offset i64, base_timestamp i64) !Record {
	length := r.read_varint()
	if length < 0 || r.remaining() < length {
		return error('record length ${length} exceeds remaining ${r.remaining()}')
	}
	end := r.off + length
	r.read_int8() // attributes
	ts_delta := r.read_varlong()
	offset_delta := r.read_varint()
	key := r.read_varint_bytes()
	value := r.read_varint_bytes()
	num_headers := r.read_varint()
	mut headers := []kbin.RecordHeader{cap: if num_headers > 0 { num_headers } else { 0 }}
	for _ in 0 .. num_headers {
		hkey := r.read_varint_string()
		hval := r.read_varint_bytes()
		headers << kbin.RecordHeader{
			key:   hkey
			value: hval
		}
	}
	r.complete() or { return error('record truncated') }
	if r.off != end {
		return error('record length mismatch: declared ${length}, consumed ${r.off - (end - length)}')
	}
	key_c := if k := key { ?[]u8(k.clone()) } else { ?[]u8(none) }
	val_c := if v := value { ?[]u8(v.clone()) } else { ?[]u8(none) }
	return kbin.Record{
		key:       key_c
		value:     val_c
		headers:   headers
		timestamp: base_timestamp + ts_delta
		offset:    base_offset + offset_delta
	}
}
