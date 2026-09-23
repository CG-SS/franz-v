// Module kbin contains Kafka primitive reading and writing functions;
// a V implementation of the wire format handled by franz-go's pkg/kbin.
//
// Encoding uses a Writer struct (initialized with plain defaults, following
// the vlib strings.Builder pattern); decoding uses a Reader struct.
//
// A note on the Reader's error design: V functions may return a Result (!T)
// or an Option (?T), but not both — `!?string` is rejected by the compiler.
// Kafka decoding needs nullable reads (Options) that can also fail, so a
// Result-based reader would force inconsistent semantics across methods.
// Instead, every read is infallibly typed: after any short read the Reader
// invalidates itself, all subsequent reads return zero values, and one call
// to complete() (or ok()) at the end of a decode reports validity. This
// matches how generated decoders want to read anyway.
//
// Kafka type mapping used throughout this module:
//   int8 -> i8, int16 -> i16, int32 -> int, int64 -> i64, uint16 -> u16,
//   uint32 -> u32, float64 -> f64, uuid -> [16]u8,
//   nullable string -> ?string, nullable bytes -> ?[]u8.
// V's int is 64 bits wide on 64-bit targets (32 on 32-bit ones), so int32
// reads sign-extend through i32 and writes keep the low 32 bits.
module kbin

import math
import math.bits

// NotEnoughDataError is returned by Reader.complete() when a decode ran out
// of data. Check for it with `err is kbin.NotEnoughDataError`.
pub struct NotEnoughDataError {
	Error
}

// msg implements IError.
pub fn (e NotEnoughDataError) msg() string {
	return 'response did not contain enough data to be valid'
}

// ---------------------------------------------------------------------------
// Varint helpers (Kafka uses protobuf-style zig-zag varints)
// ---------------------------------------------------------------------------

// uvarint_lens[bits.len_32(u)] is the encoded length of u. Index 0 (value 0)
// still encodes to 1 byte.
const uvarint_lens = [u8(1), 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4,
	4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8,
	8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 10]!

// varint_len returns how long i would be if it were varint encoded.
pub fn varint_len(i int) int {
	u := u32(i) << 1 ^ u32(i >> 31)
	return uvarint_len(u)
}

// uvarint_len returns how long u would be if it were uvarint encoded.
pub fn uvarint_len(u u32) int {
	return int(uvarint_lens[bits.len_32(u)])
}

// varlong_len returns how long i would be if it were varlong encoded.
pub fn varlong_len(i i64) int {
	u := u64(i) << 1 ^ u64(i >> 63)
	return uvarlong_len(u)
}

fn uvarlong_len(u u64) int {
	return int(uvarint_lens[bits.len_64(u)])
}

// varint decodes a zig-zag 32 bit varint from src. The second return is the
// number of bytes read; it is 0 if src ran out and -5 if the encoding
// overflowed 5 bytes.
pub fn varint(src []u8) (int, int) {
	x, n := uvarint(src)
	return int(x >> 1) ^ -int(x & 1), n
}

// uvarint decodes a 32 bit uvarint from src. The second return is the number
// of bytes read; it is 0 if src ran out and -5 on 5 byte overflow.
pub fn uvarint(src []u8) (u32, int) {
	mut x := u32(0)
	for i in 0 .. 5 {
		if i >= src.len {
			return 0, 0
		}
		c := src[i]
		if i == 4 {
			// last permissible byte: only 4 low bits may be set
			if c > 0x0f {
				return 0, -5
			}
			x |= u32(c) << 28
			return x, 5
		}
		x |= u32(c & 0x7f) << (7 * i)
		if c & 0x80 == 0 {
			return x, i + 1
		}
	}
	return 0, 0
}

// varlong decodes a zig-zag 64 bit varint from src. The second return is the
// number of bytes read; it is 0 if src ran out and -10 on 10 byte overflow.
pub fn varlong(src []u8) (i64, int) {
	x, n := uvarlong(src)
	return i64(x >> 1) ^ -i64(x & 1), n
}

fn uvarlong(src []u8) (u64, int) {
	mut x := u64(0)
	for i in 0 .. 10 {
		if i >= src.len {
			return 0, 0
		}
		c := src[i]
		if i == 9 {
			// last permissible byte: only the low bit may be set
			if c > 0x01 {
				return 0, -10
			}
			x |= u64(c) << 63
			return x, 10
		}
		x |= u64(c & 0x7f) << (7 * i)
		if c & 0x80 == 0 {
			return x, i + 1
		}
	}
	return 0, 0
}

// ---------------------------------------------------------------------------
// Writer (encoder)
// ---------------------------------------------------------------------------

// Writer accumulates Kafka-encoded bytes in buf. Initialize it with plain
// struct defaults; pre-size with `Writer{ buf: []u8{cap: n} }` if desired.
pub struct Writer {
pub mut:
	buf []u8
}

// write_bool writes 1 for true or 0 for false.
pub fn (mut w Writer) write_bool(v bool) {
	if v {
		w.buf << u8(1)
	} else {
		w.buf << u8(0)
	}
}

// write_int8 writes an int8.
pub fn (mut w Writer) write_int8(i i8) {
	w.buf << u8(i)
}

// write_int16 writes a big endian int16.
pub fn (mut w Writer) write_int16(i i16) {
	w.write_uint16(u16(i))
}

// write_uint16 writes a big endian uint16.
pub fn (mut w Writer) write_uint16(u u16) {
	w.buf << u8(u >> 8)
	w.buf << u8(u)
}

// write_int32 writes a big endian int32.
pub fn (mut w Writer) write_int32(i int) {
	w.write_uint32(u32(i))
}

// write_uint32 writes a big endian uint32.
pub fn (mut w Writer) write_uint32(u u32) {
	w.buf << u8(u >> 24)
	w.buf << u8(u >> 16)
	w.buf << u8(u >> 8)
	w.buf << u8(u)
}

// write_int64 writes a big endian int64.
pub fn (mut w Writer) write_int64(i i64) {
	w.write_uint64(u64(i))
}

// write_float64 writes a big endian float64.
pub fn (mut w Writer) write_float64(f f64) {
	w.write_uint64(math.f64_bits(f))
}

// write_uuid writes the 16 uuid bytes.
pub fn (mut w Writer) write_uuid(uuid [16]u8) {
	for b in uuid {
		w.buf << b
	}
}

fn (mut w Writer) write_uint64(u u64) {
	w.buf << u8(u >> 56)
	w.buf << u8(u >> 48)
	w.buf << u8(u >> 40)
	w.buf << u8(u >> 32)
	w.buf << u8(u >> 24)
	w.buf << u8(u >> 16)
	w.buf << u8(u >> 8)
	w.buf << u8(u)
}

// write_varint writes a zig-zag varint encoded i.
pub fn (mut w Writer) write_varint(i int) {
	w.write_uvarint(u32(i) << 1 ^ u32(i >> 31))
}

// write_uvarint writes a uvarint encoded u.
pub fn (mut w Writer) write_uvarint(u u32) {
	mut x := u
	for x >= 0x80 {
		w.buf << u8(x & 0x7f | 0x80)
		x >>= 7
	}
	w.buf << u8(x)
}

// write_varlong writes a zig-zag varlong encoded i.
pub fn (mut w Writer) write_varlong(i i64) {
	w.write_uvarlong(u64(i) << 1 ^ u64(i >> 63))
}

fn (mut w Writer) write_uvarlong(u u64) {
	mut x := u
	for x >= 0x80 {
		w.buf << u8(x & 0x7f | 0x80)
		x >>= 7
	}
	w.buf << u8(x)
}

// write_string writes a string prefixed with its int16 length.
pub fn (mut w Writer) write_string(s string) {
	w.write_int16(i16(s.len))
	w.buf << s.bytes()
}

// write_compact_string writes a string prefixed with its uvarint length
// starting at 1; 0 is reserved for null, which compact strings are not
// (nullable compact ones are!). Thus, the length is the decoded uvarint - 1.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_string(s string) {
	w.write_uvarint(1 + u32(s.len))
	w.buf << s.bytes()
}

// write_nullable_string writes a potentially absent string prefixed with its
// int16 length, or int16(-1) if none.
pub fn (mut w Writer) write_nullable_string(s ?string) {
	if v := s {
		w.write_string(v)
	} else {
		w.write_int16(-1)
	}
}

// write_compact_nullable_string writes a potentially absent string with its
// uvarint length starting at 1, with 0 indicating null.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_nullable_string(s ?string) {
	if v := s {
		w.write_compact_string(v)
	} else {
		w.write_uvarint(0)
	}
}

// write_bytes writes bytes prefixed with their int32 length.
pub fn (mut w Writer) write_bytes(b []u8) {
	w.write_int32(b.len)
	w.buf << b
}

// write_compact_bytes writes bytes prefixed with their uvarint length
// starting at 1; 0 is reserved for null.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_bytes(b []u8) {
	w.write_uvarint(1 + u32(b.len))
	w.buf << b
}

// write_nullable_bytes writes a potentially absent slice prefixed with its
// int32 length, or int32(-1) if none.
pub fn (mut w Writer) write_nullable_bytes(b ?[]u8) {
	if v := b {
		w.write_bytes(v)
	} else {
		w.write_int32(-1)
	}
}

// write_compact_nullable_bytes writes a potentially absent slice with its
// uvarint length starting at 1, with 0 indicating null.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_nullable_bytes(b ?[]u8) {
	if v := b {
		w.write_compact_bytes(v)
	} else {
		w.write_uvarint(0)
	}
}

// write_varint_string writes a string prefixed with its length encoded as a
// varint.
pub fn (mut w Writer) write_varint_string(s string) {
	w.write_varint(s.len)
	w.buf << s.bytes()
}

// write_varint_bytes writes a potentially absent slice prefixed with its
// length encoded as a varint, with -1 indicating null. Used in Record
// keys/values/headers.
pub fn (mut w Writer) write_varint_bytes(b ?[]u8) {
	if v := b {
		w.write_varint(v.len)
		w.buf << v
	} else {
		w.write_varint(-1)
	}
}

// write_array_len writes the length of an array as an int32.
pub fn (mut w Writer) write_array_len(l int) {
	w.write_int32(l)
}

// write_compact_array_len writes the length of an array as a uvarint of the
// length + 1.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_array_len(l int) {
	w.write_uvarint(1 + u32(l))
}

// write_nullable_array_len writes the length of an array as an int32, or -1
// if is_nil is true.
pub fn (mut w Writer) write_nullable_array_len(l int, is_nil bool) {
	if is_nil {
		w.write_int32(-1)
	} else {
		w.write_int32(l)
	}
}

// write_compact_nullable_array_len writes the length of an array as a
// uvarint of the length + 1; if is_nil is true, this writes 0.
//
// For KIP-482.
pub fn (mut w Writer) write_compact_nullable_array_len(l int, is_nil bool) {
	if is_nil {
		w.write_uvarint(0)
	} else {
		w.write_uvarint(1 + u32(l))
	}
}

// ---------------------------------------------------------------------------
// Reader (decoder)
// ---------------------------------------------------------------------------

// Reader decodes Kafka messages from src. Initialize it with plain struct
// defaults: `mut r := kbin.Reader{ src: data }`.
//
// If the reader has been invalidated by a short read, all read methods
// return defaults (false, 0, [], '', none). Use complete() or ok() to detect
// whether the reader was invalidated. See the module comment for why this
// design is used instead of Result returns.
pub struct Reader {
pub mut:
	src []u8
	off int
	bad bool
}

fn (mut b Reader) invalidate() {
	b.bad = true
	b.off = b.src.len
}

// remaining returns how many bytes are left unread.
pub fn (b &Reader) remaining() int {
	return b.src.len - b.off
}

// read_bool returns a bool from the reader.
pub fn (mut b Reader) read_bool() bool {
	if b.remaining() < 1 {
		b.invalidate()
		return false
	}
	t := b.src[b.off] != 0
	b.off++
	return t
}

// read_int8 returns an int8 from the reader.
pub fn (mut b Reader) read_int8() i8 {
	if b.remaining() < 1 {
		b.invalidate()
		return 0
	}
	r := b.src[b.off]
	b.off++
	return i8(r)
}

// read_int16 returns a big endian int16 from the reader.
pub fn (mut b Reader) read_int16() i16 {
	return i16(b.read_uint16())
}

// read_uint16 returns a big endian uint16 from the reader.
pub fn (mut b Reader) read_uint16() u16 {
	if b.remaining() < 2 {
		b.invalidate()
		return 0
	}
	r := u16(b.src[b.off]) << 8 | u16(b.src[b.off + 1])
	b.off += 2
	return r
}

// read_int32 returns a big endian int32 from the reader.
pub fn (mut b Reader) read_int32() int {
	// Go through i32 so negative values (e.g. -1 null lengths) sign-extend
	// when int is 64 bits wide.
	return int(i32(b.read_uint32()))
}

// read_uint32 returns a big endian uint32 from the reader.
pub fn (mut b Reader) read_uint32() u32 {
	if b.remaining() < 4 {
		b.invalidate()
		return 0
	}
	r := u32(b.src[b.off]) << 24 | u32(b.src[b.off + 1]) << 16 | u32(b.src[b.off + 2]) << 8 | u32(b.src[b.off + 3])
	b.off += 4
	return r
}

// read_int64 returns a big endian int64 from the reader.
pub fn (mut b Reader) read_int64() i64 {
	return i64(b.read_uint64())
}

// read_float64 returns a big endian float64 from the reader.
pub fn (mut b Reader) read_float64() f64 {
	return math.f64_from_bits(b.read_uint64())
}

fn (mut b Reader) read_uint64() u64 {
	if b.remaining() < 8 {
		b.invalidate()
		return 0
	}
	mut r := u64(0)
	for i in 0 .. 8 {
		r = r << 8 | u64(b.src[b.off + i])
	}
	b.off += 8
	return r
}

// read_uuid returns a uuid from the reader.
pub fn (mut b Reader) read_uuid() [16]u8 {
	mut r := [16]u8{}
	span := b.span(16)
	for i, v in span {
		r[i] = v
	}
	return r
}

// read_varint returns a zig-zag varint int32 from the reader.
pub fn (mut b Reader) read_varint() int {
	val, n := varint(b.src[b.off..])
	if n <= 0 {
		b.invalidate()
		return 0
	}
	b.off += n
	return val
}

// read_varlong returns a zig-zag varlong int64 from the reader.
pub fn (mut b Reader) read_varlong() i64 {
	val, n := varlong(b.src[b.off..])
	if n <= 0 {
		b.invalidate()
		return 0
	}
	b.off += n
	return val
}

// read_uvarint returns a uvarint encoded uint32 from the reader.
pub fn (mut b Reader) read_uvarint() u32 {
	val, n := uvarint(b.src[b.off..])
	if n <= 0 {
		b.invalidate()
		return 0
	}
	b.off += n
	return val
}

// span returns a copy of the next l bytes from the reader.
pub fn (mut b Reader) span(l int) []u8 {
	if l < 0 || b.remaining() < l {
		b.invalidate()
		return []
	}
	r := b.src[b.off..b.off + l].clone()
	b.off += l
	return r
}

// read_string returns a Kafka string (int16 length prefixed) from the reader.
pub fn (mut b Reader) read_string() string {
	l := b.read_int16()
	return b.span(int(l)).bytestr()
}

// read_compact_string returns a Kafka compact string (uvarint length+1
// prefixed) from the reader.
pub fn (mut b Reader) read_compact_string() string {
	l := int(b.read_uvarint()) - 1
	return b.span(l).bytestr()
}

// read_nullable_string returns a Kafka nullable string from the reader,
// returning none for the -1 length.
pub fn (mut b Reader) read_nullable_string() ?string {
	l := b.read_int16()
	if l < 0 {
		return none
	}
	return b.span(int(l)).bytestr()
}

// read_compact_nullable_string returns a Kafka compact nullable string from
// the reader, returning none for the 0 length.
pub fn (mut b Reader) read_compact_nullable_string() ?string {
	l := int(b.read_uvarint()) - 1
	if l < 0 {
		return none
	}
	return b.span(l).bytestr()
}

// read_bytes returns a Kafka byte array (int32 length prefixed) from the
// reader.
//
// This never returns none: unlike spec, a -1 length returns empty bytes,
// because Microsoft EventHubs erroneously uses -1 on non-nullable fields
// (mirroring the franz-go workaround).
pub fn (mut b Reader) read_bytes() []u8 {
	l := b.read_int32()
	if l == -1 {
		return []
	}
	return b.span(l)
}

// read_compact_bytes returns a Kafka compact byte array from the reader.
//
// This never returns none; see read_bytes for the -1 caveat.
pub fn (mut b Reader) read_compact_bytes() []u8 {
	l := int(b.read_uvarint()) - 1
	if l == -1 {
		return []
	}
	return b.span(l)
}

// read_nullable_bytes returns a Kafka nullable byte array from the reader,
// returning none as appropriate.
pub fn (mut b Reader) read_nullable_bytes() ?[]u8 {
	l := b.read_int32()
	if l < 0 {
		return none
	}
	return b.span(l)
}

// read_compact_nullable_bytes returns a Kafka compact nullable byte array
// from the reader, returning none as appropriate.
pub fn (mut b Reader) read_compact_nullable_bytes() ?[]u8 {
	l := int(b.read_uvarint()) - 1
	if l < 0 {
		return none
	}
	return b.span(l)
}

// read_array_len returns a Kafka array length from the reader.
pub fn (mut b Reader) read_array_len() int {
	r := b.read_int32()
	// The min size of a Kafka type is a byte, so if we do not have at least
	// the array length of bytes left, it is bad.
	if b.remaining() < r {
		b.invalidate()
		return 0
	}
	return r
}

// read_varint_array_len returns a varint Kafka array length from the reader.
pub fn (mut b Reader) read_varint_array_len() int {
	r := b.read_varint()
	if b.remaining() < r {
		b.invalidate()
		return 0
	}
	return r
}

// read_compact_array_len returns a Kafka compact array length from the
// reader.
pub fn (mut b Reader) read_compact_array_len() int {
	r := int(b.read_uvarint()) - 1
	if b.remaining() < r {
		b.invalidate()
		return 0
	}
	return r
}

// read_varint_bytes returns a Kafka varint-length-prefixed byte array from
// the reader, returning none as appropriate (used in Record keys/values).
pub fn (mut b Reader) read_varint_bytes() ?[]u8 {
	l := b.read_varint()
	if l < 0 {
		return none
	}
	return b.span(l)
}

// read_varint_string returns a Kafka varint-length-prefixed string from the
// reader. A null (-1) length returns the empty string.
pub fn (mut b Reader) read_varint_string() string {
	v := b.read_varint_bytes() or { return '' }
	return v.bytestr()
}

// complete returns a NotEnoughDataError if the source ran out while
// decoding.
pub fn (b &Reader) complete() ! {
	if b.bad {
		return NotEnoughDataError{}
	}
}

// ok returns true if the reader is still ok.
pub fn (b &Reader) ok() bool {
	return !b.bad
}
