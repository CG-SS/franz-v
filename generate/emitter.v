// The emitter half of the generator: turns parsed StructDefs into V source.
module main

// Emitter accumulates emitted lines at a running indent depth.
struct Emitter {
mut:
	out   []string
	depth int
}

fn (mut e Emitter) w(line string) {
	if line == '' {
		e.out << ''
		return
	}
	e.out << '\t'.repeat(e.depth) + line
}

fn (mut e Emitter) open(line string) {
	e.w(line)
	e.depth++
}

fn (mut e Emitter) close(line string) {
	e.depth--
	e.w(line)
}

// ---------------------------------------------------------------------------
// Type rendering
// ---------------------------------------------------------------------------

fn v_type_of_elem(et ElemType) string {
	return match et {
		PrimitiveT { primitives[et.prim] }
		StringT { 'string' }
		StructRef { et.name }
		EnumRef { et.name }
	}
}

fn v_type(ft FieldType) string {
	return match ft {
		PrimitiveT {
			primitives[ft.prim]
		}
		StringT {
			'string'
		}
		NullableStringT {
			'?string'
		}
		BytesT {
			'[]u8'
		}
		NullableBytesT {
			'?[]u8'
		}
		VarintStringT {
			'string'
		}
		VarintBytesT {
			'?[]u8'
		}
		LengthFieldMinusT {
			'[]u8'
		}
		EnumRef {
			ft.name
		}
		StructT {
			if ft.nullable {
				'?' + ft.name
			} else {
				ft.name
			}
		}
		ArrayT {
			base := '[]' + v_type_of_elem(ft.elem)
			if ft.always_nullable || ft.nullable_version > 0 {
				'?' + base
			} else {
				base
			}
		}
	}
}

// v_default renders a struct-literal default clause, if non-zero.
fn v_default(f Field) ?string {
	d := f.default_val or { return none }
	ft := f.typ
	match ft {
		PrimitiveT {
			zero := if ft.prim == 'bool' { 'false' } else { '0' }
			if d == zero {
				return none
			}
			return match ft.prim {
				'int64' { 'i64(${d})' }
				'int16' { 'i16(${d})' }
				'int8' { 'i8(${d})' }
				else { d }
			}
		}
		EnumRef {
			if d == '0' {
				return none
			}
			return '${ft.name}(${d})'
		}
		StringT, NullableStringT {
			if d != '' {
				return "'${d}'"
			}
			return none
		}
		else {
			return none
		}
	}
}

fn version_guard(f Field) ?string {
	mut conds := []string{}
	if f.min_version > 0 {
		conds << 'version >= ${f.min_version}'
	}
	if maxv := f.max_version {
		conds << 'version <= ${maxv}'
	}
	if conds.len == 0 {
		return none
	}
	return conds.join(' && ')
}

fn zero_value_of_elem(et ElemType) string {
	match et {
		PrimitiveT {
			return match et.prim {
				'bool' { 'false' }
				'uuid' { '[16]u8{}' }
				'int64', 'varlong' { 'i64(0)' }
				'int16' { 'i16(0)' }
				'int8' { 'i8(0)' }
				'uint16' { 'u16(0)' }
				'uint32' { 'u32(0)' }
				'float64' { 'f64(0)' }
				else { '0' }
			}
		}
		StringT {
			return "''"
		}
		StructRef {
			return et.name + '{}'
		}
		EnumRef {
			return '${et.name}(0)'
		}
	}
}

// tag_default_check renders the condition under which a tagged field is
// non-default and must therefore be encoded.
fn tag_default_check(f Field) !string {
	fname := snake(f.name)
	ft := f.typ
	match ft {
		ArrayT {
			if ft.always_nullable || ft.nullable_version > 0 {
				return 'm.${fname} != none'
			}
			return 'm.${fname}.len > 0'
		}
		PrimitiveT {
			if ft.prim == 'uuid' {
				return 'm.${fname} != [16]u8{}'
			}
			zero := if ft.prim == 'bool' { 'false' } else { '0' }
			d := f.default_val or { zero }
			if ft.prim == 'bool' {
				return if d == 'false' { 'm.${fname}' } else { '!m.${fname}' }
			}
			return 'm.${fname} != ${d}'
		}
		EnumRef {
			d := f.default_val or { '0' }
			return 'm.${fname} != ${ft.name}(${d})'
		}
		StringT {
			d := f.default_val or { '' }
			return "m.${fname} != '${d}'"
		}
		NullableStringT {
			return 'm.${fname} != none'
		}
		StructT {
			if ft.nullable {
				return 'm.${fname} != none'
			}
			// non-default means: differs from the zero-value struct
			return 'm.${fname} != ${ft.name}{}'
		}
		else {
			return error('tag default for ${v_type(ft)} not supported yet')
		}
	}
}

// enum_casts returns the write cast, kbin write fn and read fn for an
// enum's underlying wire type.
fn enum_casts(underlying string) (string, string, string) {
	return match underlying {
		'int8' { 'i8', 'write_int8', 'read_int8' }
		'int16' { 'i16', 'write_int16', 'read_int16' }
		'int32' { 'int', 'write_int32', 'read_int32' }
		else { 'int', 'write_int32', 'read_int32' }
	}
}

// ---------------------------------------------------------------------------
// Value writers
// ---------------------------------------------------------------------------

const write_fns = {
	'bool':    'write_bool'
	'int8':    'write_int8'
	'int16':   'write_int16'
	'int32':   'write_int32'
	'int64':   'write_int64'
	'float64': 'write_float64'
	'uuid':    'write_uuid'
	'varint':  'write_varint'
	'varlong': 'write_varlong'
	'uint16':  'write_uint16'
	'uint32':  'write_uint32'
}

const read_fns = {
	'bool':    'read_bool'
	'int8':    'read_int8'
	'int16':   'read_int16'
	'int32':   'read_int32'
	'int64':   'read_int64'
	'float64': 'read_float64'
	'uuid':    'read_uuid'
	'varint':  'read_varint'
	'varlong': 'read_varlong'
	'uint16':  'read_uint16'
	'uint32':  'read_uint32'
}

fn emit_write_string_like(mut e Emitter, compact string, plain string, src string) {
	e.open('if flexible {')
	e.w('w.${compact}(${src})')
	e.close('}')
	e.open('else {')
	e.w('w.${plain}(${src})')
	e.close('}')
}

fn emit_write_elem(mut e Emitter, et ElemType, src string) {
	match et {
		PrimitiveT {
			e.w('w.${write_fns[et.prim]}(${src})')
		}
		StringT {
			emit_write_string_like(mut e, 'write_compact_string', 'write_string', src)
		}
		StructRef {
			e.w('${src}.write_to_versioned(mut w, version, flexible)')
		}
		EnumRef {
			cast, wfn, _ := enum_casts(et.underlying)
			e.w('w.${wfn}(${cast}(${src}))')
		}
	}
}

fn emit_write_value(mut e Emitter, ft FieldType, src string, uniq string) {
	match ft {
		PrimitiveT {
			e.w('w.${write_fns[ft.prim]}(${src})')
		}
		StringT {
			emit_write_string_like(mut e, 'write_compact_string', 'write_string', src)
		}
		NullableStringT {
			if ft.nullable_version > 0 {
				// below nullable_version: plain string, none -> ''
				e.open('if version < ${ft.nullable_version} {')
				e.w("plain := ${src} or { '' }")
				emit_write_string_like(mut e, 'write_compact_string', 'write_string', 'plain')
				e.close('}')
				e.open('else {')
				emit_write_string_like(mut e, 'write_compact_nullable_string',
					'write_nullable_string', src)
				e.close('}')
			} else {
				emit_write_string_like(mut e, 'write_compact_nullable_string',
					'write_nullable_string', src)
			}
		}
		BytesT {
			emit_write_string_like(mut e, 'write_compact_bytes', 'write_bytes', src)
		}
		NullableBytesT {
			emit_write_string_like(mut e, 'write_compact_nullable_bytes', 'write_nullable_bytes',
				src)
		}
		VarintStringT {
			e.w('w.write_varint_string(${src})')
		}
		VarintBytesT {
			e.w('w.write_varint_bytes(${src})')
		}
		LengthFieldMinusT {
			// raw bytes; the referenced length field covers them
			e.w('w.buf << ${src}')
		}
		EnumRef {
			cast, wfn, _ := enum_casts(ft.underlying)
			e.w('w.${wfn}(${cast}(${src}))')
		}
		StructT {
			if ft.nullable {
				// wire: int8 presence marker, -1 null / 1 present
				e.open('if sub_${uniq} := ${src} {')
				e.w('w.write_int8(1)')
				e.w('sub_${uniq}.write_to_versioned(mut w, version, flexible)')
				e.close('}')
				e.open('else {')
				e.w('w.write_int8(-1)')
				e.close('}')
			} else {
				e.w('${src}.write_to_versioned(mut w, version, flexible)')
			}
		}
		ArrayT {
			emit_write_array(mut e, ft, src, uniq)
		}
	}
}

fn emit_write_array(mut e Emitter, ft ArrayT, src string, uniq string) {
	arr := 'arr_${uniq}'
	if !ft.always_nullable && ft.nullable_version == 0 {
		e.w('${arr} := ${src}')
		emit_write_array_body(mut e, ft, arr, uniq)
		return
	}

	// optional field: unwrap; none -> either null marker or empty array
	e.open('if ${arr} := ${src} {')
	emit_write_array_body(mut e, ft, arr, uniq)
	e.close('}')
	e.open('else {')
	if ft.nullable_version == 0 {
		// nullable at every version
		e.open('if flexible {')
		e.w('w.write_uvarint(0)')
		e.close('}')
		e.open('else {')
		e.w('w.write_int32(-1)')
		e.close('}')
	} else {
		e.open('if version >= ${ft.nullable_version} {')
		e.open('if flexible {')
		e.w('w.write_uvarint(0)')
		e.close('}')
		e.open('else {')
		e.w('w.write_int32(-1)')
		e.close('}')
		e.close('}')
		e.open('else {')
		e.open('if flexible {')
		e.w('w.write_compact_array_len(0)')
		e.close('}')
		e.open('else {')
		e.w('w.write_array_len(0)')
		e.close('}')
		e.close('}')
	}
	e.close('}')
}

fn emit_write_array_body(mut e Emitter, ft ArrayT, arr string, uniq string) {
	e.open('if flexible {')
	e.w('w.write_compact_array_len(${arr}.len)')
	e.close('}')
	e.open('else {')
	e.w('w.write_array_len(${arr}.len)')
	e.close('}')
	e.open('for item_${uniq} in ${arr} {')
	emit_write_elem(mut e, ft.elem, 'item_${uniq}')
	e.close('}')
}

// ---------------------------------------------------------------------------
// Value readers
// ---------------------------------------------------------------------------

fn emit_read_string_like(mut e Emitter, compact string, plain string, dst string) {
	e.open('if flexible {')
	e.w('${dst} = r.${compact}()')
	e.close('}')
	e.open('else {')
	e.w('${dst} = r.${plain}()')
	e.close('}')
}

fn emit_read_value(mut e Emitter, ft FieldType, dst string, uniq string) {
	match ft {
		PrimitiveT {
			e.w('${dst} = r.${read_fns[ft.prim]}()')
		}
		StringT {
			emit_read_string_like(mut e, 'read_compact_string', 'read_string', dst)
		}
		NullableStringT {
			if ft.nullable_version > 0 {
				e.open('if version < ${ft.nullable_version} {')
				emit_read_string_like(mut e, 'read_compact_string', 'read_string', dst)
				e.close('}')
				e.open('else {')
				emit_read_string_like(mut e, 'read_compact_nullable_string',
					'read_nullable_string', dst)
				e.close('}')
			} else {
				emit_read_string_like(mut e, 'read_compact_nullable_string',
					'read_nullable_string', dst)
			}
		}
		BytesT {
			emit_read_string_like(mut e, 'read_compact_bytes', 'read_bytes', dst)
		}
		NullableBytesT {
			emit_read_string_like(mut e, 'read_compact_nullable_bytes', 'read_nullable_bytes', dst)
		}
		VarintStringT {
			e.w('${dst} = r.read_varint_string()')
		}
		VarintBytesT {
			e.w('${dst} = r.read_varint_bytes()')
		}
		LengthFieldMinusT {
			e.w('${dst} = r.span(int(m.${snake(ft.field)}) - ${ft.minus}).clone()')
		}
		EnumRef {
			_, _, rfn := enum_casts(ft.underlying)
			e.w('${dst} = ${ft.name}(r.${rfn}())')
		}
		StructT {
			if ft.nullable {
				e.open('if r.read_int8() < 0 {')
				e.w('${dst} = none')
				e.close('}')
				e.open('else {')
				e.w('mut sub_${uniq} := ${ft.name}{}')
				e.w('sub_${uniq}.read_from_versioned(mut r, version, flexible)!')
				e.w('${dst} = sub_${uniq}')
				e.close('}')
			} else {
				e.w('${dst}.read_from_versioned(mut r, version, flexible)!')
			}
		}
		ArrayT {
			emit_read_array(mut e, ft, dst, uniq)
		}
	}
}

fn emit_read_array(mut e Emitter, ft ArrayT, dst string, uniq string) {
	elem_t := v_type_of_elem(ft.elem)
	e.open('if flexible {')
	e.w('arr_len = r.read_compact_array_len()')
	e.close('}')
	e.open('else {')
	e.w('arr_len = r.read_array_len()')
	e.close('}')

	optional := ft.always_nullable || ft.nullable_version > 0
	if optional {
		e.open('if arr_len < 0 {')
		e.w('${dst} = none')
		e.close('}')
		e.open('else {')
	}
	arr := 'arr_${uniq}'
	item := 'item_${uniq}'
	e.w('mut ${arr} := []${elem_t}{cap: if arr_len > 0 { arr_len } else { 0 }}')
	e.open('for _ in 0 .. arr_len {')
	match ft.elem {
		StructRef {
			e.w('mut ${item} := ${ft.elem.name}{}')
			e.w('${item}.read_from_versioned(mut r, version, flexible)!')
			e.w('${arr} << ${item}')
		}
		PrimitiveT {
			e.w('mut ${item} := ${zero_value_of_elem(ft.elem)}')
			e.w('${item} = r.${read_fns[ft.elem.prim]}()')
			e.w('${arr} << ${item}')
		}
		StringT {
			e.w('mut ${item} := ${zero_value_of_elem(ft.elem)}')
			emit_read_string_like(mut e, 'read_compact_string', 'read_string', item)
			e.w('${arr} << ${item}')
		}
		EnumRef {
			_, _, rfn := enum_casts(ft.elem.underlying)
			e.w('mut ${item} := ${zero_value_of_elem(ft.elem)}')
			e.w('${item} = ${ft.elem.name}(r.${rfn}())')
			e.w('${arr} << ${item}')
		}
	}

	e.close('}')
	e.w('${dst} = ${arr}')
	if optional {
		e.close('}')
	}
}

// ---------------------------------------------------------------------------
// Struct emission
// ---------------------------------------------------------------------------

fn emit_write_tag_payload(mut e Emitter, f Field) {
	// encode the payload into temp writer `tw` by generating against `w`
	// and renaming; the rename is a plain text transform on emitted lines.
	mut sub := Emitter{
		depth: e.depth
	}
	emit_write_value(mut sub, f.typ, 'm.${snake(f.name)}', 'tag_' + snake(f.name))
	for line in sub.out {
		e.out << line.replace('w.', 'tw.').replace('mut w,', 'mut tw,')
	}
}

fn uses_array(ft FieldType) bool {
	return ft is ArrayT
}

// flexible_expr renders the flexibility test for a struct's own version.
fn flexible_expr(st StructDef) string {
	if fa := st.flexible_at {
		return 'version >= ${fa}'
	}
	return 'false'
}

fn emit_struct(mut e Emitter, st StructDef) ! {
	mut tagged := st.fields.filter(it.tag != none)
	tagged.sort_with_compare(fn (a &Field, b &Field) int {
		at := a.tag or { 0 }
		bt := b.tag or { 0 }
		return at - bt
	})
	plain := st.fields.filter(it.tag == none)
	versioned := st.top_level || (st.named && st.with_version_field)
	has_wrappers := st.top_level || (st.named && !st.no_encoding)

	// ---- struct definition
	e.open('pub struct ${st.name} {')
	e.w('pub mut:')
	if st.top_level {
		e.w('\t// version is the wire version to encode with / that was used')
		e.w('\t// by the request this response answers. Set it before use.')
		e.w('\tversion i16')
	} else if st.with_version_field {
		e.w('\t// version is the wire-encoded int16 version prefix of this record.')
		e.w('\tversion i16')
	}
	for f in st.fields {
		mut decl := '\t${snake(f.name)} ${v_type(f.typ)}'
		if d := v_default(f) {
			decl += ' = ${d}'
		}
		e.w(decl)
	}
	e.w('\t// unknown_tags preserves tagged fields this client does not know.')
	e.w('\tunknown_tags []UnknownTag')
	e.close('}')
	e.w('')

	if st.top_level {
		e.w('// key returns the Kafka API key of ${st.name} (${st.key}).')
		e.open('pub fn (m &${st.name}) key() i16 {')
		e.w('return ${st.key}')
		e.close('}')
		e.w('')
		e.w('// max_version returns the maximum wire version this client supports.')
		e.open('pub fn (m &${st.name}) max_version() i16 {')
		e.w('return ${st.max_version}')
		e.close('}')
		e.w('')
		e.w('// is_flexible reports whether m.version uses flexible (compact) encoding.')
		e.open('pub fn (m &${st.name}) is_flexible() bool {')
		if fa := st.flexible_at {
			e.w('return m.version >= ${fa}')
		} else {
			e.w('return false')
		}
		e.close('}')
		e.w('')
	}

	// ---- public write_to wrapper
	if has_wrappers {
		if st.top_level {
			e.w('// write_to encodes m at m.version into w.')
			e.open('pub fn (m &${st.name}) write_to(mut w kbin.Writer) {')
			e.w('version := m.version')
			e.w('flexible := m.is_flexible()')
			e.w('m.write_to_versioned(mut w, version, flexible)')
			e.close('}')
		} else if st.with_version_field {
			e.w('// write_to encodes m, prefixed by its wire-encoded version.')
			e.open('pub fn (m &${st.name}) write_to(mut w kbin.Writer) {')
			e.w('w.write_int16(m.version)')
			e.w('version := m.version')
			e.w('flexible := ${flexible_expr(st)}')
			e.w('m.write_to_versioned(mut w, version, flexible)')
			e.close('}')
		} else {
			v0_flexible := if fa := st.flexible_at {
				if fa == 0 { 'true' } else { 'false' }
			} else {
				'false'
			}
			e.w('// write_to encodes m (this type is versionless on the wire).')
			e.open('pub fn (m &${st.name}) write_to(mut w kbin.Writer) {')
			e.w('m.write_to_versioned(mut w, 0, ${v0_flexible})')
			e.close('}')
		}
		e.w('')
	}
	e.open('fn (m &${st.name}) write_to_versioned(mut w kbin.Writer, version i16, flexible bool) {')
	e.w('_ = version')
	e.w('_ = flexible')
	for f in plain {
		guard := version_guard(f)
		if g := guard {
			e.open('if ${g} {')
		}
		emit_write_value(mut e, f.typ, 'm.${snake(f.name)}', snake(f.name))
		if guard != none {
			e.close('}')
		}
	}
	// tag section
	e.open('if flexible {')
	if tagged.len > 0 {
		e.w('mut tag_buf := kbin.Writer{}')
		e.w('mut num_tags := u32(0)')
		for f in tagged {
			mut cond := tag_default_check(f)!
			if g := version_guard(f) {
				cond = '(${g}) && ${cond}'
			}
			e.open('if ${cond} {')
			e.w('num_tags++')
			e.w('mut tw := kbin.Writer{}')
			emit_write_tag_payload(mut e, f)
			tagnum := f.tag or { 0 }
			e.w('tag_buf.write_uvarint(${tagnum})')
			e.w('tag_buf.write_uvarint(u32(tw.buf.len))')
			e.w('tag_buf.buf << tw.buf')
			e.close('}')
		}
		e.w('w.write_uvarint(num_tags + u32(m.unknown_tags.len))')
		e.w('w.buf << tag_buf.buf')
		e.w('write_unknown_tags(mut w, m.unknown_tags)')
	} else {
		e.w('w.write_uvarint(u32(m.unknown_tags.len))')
		e.w('write_unknown_tags(mut w, m.unknown_tags)')
	}
	e.close('}')
	e.close('}')
	e.w('')

	// ---- public read_from wrapper
	if has_wrappers {
		if st.top_level {
			e.w('// read_from decodes into m using m.version; call on a fresh')
			e.w('// struct (or after reassigning m = ${st.name}{version: v}).')
			e.open('pub fn (mut m ${st.name}) read_from(mut r kbin.Reader) ! {')
			e.w('version := m.version')
			e.w('flexible := m.is_flexible()')
			e.w('m.read_from_versioned(mut r, version, flexible)!')
			e.w('r.complete()!')
			e.close('}')
		} else if st.with_version_field {
			e.w('// read_from decodes into m, reading the wire-encoded version')
			e.w('// prefix first; call on a fresh struct.')
			e.open('pub fn (mut m ${st.name}) read_from(mut r kbin.Reader) ! {')
			e.w('m.version = r.read_int16()')
			e.w('version := m.version')
			e.w('flexible := ${flexible_expr(st)}')
			e.w('m.read_from_versioned(mut r, version, flexible)!')
			e.w('r.complete()!')
			e.close('}')
		} else {
			v0_flexible := if fa := st.flexible_at {
				if fa == 0 { 'true' } else { 'false' }
			} else {
				'false'
			}
			e.w('// read_from decodes into m (this type is versionless on the')
			e.w('// wire); call on a fresh struct.')
			e.open('pub fn (mut m ${st.name}) read_from(mut r kbin.Reader) ! {')
			e.w('m.read_from_versioned(mut r, 0, ${v0_flexible})!')
			e.w('r.complete()!')
			e.close('}')
		}
		e.w('')
	}
	e.open('fn (mut m ${st.name}) read_from_versioned(mut r kbin.Reader, version i16, flexible bool) ! {')
	e.w('_ = version')
	e.w('_ = flexible')
	if st.fields.any(uses_array(it.typ)) {
		e.w('mut arr_len := 0')
		e.w('_ = arr_len')
	}
	for f in plain {
		guard := version_guard(f)
		if g := guard {
			e.open('if ${g} {')
		}
		emit_read_value(mut e, f.typ, 'm.${snake(f.name)}', snake(f.name))
		if guard != none {
			e.close('}')
		}
	}
	// tag section
	e.open('if flexible {')
	e.w('num_tags := r.read_uvarint()')
	e.open('for _ in 0 .. num_tags {')
	e.w('tag := r.read_uvarint()')
	e.w('size := int(r.read_uvarint())')
	if tagged.len > 0 {
		for i, f in tagged {
			kw := if i == 0 { 'if' } else { 'else if' }
			tagnum := f.tag or { 0 }
			e.open('${kw} tag == ${tagnum} {')
			emit_read_value(mut e, f.typ, 'm.${snake(f.name)}', 'tag_' + snake(f.name))
			e.close('}')
		}
		e.open('else {')
		e.w('data := r.span(size)')
		e.open('if r.ok() {')
		e.w('m.unknown_tags << UnknownTag{')
		e.w('\ttag:  tag')
		e.w('\tdata: data.clone()')
		e.w('}')
		e.close('}')
		e.close('}')
	} else {
		e.w('data := r.span(size)')
		e.open('if r.ok() {')
		e.w('m.unknown_tags << UnknownTag{')
		e.w('\ttag:  tag')
		e.w('\tdata: data.clone()')
		e.w('}')
		e.close('}')
	}
	e.close('}')
	e.close('}')
	e.w('r.complete()!')
	e.close('}')
	e.w('')
	_ = versioned
}

// emit_file renders all structs of one definitions file into V source.
fn emit_file(structs []StructDef, src_name string) !string {
	mut e := Emitter{}
	e.w('// Code generated by `v run generate/` from')
	e.w('// franz-go generate/definitions/${src_name} — DO NOT EDIT.')
	e.w('module kmsg')
	e.w('')
	e.w('import kbin')
	e.w('')
	for st in structs {
		emit_struct(mut e, st)!
	}
	return e.out.join('\n') + '\n'
}

// emit_enums_file renders the enums registry as open type aliases plus
// named constants. Open aliases (rather than closed V enums) are a
// deliberate wire-safety choice: brokers may send values newer than this
// client knows, and decoding those into a closed enum would be undefined.
fn emit_enums_file(enums []EnumDef) string {
	mut e := Emitter{}
	e.w('// Code generated by `v run generate/` from')
	e.w('// franz-go generate/definitions/enums — DO NOT EDIT.')
	e.w('module kmsg')
	e.w('')
	for en in enums {
		e.w('// ${en.name} is a Kafka protocol enum, kept as an open alias so')
		e.w('// unknown future wire values still decode losslessly.')
		e.w('pub type ${en.name} = ${primitives[en.underlying]}')
		e.w('')
		prefix := snake(en.name)
		for val in en.vals {
			e.w('pub const ${prefix}_${snake(val.name)} = ${en.name}(${val.num})')
		}
		e.w('')
	}
	return e.out.join('\n') + '\n'
}
