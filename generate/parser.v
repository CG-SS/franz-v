// gen — the franz-v kmsg code generator, self-hosted in V.
//
// Parses franz-go's generate/definitions DSL and emits V structs with
// write_to/read_from methods encoding the Kafka wire protocol via kbin.
//
// Usage: v run generate/ <definitions_dir> <out_dir> <family_files...>
// The `enums` and `misc` definition files are always parsed first (when
// present) since message families reference their types.
//
// This is a dev-time tool; only its V output ships. The emitted code follows
// the project's V-first guidelines (struct defaults, Options, typed errors,
// no unsafe) — and so does this generator itself: the DSL type system is a
// V sum type dispatched with match, optional concepts are Options rather
// than sentinel values.
module main

import os

// ---------------------------------------------------------------------------
// DSL model
// ---------------------------------------------------------------------------

// ElemType is what an array may contain. The DSL as used by the supported
// families never nests arrays directly; a nested array is a hard parse
// error until a definition needs it.
type ElemType = EnumRef | PrimitiveT | StringT | StructRef

struct PrimitiveT {
	prim string // DSL name: bool, int8, ..., uuid, varint, varlong
}

struct StringT {}

struct StructRef {
	name string
}

struct EnumRef {
	name       string
	underlying string // int8 / int16 / int32
}

// FieldType is the type of one struct field.
type FieldType = ArrayT
	| BytesT
	| EnumRef
	| LengthFieldMinusT
	| NullableBytesT
	| NullableStringT
	| PrimitiveT
	| StringT
	| StructT
	| VarintBytesT
	| VarintStringT

struct NullableStringT {
	// nullable_version 0 means nullable at every version; N means the field
	// is a plain string below vN and nullable from vN on.
	nullable_version int
}

struct BytesT {}

struct NullableBytesT {}

// VarintStringT is a varint-length-prefixed string (Record headers).
struct VarintStringT {}

// VarintBytesT is a varint-length-prefixed nullable byte blob (Record
// keys/values/headers).
struct VarintBytesT {}

// LengthFieldMinusT is a raw byte blob whose decoded length is the value of
// an earlier int field minus a constant (RecordBatch.Records).
struct LengthFieldMinusT {
	field string // DSL name of the referenced length field
	minus int
}

// StructT is a struct-typed field: either an inline `=>` definition or a
// reference to a named `not top level` struct. When nullable, the wire
// carries a single int8 presence marker (-1 null, 1 present) before the
// fields.
struct StructT {
	name     string
	nullable bool
}

struct ArrayT {
	elem             ElemType
	nullable_version int
	always_nullable  bool
}

struct Field {
mut:
	name        string
	typ         FieldType = PrimitiveT{
		prim: 'bool'
	}
	min_version int
	max_version ?int
	tag         ?int
	default_val ?string
	is_throttle bool
	is_timeout  bool
}

struct StructDef {
mut:
	name               string
	fields             []Field
	top_level          bool
	key                int = -1
	max_version        int = -1
	flexible_at        ?int
	is_request         bool
	named              bool // declared `=> not top level`
	with_version_field bool // carries a wire-encoded int16 version prefix
	no_encoding        bool // no public write_to/read_from wrappers
}

// EnumVal is one value of a DSL enum.
struct EnumVal {
	num  int
	name string
}

struct EnumDef {
mut:
	name       string
	underlying string
	vals       []EnumVal
}

// ---------------------------------------------------------------------------
// Small text helpers
// ---------------------------------------------------------------------------

const v_keywords = ['type', 'match', 'none', 'true', 'false', 'module', 'struct', 'interface',
	'enum', 'union', 'const', 'mut', 'pub', 'fn', 'return', 'in', 'is', 'or', 'select', 'lock',
	'rlock', 'atomic', 'asm', 'defer', 'go', 'spawn', 'map']!

const primitives = {
	'bool':    'bool'
	'int8':    'i8'
	'int16':   'i16'
	'int32':   'int'
	'int64':   'i64'
	'float64': 'f64'
	'uuid':    '[16]u8'
	'varint':  'int'
	'varlong': 'i64'
	'uint16':  'u16'
	'uint32':  'u32'
}

fn is_lower_or_digit(c u8) bool {
	return (c >= `a` && c <= `z`) || (c >= `0` && c <= `9`)
}

fn is_upper(c u8) bool {
	return c >= `A` && c <= `Z`
}

fn is_lower(c u8) bool {
	return c >= `a` && c <= `z`
}

// snake converts GoCamelCase to snake_case, keeping acronym runs readable
// (InvalidGroupID -> invalid_group_id, NodeID -> node_id, ISR -> isr).
fn snake(name string) string {
	mut out := []u8{cap: name.len + 8}
	for i, c in name {
		if i > 0 && is_upper(c) {
			prev := name[i - 1]
			next_is_lower := i + 1 < name.len && is_lower(name[i + 1])
			if is_lower_or_digit(prev) || (is_upper(prev) && next_is_lower) {
				out << `_`
			}
		}
		out << if is_upper(c) { c + 32 } else { c }
	}
	mut s := out.bytestr()
	if s in v_keywords {
		s += '_'
	}
	return s
}

// singular strips a plural suffix for nested struct naming
// (Topics -> Topic, Statuses -> Status).
fn singular(name string) string {
	if name.ends_with('ses') {
		return name[..name.len - 2]
	}
	if name.ends_with('s') && !name.ends_with('ss') {
		return name[..name.len - 1]
	}
	return name
}

fn line_indent(line string) int {
	return line.len - line.trim_left(' ').len
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

fn parse_int(s string) ?int {
	mut t := s
	mut neg := false
	if t.starts_with('-') {
		neg = true
		t = t[1..]
	}
	if !all_digits(t) {
		return none
	}
	v := t.int()
	return if neg { -v } else { v }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct VersionComment {
	min_version int
	max_version ?int
	tag         ?int
}

// parse_version_comment reads '// vA+', '// vA-vB' and '// tag N'
// annotations from a field's trailing comment.
fn parse_version_comment(comment string) VersionComment {
	mut minv := 0
	mut maxv := ?int(none)
	mut tag := ?int(none)
	toks := comment.trim_space().split(' ')
	for i, tok in toks {
		if tok.starts_with('v') && tok.ends_with('+') && all_digits(tok[1..tok.len - 1]) {
			minv = tok[1..tok.len - 1].int()
		} else if tok.starts_with('v') && tok.contains('-v') {
			parts := tok.split('-v')
			if parts.len == 2 && parts[0].len > 1 && all_digits(parts[0][1..])
				&& all_digits(parts[1]) {
				minv = parts[0][1..].int()
				maxv = parts[1].int()
			}
		} else if tok == 'tag' && i + 1 < toks.len && all_digits(toks[i + 1]) {
			tag = toks[i + 1].int()
		}
	}
	return VersionComment{
		min_version: minv
		max_version: maxv
		tag:         tag
	}
}

// Generator carries the cross-file state: named struct and enum registries
// persist across families (e.g. TaskIDs is defined in 88_* and referenced
// in 89_*), while lines/idx/file_structs reset per file.
struct Generator {
mut:
	lines        []string
	idx          int
	file_structs []StructDef
	known        map[string]bool    // named struct registry
	enums        map[string]EnumDef // enum registry
}

// parse_elem_type resolves the inside of [...] to an array element type.
fn (g &Generator) parse_elem_type(typ string, parent string, fname string) !ElemType {
	if typ in primitives {
		return PrimitiveT{
			prim: typ
		}
	}
	if typ == 'string' {
		return StringT{}
	}
	if typ.starts_with('enum-') {
		ename := typ['enum-'.len..]
		e := g.enums[ename] or { return error('unknown enum `${ename}` in ${parent}.${fname}') }
		return EnumRef{
			name:       ename
			underlying: e.underlying
		}
	}
	if typ in g.known {
		return StructRef{
			name: typ
		}
	}
	return error('unsupported array element type `${typ}` in ${parent}.${fname}')
}

// parse_inline_struct parses the indented body following a `=>` field into
// a new named struct and registers it.
fn (mut g Generator) parse_inline_struct(child_name string, indent int) !StructDef {
	mut child := StructDef{
		name: child_name
	}
	g.parse_struct_body(mut child, indent + 2)!
	g.file_structs << child
	g.known[child_name] = true
	return child
}

// parse_type resolves a DSL type string; `=>` forms consume the following
// indented lines as a nested struct definition.
fn (mut g Generator) parse_type(typ_ string, parent string, fname string, indent int) !FieldType {
	mut typ := typ_
	mut nullable_version := 0
	mut always_nullable := false

	// length-field-minus => Field - N  (contains spaces; handled first)
	if typ.starts_with('length-field-minus => ') {
		rest := typ['length-field-minus => '.len..]
		parts := rest.split(' - ')
		if parts.len != 2 {
			return error('bad length-field-minus `${typ_}` in ${parent}.${fname}')
		}
		minus := parse_int(parts[1].trim_space()) or {
			return error('bad length-field-minus amount `${typ_}` in ${parent}.${fname}')
		}
		return LengthFieldMinusT{
			field: parts[0].trim_space()
			minus: minus
		}
	}

	if typ.starts_with('nullable-v') {
		rest := typ['nullable-v'.len..]
		plus := rest.index('+') or {
			return error('bad nullable version prefix `${typ_}` in ${parent}.${fname}')
		}
		if !all_digits(rest[..plus]) {
			return error('bad nullable version number `${typ_}` in ${parent}.${fname}')
		}
		nullable_version = rest[..plus].int()
		always_nullable = true
		typ = rest[plus + 1..]
	} else if typ.starts_with('nullable[') {
		always_nullable = true
		typ = typ['nullable'.len..]
	}

	// inline struct fields: `=>` and `nullable=>`
	if typ == '=>' || typ == 'nullable=>' {
		child := g.parse_inline_struct(parent + fname, indent)!
		return StructT{
			name:     child.name
			nullable: typ.starts_with('nullable')
		}
	}

	if typ.starts_with('[') && typ.ends_with(']') {
		inner := typ[1..typ.len - 1]
		if inner.starts_with('=>') {
			// `[=>]` uses singular(field name); `[=>Rename]` overrides it
			rename := inner['=>'.len..]
			child_name := if rename == '' {
				parent + singular(fname)
			} else {
				parent + rename
			}
			child := g.parse_inline_struct(child_name, indent)!
			return ArrayT{
				elem:             StructRef{
					name: child.name
				}
				nullable_version: nullable_version
				always_nullable:  always_nullable
			}
		}
		return ArrayT{
			elem:             g.parse_elem_type(inner, parent, fname)!
			nullable_version: nullable_version
			always_nullable:  always_nullable
		}
	}

	if typ in primitives {
		return PrimitiveT{
			prim: typ
		}
	}
	if typ == 'string' {
		return StringT{}
	}
	if typ == 'nullable-string' {
		return NullableStringT{}
	}
	if typ.starts_with('nullable-string-v') && typ.ends_with('+') {
		num := typ['nullable-string-v'.len..typ.len - 1]
		if !all_digits(num) {
			return error('bad nullable-string version in `${typ_}` at ${parent}.${fname}')
		}
		return NullableStringT{
			nullable_version: num.int()
		}
	}
	if typ == 'bytes' {
		return BytesT{}
	}
	if typ == 'nullable-bytes' {
		return NullableBytesT{}
	}
	if typ == 'varint-string' {
		return VarintStringT{}
	}
	if typ == 'varint-bytes' {
		return VarintBytesT{}
	}
	if typ.starts_with('enum-') {
		ename := typ['enum-'.len..]
		e := g.enums[ename] or { return error('unknown enum `${ename}` in ${parent}.${fname}') }
		return EnumRef{
			name:       ename
			underlying: e.underlying
		}
	}
	if typ in g.known {
		return StructT{
			name: typ
		}
	}
	return error('unsupported type `${typ_}` in ${parent}.${fname}')
}

// parse_struct_body parses fields at exactly `indent` until dedent.
fn (mut g Generator) parse_struct_body(mut st StructDef, indent int) ! {
	for g.idx < g.lines.len {
		raw := g.lines[g.idx]
		trimmed := raw.trim_space()
		if trimmed == '' || trimmed.starts_with('//') {
			g.idx++
			continue
		}
		if line_indent(raw) < indent {
			return
		}
		if line_indent(raw) != indent {
			return error('unexpected indent at: `${raw}`')
		}
		g.idx++

		mut line := trimmed
		mut comment := ''
		if idx := line.index('//') {
			comment = line[idx + 2..]
			line = line[..idx].trim_space()
		}

		// ThrottleMillis(N) / TimeoutMillis(N) / TimeoutMillis specials
		if line.starts_with('ThrottleMillis') || line.starts_with('TimeoutMillis') {
			base := line.all_before('(')
			if base == 'ThrottleMillis' || base == 'TimeoutMillis' {
				vc := parse_version_comment(comment)
				mut f := Field{
					name:        base
					typ:         PrimitiveT{
						prim: 'int32'
					}
					min_version: vc.min_version
					max_version: vc.max_version
					tag:         vc.tag
					is_throttle: base == 'ThrottleMillis'
					is_timeout:  base == 'TimeoutMillis'
				}
				if f.is_timeout {
					f.default_val = '15000' // franz-go TimeoutMillis default
				}
				st.fields << f
				continue
			}
		}

		colon := line.index(':') or { return error('cannot parse field line: `${raw}`') }
		fname := line[..colon]
		mut typstr := line[colon + 1..].trim_space()
		if typstr.contains(' ') && !typstr.starts_with('length-field-minus') {
			return error('cannot parse field line: `${raw}`')
		}

		mut default_val := ?string(none)
		if typstr.ends_with(')') && !typstr.contains('=>') {
			if open := typstr.index('(') {
				inner := typstr[open + 1..typstr.len - 1]
				if !inner.contains('(') && !inner.contains(')') {
					default_val = inner
					typstr = typstr[..open]
				}
			}
		}

		vc := parse_version_comment(comment)
		ft := g.parse_type(typstr, st.name, fname, indent)!
		st.fields << Field{
			name:        fname
			typ:         ft
			min_version: vc.min_version
			max_version: vc.max_version
			tag:         vc.tag
			default_val: default_val
		}
	}
}

// parse_named_header applies `not top level` header flags to st.
fn parse_named_header(mut st StructDef, segments []string) {
	st.named = true
	for seg in segments {
		s := seg.trim_space()
		if s == 'no encoding' {
			st.no_encoding = true
		} else if s == 'with version field' {
			st.with_version_field = true
		} else if s.starts_with('flexible v') && s.ends_with('+') {
			st.flexible_at = s['flexible v'.len..s.len - 1].int()
		}
	}
}

// parse_family parses one definitions file, returning its structs; named
// struct and enum registries persist in g across calls.
fn (mut g Generator) parse_family(path string) ![]StructDef {
	content := os.read_file(path)!
	g.lines = content.split_into_lines()
	g.idx = 0
	g.file_structs = []
	mut last_request := ?StructDef(none)
	for g.idx < g.lines.len {
		line := g.lines[g.idx]
		trimmed := line.trim_space()
		if trimmed == '' || trimmed.starts_with('//') {
			g.idx++
			continue
		}
		arrow := line.index('=>') or { return error('expected top-level struct: `${line}`') }
		if line_indent(line) != 0 {
			return error('expected top-level struct: `${line}`')
		}
		mut st := StructDef{
			name: line[..arrow].trim_space()
		}
		header := line[arrow + 2..].trim_space()
		segments := header.split(',')
		g.idx++
		if header.starts_with('not top level') {
			parse_named_header(mut st, segments)
		} else if header != '' {
			for seg in segments {
				s := seg.trim_space()
				if s.starts_with('key ') {
					st.key = s['key '.len..].int()
				} else if s.starts_with('max version ') {
					st.max_version = s['max version '.len..].int()
				} else if s.starts_with('flexible v') && s.ends_with('+') {
					st.flexible_at = s['flexible v'.len..s.len - 1].int()
				}
			}
			st.top_level = true
			st.is_request = true
		} else {
			req := last_request or {
				return error('${st.name}: response without a preceding request')
			}

			st.top_level = true
			st.key = req.key
			st.max_version = req.max_version
			st.flexible_at = req.flexible_at
		}
		g.known[st.name] = true
		g.parse_struct_body(mut st, 2)!

		// `with version field` structs declare `Version: int16` as their
		// first field; it is handled explicitly by the emitter.
		if st.with_version_field {
			if st.fields.len == 0 || st.fields[0].name != 'Version' {
				return error('${st.name}: `with version field` requires a leading Version: int16 field')
			}
			st.fields.delete(0)
		}

		g.file_structs << st
		if st.is_request {
			last_request = st
		}
	}
	return g.file_structs
}

// parse_enums parses the `enums` definitions file into the registry.
fn (mut g Generator) parse_enums(path string) ! {
	content := os.read_file(path)!
	lines := content.split_into_lines()
	mut i := 0
	for i < lines.len {
		trimmed := lines[i].trim_space()
		if trimmed == '' || trimmed.starts_with('//') {
			i++
			continue
		}
		// `Name int8 (`
		if !trimmed.ends_with('(') {
			return error('enums: expected `Name underlying (`, got `${trimmed}`')
		}
		head := trimmed[..trimmed.len - 1].trim_space()
		// optional trailing `camelcase` flag only affects Go const naming;
		// V const names are snake_cased either way
		parts := head.split(' ').filter(it != 'camelcase' && it != '')
		if parts.len != 2 || parts[1] !in primitives {
			return error('enums: bad enum header `${trimmed}`')
		}
		mut e := EnumDef{
			name:       parts[0]
			underlying: parts[1]
		}
		i++
		for i < lines.len {
			v := lines[i].trim_space()
			i++
			if v == ')' {
				break
			}
			if v == '' || v.starts_with('//') {
				continue
			}
			colon := v.index(':') or { return error('enums: bad value line `${v}` in ${e.name}') }
			num := parse_int(v[..colon].trim_space()) or {
				return error('enums: bad value number `${v}` in ${e.name}')
			}
			e.vals << EnumVal{
				num:  num
				name: v[colon + 1..].trim_space()
			}
		}
		g.enums[e.name] = e
	}
}
