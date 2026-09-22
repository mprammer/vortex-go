// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package encoding

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
)

// Canonical scalar values retain IEEE payloads and full unsigned integer bits.
// Variable-width values alias checked input buffers where possible.
type values struct {
	typ     Type
	n       int
	bits    []uint64
	bytes   [][]byte
	wide    []byte
	valid   []bool // nil means all valid
	fields  []*values
	offsets []int32
}
type decoder struct {
	ctx       context.Context
	remaining int64
	depth     int
}

const maxDecodeBytes = 512 << 20
const maxRows = maxDecodeBytes / 16

func Decode(ctx context.Context, node *Node, typ Type, length int) (arrow.Array, error) {
	d := decoder{ctx: ctx, remaining: maxDecodeBytes}
	v, e := d.decode(node, typ, length)
	if e != nil {
		return nil, e
	}
	return v.arrow(ctx)
}
func (d *decoder) reserve(n int64) error {
	if n < 0 || n > d.remaining {
		return fmt.Errorf("array exceeds decoder allocation limit")
	}
	d.remaining -= n
	return nil
}
func (d *decoder) empty(t Type, n int) (*values, error) {
	if n < 0 || n > maxRows {
		return nil, fmt.Errorf("invalid array length %d", n)
	}
	v := &values{typ: t, n: n}
	switch {
	case t.Kind == "utf8" || t.Kind == "binary":
		if e := d.reserve(int64(n) * 24); e != nil {
			return nil, e
		}
		v.bytes = make([][]byte, n)
	case t.Kind == "decimal":
		w := 16
		if t.Precision > 38 {
			w = 32
		}
		if e := d.reserve(int64(n) * int64(w)); e != nil {
			return nil, e
		}
		v.wide = make([]byte, n*w)
	case t.width() > 0:
		if e := d.reserve(int64(n) * 8); e != nil {
			return nil, e
		}
		v.bits = make([]uint64, n)
	case t.Kind == "extension":
		if t.Storage == nil {
			return nil, fmt.Errorf("extension missing storage")
		}
		out, e := d.empty(*t.Storage, n)
		if e != nil {
			return nil, e
		}
		out.typ = t
		return out, nil
	case t.Kind == "null":
		if e := d.reserve(int64(n)); e != nil {
			return nil, e
		}
		v.valid = make([]bool, n)
	case t.Kind == "struct":
		v.fields = make([]*values, len(t.Fields))
		for i, f := range t.Fields {
			x, e := d.empty(f.Type, n)
			if e != nil {
				return nil, e
			}
			v.fields[i] = x
		}
	default:
		return nil, fmt.Errorf("unsupported canonical type %q", t.Kind)
	}
	return v, nil
}
func (v *values) isValid(i int) bool { return v.valid == nil || v.valid[i] }
func (d *decoder) mask(v *values) error {
	if v.valid == nil {
		if e := d.reserve(int64(v.n)); e != nil {
			return e
		}
		v.valid = make([]bool, v.n)
		for i := range v.valid {
			v.valid[i] = true
		}
	}
	return nil
}

// Copy operations must preserve each field's validity, including nullable
// fields inside a non-null struct dictionary value.
func (d *decoder) copyValidityShape(dst, src *values) error {
	if src.valid != nil {
		if err := d.mask(dst); err != nil {
			return err
		}
	}
	for i := range dst.fields {
		if err := d.copyValidityShape(dst.fields[i], src.fields[i]); err != nil {
			return err
		}
	}
	return nil
}

func (v *values) at(dst int, src *values, i int) {
	if v.bits != nil {
		v.bits[dst] = src.bits[i]
	}
	if v.bytes != nil {
		v.bytes[dst] = src.bytes[i]
	}
	if v.wide != nil {
		w := len(v.wide) / max(v.n, 1)
		copy(v.wide[dst*w:(dst+1)*w], src.wide[i*w:(i+1)*w])
	}
	if v.valid != nil {
		v.valid[dst] = src.isValid(i)
	}
	for k := range v.fields {
		v.fields[k].at(dst, src.fields[k], i)
	}
}
func (d *decoder) child(n *Node, i int, t Type, count int) (*values, error) {
	if i < 0 || i >= len(n.Children) {
		return nil, fmt.Errorf("missing child %d", i)
	}
	return d.decode(n.Children[i], t, count)
}
func (d *decoder) validity(n *Node, index, length int) ([]bool, error) {
	if len(n.Children) == index {
		return nil, nil
	}
	if len(n.Children) != index+1 {
		return nil, fmt.Errorf("expected %d or %d children, got %d", index, index+1, len(n.Children))
	}
	v, e := d.child(n, index, Type{Kind: "bool"}, length)
	if e != nil {
		return nil, e
	}
	if e = d.reserve(int64(length)); e != nil {
		return nil, e
	}
	out := make([]bool, length)
	for i, b := range v.bits {
		if !v.isValid(i) {
			return nil, fmt.Errorf("nullable validity child")
		}
		out[i] = b != 0
	}
	return out, nil
}
func boundedInt(x uint64) (int, error) {
	if x > maxRows {
		return 0, fmt.Errorf("array length %d exceeds limit", x)
	}
	return int(x), nil
}
func countChildren(n *Node, c int) error {
	if len(n.Children) != c {
		return fmt.Errorf("expected %d children, got %d", c, len(n.Children))
	}
	return nil
}
func oneBuffer(n *Node) ([]byte, error) {
	if len(n.Buffers) != 1 {
		return nil, fmt.Errorf("expected one buffer, got %d", len(n.Buffers))
	}
	return n.Buffers[0], nil
}
func readWord(b []byte, w int) uint64 {
	switch w {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func writeWord(b []byte, w int, x uint64) {
	switch w {
	case 1:
		b[0] = byte(x)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(x))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(x))
	case 8:
		binary.LittleEndian.PutUint64(b, x)
	}
}
func bitMask(w int) uint64 {
	if w >= 64 {
		return math.MaxUint64
	}
	return uint64(1)<<w - 1
}
func asSigned(x uint64, w int) int64 { return int64(x<<(64-w*8)) >> (64 - w*8) }

func (d *decoder) decode(n *Node, t Type, length int) (v *values, err error) {
	if err = d.ctx.Err(); err != nil {
		return nil, err
	}
	if n == nil {
		return nil, fmt.Errorf("nil encoded array")
	}
	if d.depth >= 64 {
		return nil, fmt.Errorf("array nesting exceeds limit")
	}
	if length < 0 || length > maxRows {
		return nil, fmt.Errorf("invalid array length %d", length)
	}
	d.depth++
	defer func() {
		d.depth--
		// Every encoding and composed child must honor its declared nullability
		// before its values reach a parent decoder or an Arrow schema.
		if err == nil && !t.Nullable && t.Kind != "null" {
			for row, valid := range v.valid {
				if !valid {
					err = fmt.Errorf("null at row %d for nonnullable %s", row, t.Kind)
					v = nil
					break
				}
			}
		}
		if err != nil {
			err = fmt.Errorf("%s: %w", n.Encoding, err)
		}
	}()
	m, e := parseProto(n.Metadata)
	if e != nil {
		return nil, e
	}
	switch n.Encoding {
	case "vortex.null":
		if t.Kind != "null" {
			return nil, fmt.Errorf("null encoding has type %q", t.Kind)
		}
		return d.empty(t, length)
	case "vortex.primitive":
		w := t.width()
		if w == 0 || t.Kind == "bool" {
			return nil, fmt.Errorf("nonprimitive dtype")
		}
		b, e := oneBuffer(n)
		if e != nil {
			return nil, e
		}
		if len(n.Metadata) != 0 || len(b) != length*w {
			return nil, fmt.Errorf("incomplete or invalid primitive buffer")
		}
		v, e = d.empty(t, length)
		if e != nil {
			return nil, e
		}
		for i := range v.bits {
			v.bits[i] = readWord(b[i*w:], w)
		}
		v.valid, e = d.validity(n, 0, length)
		return v, e
	case "vortex.bool", "vortex.bytebool":
		if t.Kind != "bool" {
			return nil, fmt.Errorf("nonboolean dtype")
		}
		b, e := oneBuffer(n)
		if e != nil {
			return nil, e
		}
		off := m.num(1)
		if off >= 8 {
			return nil, fmt.Errorf("invalid boolean offset")
		}
		v, e = d.empty(t, length)
		if e != nil {
			return nil, e
		}
		if n.Encoding == "vortex.bool" {
			if uint64(len(b))*8 < uint64(length)+off {
				return nil, fmt.Errorf("incomplete boolean buffer")
			}
			for i := range v.bits {
				j := uint64(i) + off
				v.bits[i] = uint64((b[j/8] >> (j % 8)) & 1)
			}
		} else {
			if len(b) != length {
				return nil, fmt.Errorf("incomplete bytebool buffer")
			}
			for i, x := range b {
				if x > 1 {
					return nil, fmt.Errorf("invalid bytebool")
				}
				v.bits[i] = uint64(x)
			}
		}
		v.valid, e = d.validity(n, 0, length)
		return v, e
	case "vortex.constant":
		b, e := oneBuffer(n)
		if e != nil {
			return nil, e
		}
		if e = countChildren(n, 0); e != nil {
			return nil, e
		}
		return d.constant(t, length, b)
	case "vortex.struct":
		if t.Kind != "struct" {
			return nil, fmt.Errorf("nonstruct dtype")
		}
		off := len(n.Children) - len(t.Fields)
		if off < 0 || off > 1 {
			return nil, fmt.Errorf("invalid struct child count")
		}
		v = &values{typ: t, n: length, fields: make([]*values, len(t.Fields))}
		if off == 1 {
			mask, e := d.child(n, 0, Type{Kind: "bool"}, length)
			if e != nil {
				return nil, e
			}
			v.valid = make([]bool, length)
			for i, x := range mask.bits {
				v.valid[i] = x != 0
			}
		}
		for i, f := range t.Fields {
			v.fields[i], e = d.child(n, i+off, f.Type, length)
			if e != nil {
				return nil, e
			}
		}
		return v, nil
	case "vortex.varbin", "vortex.varbinview":
		return d.varbin(n, t, length, m)
	case "vortex.dict":
		return d.dictionary(n, t, length, m)
	case "vortex.masked":
		ct := t
		ct.Nullable = false
		v, e = d.child(n, 0, ct, length)
		if e != nil {
			return nil, e
		}
		v.typ = t
		v.valid, e = d.validity(n, 1, length)
		return v, e
	case "vortex.extension", "vortex.ext":
		if t.Kind != "extension" || t.Storage == nil {
			return nil, fmt.Errorf("invalid extension type")
		}
		if _, e = t.ArrowType(); e != nil {
			return nil, e
		}
		if e = countChildren(n, 1); e != nil {
			return nil, e
		}
		v, e = d.child(n, 0, *t.Storage, length)
		if e != nil {
			return nil, e
		}
		v.typ = t
		return v, nil
	case "vortex.decimal":
		return d.decimal(n, t, length, m)
	case "vortex.decimal_byte_parts":
		if t.Kind != "decimal" || m.num(2) != 0 {
			return nil, fmt.Errorf("unsupported decimal byte parts")
		}
		ct, e := primitiveType(m.num(1), t.Nullable)
		if e != nil {
			return nil, e
		}
		if !ct.signed() {
			return nil, fmt.Errorf("decimal parts must be signed")
		}
		c, e := d.child(n, 0, ct, length)
		if e != nil {
			return nil, e
		}
		v, e = d.empty(t, length)
		if e != nil {
			return nil, e
		}
		v.valid = c.valid
		w := len(v.wide) / max(length, 1)
		for i, x := range c.bits {
			sign := asSigned(x, ct.width())
			if sign < 0 {
				for j := 0; j < w; j++ {
					v.wide[i*w+j] = 255
				}
			}
			writeWord(v.wide[i*w:], min(w, 8), uint64(sign))
		}
		return v, nil
	case "fastlanes.bitpacked":
		return d.bitpacked(n, t, length, m)
	case "fastlanes.for", "vortex.zigzag":
		if !t.integer() {
			return nil, fmt.Errorf("integer encoding requires integer dtype")
		}
		if e = countChildren(n, 1); e != nil {
			return nil, e
		}
		ct := t
		if n.Encoding == "vortex.zigzag" {
			if !t.signed() {
				return nil, fmt.Errorf("zigzag requires signed dtype")
			}
			ct = unsigned(t)
		}
		v, e = d.child(n, 0, ct, length)
		if e != nil {
			return nil, e
		}
		v.typ = t
		var base uint64
		if n.Encoding == "fastlanes.for" {
			s, e := d.constant(t, 1, n.Metadata)
			if e != nil {
				return nil, e
			}
			if !s.isValid(0) {
				return nil, fmt.Errorf("null FOR reference")
			}
			base = s.bits[0]
		}
		mask := bitMask(t.width() * 8)
		for i, x := range v.bits {
			if n.Encoding == "vortex.zigzag" {
				v.bits[i] = ((x >> 1) ^ uint64(-int64(x&1))) & mask
			} else {
				v.bits[i] = (x + base) & mask
			}
		}
		return v, nil
	case "vortex.sequence":
		return d.sequence(n, t, length, m)
	case "vortex.runend":
		return d.runend(n, t, length, m)
	case "fastlanes.rle":
		return d.rle(n, t, length, m)
	case "vortex.sparse":
		b, e := oneBuffer(n)
		if e != nil {
			return nil, e
		}
		v, e = d.constant(t, length, b)
		if e != nil {
			return nil, e
		}
		_, e = d.patches(n, 0, m.bytes(1), v, t)
		return v, e
	case "vortex.alp":
		return d.alp(n, t, length, m)
	case "vortex.alprd":
		return d.alprd(n, t, length, m)
	case "vortex.fsst":
		return d.fsst(n, t, length, m)
	case "vortex.onpair":
		return d.onpair(n, t, length, m)
	case "vortex.zstd":
		return d.zstd(n, t, length, m)
	case "vortex.datetimeparts":
		return d.datetimeParts(n, t, length, m)
	case "fastlanes.delta":
		return d.delta(n, t, length, m)
	case "vortex.pco":
		return d.pco(n, t, length, m)
	default:
		return nil, fmt.Errorf("unsupported encoding")
	}
}

func (d *decoder) constant(t Type, length int, b []byte) (*values, error) {
	if t.Kind == "extension" {
		if t.Storage == nil {
			return nil, fmt.Errorf("extension missing storage")
		}
		v, e := d.constant(*t.Storage, length, b)
		if e == nil {
			v.typ = t
		}
		return v, e
	}
	m, e := parseProto(b)
	if e != nil {
		return nil, e
	}
	v, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if len(m) == 0 || m.has(1) {
		if !t.Nullable && t.Kind != "null" {
			return nil, fmt.Errorf("null scalar for nonnullable type")
		}
		v.valid = make([]bool, length)
		return v, nil
	}
	if t.Kind == "struct" {
		list, e := parseProto(m.bytes(9))
		if e != nil {
			return nil, e
		}
		if len(list) != len(t.Fields) {
			return nil, fmt.Errorf("invalid struct scalar")
		}
		for i, f := range t.Fields {
			v.fields[i], e = d.constant(f.Type, length, list[i].data)
			if e != nil {
				return nil, e
			}
		}
		return v, nil
	}
	if t.Kind == "decimal" {
		f, ok := m.get(8)
		if !ok {
			return nil, fmt.Errorf("decimal scalar lacks bytes")
		}
		w := 16
		if t.Precision > 38 {
			w = 32
		}
		if len(f.data) > w || len(f.data) == 0 {
			return nil, fmt.Errorf("invalid decimal scalar size")
		}
		for i := 0; i < length; i++ {
			if f.data[len(f.data)-1]&128 != 0 {
				for j := 0; j < w; j++ {
					v.wide[i*w+j] = 255
				}
			}
			copy(v.wide[i*w:(i+1)*w], f.data)
		}
		return v, nil
	}
	if t.Kind == "utf8" || t.Kind == "binary" {
		tag := 8
		if t.Kind == "utf8" {
			tag = 7
		}
		f, ok := m.get(tag)
		if !ok {
			return nil, fmt.Errorf("invalid byte scalar")
		}
		if t.Kind == "utf8" && !utf8.Valid(f.data) {
			return nil, fmt.Errorf("invalid UTF8 scalar")
		}
		for i := range v.bytes {
			v.bytes[i] = f.data
		}
		return v, nil
	}
	var x uint64
	switch {
	case t.Kind == "bool":
		if !m.has(2) {
			return nil, fmt.Errorf("invalid boolean scalar")
		}
		x = m.num(2)
		if x > 1 {
			return nil, fmt.Errorf("invalid boolean scalar")
		}
	case t.signed():
		if !m.has(3) {
			return nil, fmt.Errorf("invalid signed scalar")
		}
		u := m.num(3)
		x = (u >> 1) ^ uint64(-int64(u&1))
		if asSigned(x, t.width()) != int64(x) {
			return nil, fmt.Errorf("signed scalar overflow")
		}
	case t.integer():
		if !m.has(4) {
			return nil, fmt.Errorf("invalid unsigned scalar")
		}
		x = m.num(4)
		if x > bitMask(t.width()*8) {
			return nil, fmt.Errorf("unsigned scalar overflow")
		}
	case t.Kind == "f16":
		if !m.has(10) {
			return nil, fmt.Errorf("invalid f16 scalar")
		}
		x = m.num(10)
	case t.Kind == "f32":
		if !m.has(5) {
			return nil, fmt.Errorf("invalid f32 scalar")
		}
		x = m.num(5)
	case t.Kind == "f64":
		if !m.has(6) {
			return nil, fmt.Errorf("invalid f64 scalar")
		}
		x = m.num(6)
	default:
		return nil, fmt.Errorf("unsupported scalar type %q", t.Kind)
	}
	for i := range v.bits {
		v.bits[i] = x & bitMask(t.width()*8)
	}
	return v, nil
}
