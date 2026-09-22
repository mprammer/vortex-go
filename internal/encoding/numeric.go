// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
// FastLanes unpacking and ALP reconstruction derive from the Apache-2.0
// Vortex/fastlanes/alp Rust implementations; see PROVENANCE.md.
package encoding

import (
	"fmt"
	"math"
	"math/big"
)

func (d *decoder) dictionary(n *Node, t Type, length int, m proto) (*values, error) {
	if e := countChildren(n, 2); e != nil {
		return nil, e
	}
	nullable := t.Nullable
	if m.has(3) {
		nullable = m.num(3) != 0
	}
	ct, e := primitiveType(m.num(2), nullable)
	if e != nil {
		return nil, e
	}
	if !ct.integer() {
		return nil, fmt.Errorf("dictionary codes must be integers")
	}
	count, e := boundedInt(m.num(1))
	if e != nil {
		return nil, e
	}
	codes, e := d.child(n, 0, ct, length)
	if e != nil {
		return nil, e
	}
	dict, e := d.child(n, 1, t, count)
	if e != nil {
		return nil, e
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if err := d.copyValidityShape(out, dict); err != nil {
		return nil, err
	}
	if codes.valid != nil || dict.valid != nil {
		if e = d.mask(out); e != nil {
			return nil, e
		}
	}
	for i, c := range codes.bits {
		if !codes.isValid(i) {
			out.valid[i] = false
			continue
		}
		if c >= uint64(count) {
			return nil, fmt.Errorf("dictionary code %d out of range %d", c, count)
		}
		out.at(i, dict, int(c))
	}
	return out, nil
}

// Patches store absolute row indices. Applying them directly avoids depending on
// the optional chunk index acceleration structure, including after a slice.
func (d *decoder) patches(n *Node, start int, raw []byte, out *values, t Type) (int, error) {
	return d.applyPatches(n, start, raw, out, t, false)
}

// ALPRD exceptions correct numeric parts without replacing the source validity.
func (d *decoder) numericPatches(n *Node, start int, raw []byte, out *values, t Type) (int, error) {
	return d.applyPatches(n, start, raw, out, t, true)
}

func (d *decoder) applyPatches(n *Node, start int, raw []byte, out *values, t Type, numericOnly bool) (int, error) {
	if raw == nil {
		return start, nil
	}
	m, e := parseProto(raw)
	if e != nil {
		return 0, e
	}
	count, e := boundedInt(m.num(1))
	if e != nil {
		return 0, e
	}
	it, e := primitiveType(m.num(3), false)
	if e != nil {
		return 0, e
	}
	if !it.integer() || it.signed() {
		return 0, fmt.Errorf("patch indices must be unsigned integers")
	}
	idx, e := d.child(n, start, it, count)
	if e != nil {
		return 0, e
	}
	vals, e := d.child(n, start+1, t, count)
	if e != nil {
		return 0, e
	}
	next := start + 2
	if m.has(5) {
		ct, e := primitiveType(m.num(5), false)
		if e != nil {
			return 0, e
		}
		cn, e := boundedInt(m.num(4))
		if e != nil {
			return 0, e
		}
		c, e := d.child(n, next, ct, cn)
		if e != nil {
			return 0, e
		}
		if !ct.integer() || ct.signed() {
			return 0, fmt.Errorf("invalid patch chunk offsets dtype")
		}
		for i, x := range c.bits {
			if i > 0 && x < c.bits[i-1] {
				return 0, fmt.Errorf("patch chunk offsets not sorted")
			}
		}
		next++
	}
	if !numericOnly {
		if e = d.copyValidityShape(out, vals); e != nil {
			return 0, e
		}
	}
	off := m.num(2)
	var prev uint64
	for i, x := range idx.bits {
		if x < off || x-off >= uint64(out.n) {
			return 0, fmt.Errorf("patch index %d outside offset %d length %d", x, off, out.n)
		}
		if i > 0 && x <= prev {
			return 0, fmt.Errorf("patch indices not strictly sorted")
		}
		prev = x
		if numericOnly {
			out.bits[int(x-off)] = vals.bits[i]
		} else {
			out.at(int(x-off), vals, i)
		}
	}
	return next, nil
}
func (d *decoder) bitpacked(n *Node, t Type, length int, m proto) (*values, error) {
	if !t.integer() {
		return nil, fmt.Errorf("bitpacking requires integer type")
	}
	bw, off := m.num(1), m.num(2)
	w := t.width()
	bits := w * 8
	if bw > uint64(bits) || off >= 1024 {
		return nil, fmt.Errorf("invalid bit width or slice offset")
	}
	b, e := oneBuffer(n)
	if e != nil {
		return nil, e
	}
	chunks := (uint64(length) + off + 1023) / 1024
	need := chunks * bw * 128
	if uint64(len(b)) != need {
		return nil, fmt.Errorf("packed buffer length %d; expected %d", len(b), need)
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	lanes := 1024 / bits
	order := [8]int{0, 4, 2, 6, 1, 5, 3, 7}
	mask := bitMask(int(bw))
	if bw != 0 {
		for i := range out.bits {
			at := int(off) + i
			chunk, position := at/1024, at%1024
			lane := position % lanes
			stripe := position / 128
			permuted := (position - stripe*128 - lane) / 16
			row := order[permuted]*8 + stripe
			startBit := row * int(bw)
			word, shift := startBit/bits, startBit%bits
			base := chunk * int(bw) * 128
			lo := readWord(b[base+(word*lanes+lane)*w:], w) >> shift
			remain := bits - shift
			if remain < int(bw) {
				lo |= readWord(b[base+((word+1)*lanes+lane)*w:], w) << remain
			}
			out.bits[i] = lo & mask
		}
	}
	next, e := d.patches(n, 0, m.bytes(3), out, t)
	if e != nil {
		return nil, e
	}
	valid, e := d.validity(n, next, length)
	if e != nil {
		return nil, e
	}
	out.valid = valid
	return out, nil
}
func (d *decoder) sequence(n *Node, t Type, length int, m proto) (*values, error) {
	if !t.integer() {
		return nil, fmt.Errorf("sequence requires integer type")
	}
	if e := countChildren(n, 0); e != nil {
		return nil, e
	}
	base, e := d.constant(t, 1, m.bytes(1))
	if e != nil {
		return nil, e
	}
	step, stepSigned, e := sequenceStep(m.bytes(2))
	if e != nil {
		return nil, e
	}
	if !base.isValid(0) {
		return nil, fmt.Errorf("null sequence scalar")
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	a, b := new(big.Int), new(big.Int)
	if t.signed() {
		a.SetInt64(asSigned(base.bits[0], t.width()))
	} else {
		a.SetUint64(base.bits[0])
	}
	if stepSigned {
		b.SetInt64(int64(step))
	} else {
		b.SetUint64(step)
	}
	last := new(big.Int).Mul(b, big.NewInt(int64(max(length-1, 0))))
	last.Add(last, a)
	nbits := t.width() * 8
	if t.signed() {
		lo := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), uint(nbits-1)))
		hi := new(big.Int).Sub(new(big.Int).Neg(lo), big.NewInt(1))
		if last.Cmp(lo) < 0 || last.Cmp(hi) > 0 {
			return nil, fmt.Errorf("sequence overflow")
		}
	} else if last.Sign() < 0 || last.BitLen() > nbits {
		return nil, fmt.Errorf("sequence overflow")
	}
	mask := bitMask(nbits)
	for i := range out.bits {
		out.bits[i] = (base.bits[0] + uint64(i)*step) & mask
	}
	return out, nil
}

// sequenceStep decodes a sequence's step scalar, reporting its raw 64-bit
// pattern and whether the wire form was signed.
//
// The step's serialized form preserves its signedness but not its width:
// Vortex normalizes the step to i64 when it fits and to u64 otherwise,
// independently of the sequence's own type. A sequence over an unsigned type
// can therefore carry a signed step, so read whichever kind is present rather
// than the one the array type implies. The step may also fall outside the
// array's type; the caller's range check on the last element is what bounds
// the values the sequence actually produces.
func sequenceStep(b []byte) (uint64, bool, error) {
	m, e := parseProto(b)
	if e != nil {
		return 0, false, e
	}
	switch {
	case len(m) == 0 || m.has(1):
		return 0, false, fmt.Errorf("null sequence scalar")
	case m.has(3):
		u := m.num(3)
		return (u >> 1) ^ uint64(-int64(u&1)), true, nil
	case m.has(4):
		return m.num(4), false, nil
	default:
		return 0, false, fmt.Errorf("invalid sequence step scalar")
	}
}
func (d *decoder) runend(n *Node, t Type, length int, m proto) (*values, error) {
	if e := countChildren(n, 2); e != nil {
		return nil, e
	}
	ct, e := primitiveType(m.num(1), false)
	if e != nil {
		return nil, e
	}
	if !ct.integer() {
		return nil, fmt.Errorf("run ends require integer type")
	}
	count, e := boundedInt(m.num(2))
	if e != nil {
		return nil, e
	}
	ends, e := d.child(n, 0, ct, count)
	if e != nil {
		return nil, e
	}
	vals, e := d.child(n, 1, t, count)
	if e != nil {
		return nil, e
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if e = d.copyValidityShape(out, vals); e != nil {
		return nil, e
	}
	off := m.num(3)
	if off > math.MaxUint64-uint64(length) {
		return nil, fmt.Errorf("run end offset overflow")
	}
	for i, x := range ends.bits {
		if x == 0 || (i > 0 && x <= ends.bits[i-1]) {
			return nil, fmt.Errorf("run ends not strictly increasing")
		}
	}
	run := 0
	for i := 0; i < length; i++ {
		for run < count && ends.bits[run] <= off+uint64(i) {
			run++
		}
		if run == count {
			return nil, fmt.Errorf("run ends do not cover array")
		}
		out.at(i, vals, run)
	}
	return out, nil
}
func (d *decoder) rle(n *Node, t Type, length int, m proto) (*values, error) {
	if e := countChildren(n, 3); e != nil {
		return nil, e
	}
	if t.width() == 0 {
		return nil, fmt.Errorf("RLE requires primitive type")
	}
	vl, e := boundedInt(m.num(1))
	if e != nil {
		return nil, e
	}
	il, e := boundedInt(m.num(2))
	if e != nil {
		return nil, e
	}
	ol, e := boundedInt(m.num(4))
	if e != nil {
		return nil, e
	}
	it, e := primitiveType(m.num(3), t.Nullable)
	if e != nil {
		return nil, e
	}
	ot, e := primitiveType(m.num(5), false)
	if e != nil {
		return nil, e
	}
	if (it.Kind != "u8" && it.Kind != "u16") || !ot.integer() || ot.signed() {
		return nil, fmt.Errorf("RLE indices must be u8/u16 and offsets must be unsigned integers")
	}
	vt := t
	vt.Nullable = false
	vals, e := d.child(n, 0, vt, vl)
	if e != nil {
		return nil, e
	}
	idx, e := d.child(n, 1, it, il)
	if e != nil {
		return nil, e
	}
	offsets, e := d.child(n, 2, ot, ol)
	if e != nil {
		return nil, e
	}
	off := m.num(6)
	if off >= 1024 || off+uint64(length) > uint64(il) || il%1024 != 0 || ol != il/1024 {
		return nil, fmt.Errorf("invalid RLE shape")
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if idx.valid != nil {
		if e = d.mask(out); e != nil {
			return nil, e
		}
	}
	for i := 0; i < length; i++ {
		j := i + int(off)
		if !idx.isValid(j) {
			out.valid[i] = false
			continue
		}
		chunk := j / 1024
		start := offsets.bits[chunk]
		end := uint64(vl)
		if chunk+1 < ol {
			end = offsets.bits[chunk+1]
		}
		if start >= end || end > uint64(vl) {
			return nil, fmt.Errorf("invalid RLE value offsets")
		}
		code := idx.bits[j]
		if end-start == 1 {
			code = 0
		}
		if code >= end-start {
			return nil, fmt.Errorf("RLE index out of chunk range")
		}
		out.bits[i] = vals.bits[start+code]
	}
	return out, nil
}

var alpPowers = [...]float64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}
var alpInverse = [...]float64{1, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18}

func (d *decoder) alp(n *Node, t Type, length int, m proto) (*values, error) {
	limit := uint64(18)
	ct := Type{Kind: "i64", Nullable: t.Nullable}
	if t.Kind == "f32" {
		limit = 10
		ct.Kind = "i32"
	} else if t.Kind != "f64" {
		return nil, fmt.Errorf("ALP requires f32/f64")
	}
	exp, factor := m.num(1), m.num(2)
	if exp > limit || factor > limit {
		return nil, fmt.Errorf("ALP exponent out of range")
	}
	out, e := d.child(n, 0, ct, length)
	if e != nil {
		return nil, e
	}
	out.typ = t
	for i, x := range out.bits {
		if t.Kind == "f32" {
			v := float32(int32(x))
			v = float32(v * float32(alpPowers[factor]))
			v = float32(v * float32(alpInverse[exp]))
			out.bits[i] = uint64(math.Float32bits(v))
		} else {
			v := float64(int64(x))
			v = float64(v * alpPowers[factor])
			v = float64(v * alpInverse[exp])
			out.bits[i] = math.Float64bits(v)
		}
	}
	next, e := d.patches(n, 1, m.bytes(3), out, t)
	if e != nil {
		return nil, e
	}
	if len(n.Children) != next {
		return nil, fmt.Errorf("unexpected ALP children")
	}
	return out, nil
}
func (d *decoder) alprd(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "f32" && t.Kind != "f64" {
		return nil, fmt.Errorf("ALPRD requires f32/f64")
	}
	bw := m.num(1)
	if bw >= uint64(t.width()*8) || bw+16 < uint64(t.width()*8) {
		return nil, fmt.Errorf("invalid ALPRD right width")
	}
	dict, e := m.repeated(3)
	if e != nil {
		return nil, e
	}
	if m.num(2) > uint64(len(dict)) || m.num(2) > 65536 {
		return nil, fmt.Errorf("invalid ALPRD dictionary length")
	}
	dict = dict[:int(m.num(2))]
	for _, x := range dict {
		if x > 65535 {
			return nil, fmt.Errorf("ALPRD dictionary value exceeds u16")
		}
	}
	lt, e := primitiveType(m.num(4), t.Nullable)
	if e != nil {
		return nil, e
	}
	if !lt.integer() || lt.signed() {
		return nil, fmt.Errorf("invalid ALPRD left type")
	}
	left, e := d.child(n, 0, lt, length)
	if e != nil {
		return nil, e
	}
	rt := Type{Kind: "u64"}
	if t.Kind == "f32" {
		rt.Kind = "u32"
	}
	out, e := d.child(n, 1, rt, length)
	if e != nil {
		return nil, e
	}
	out.typ = t
	out.valid = left.valid
	for i, c := range left.bits {
		if !out.isValid(i) {
			continue
		}
		if c >= uint64(len(dict)) {
			return nil, fmt.Errorf("ALPRD dictionary code out of range")
		}
		left.bits[i] = dict[c]
	}
	pt := lt
	pt.Nullable = false
	next, e := d.numericPatches(n, 2, m.bytes(5), left, pt)
	if e != nil {
		return nil, e
	}
	if len(n.Children) != next {
		return nil, fmt.Errorf("unexpected ALPRD children")
	}
	mask := bitMask(int(bw))
	for i, x := range left.bits {
		if !out.isValid(i) {
			continue
		}
		if x > 65535 || out.bits[i]&^mask != 0 {
			return nil, fmt.Errorf("ALPRD part exceeds width")
		}
		out.bits[i] |= x << bw
	}
	return out, nil
}
