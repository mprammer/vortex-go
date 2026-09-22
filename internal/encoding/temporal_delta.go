// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
// FastLanes delta/transpose derives from the Apache-2.0 upstream algorithms.
package encoding

import (
	"fmt"
	"math/big"
)

func (d *decoder) datetimeParts(n *Node, t Type, length int, m proto) (*values, error) {
	if t.Kind != "extension" || t.Extension != "vortex.timestamp" || len(t.Metadata) == 0 || t.Metadata[0] > 3 {
		return nil, fmt.Errorf("datetime parts requires timestamp")
	}
	if _, e := t.ArrowType(); e != nil {
		return nil, e
	}
	if e := countChildren(n, 3); e != nil {
		return nil, e
	}
	parts := make([]*values, 3)
	for i := range parts {
		pt, e := primitiveType(m.num(i+1), i == 0 && t.Nullable)
		if e != nil {
			return nil, e
		}
		if !pt.integer() {
			return nil, fmt.Errorf("datetime part must be integer")
		}
		parts[i], e = d.child(n, i, pt, length)
		if e != nil {
			return nil, e
		}
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	out.valid = parts[0].valid
	factor := []int64{1000000000, 1000000, 1000, 1}[t.Metadata[0]]
	dayScale := big.NewInt(86400 * factor)
	secondScale := big.NewInt(factor)
	value := new(big.Int)
	part := new(big.Int)
	for i := 0; i < length; i++ {
		if !out.isValid(i) {
			continue
		}
		set := func(v *values) {
			if v.typ.signed() {
				part.SetInt64(asSigned(v.bits[i], v.typ.width()))
			} else {
				part.SetUint64(v.bits[i])
			}
		}
		set(parts[0])
		value.Mul(part, dayScale)
		set(parts[1])
		part.Mul(part, secondScale)
		value.Add(value, part)
		set(parts[2])
		value.Add(value, part)
		if !value.IsInt64() {
			return nil, fmt.Errorf("datetime parts overflow")
		}
		out.bits[i] = uint64(value.Int64())
	}
	return out, nil
}

func (d *decoder) delta(n *Node, t Type, length int, m proto) (*values, error) {
	if !t.integer() {
		return nil, fmt.Errorf("delta requires integer dtype")
	}
	if e := countChildren(n, 2); e != nil {
		return nil, e
	}
	count, e := boundedInt(m.num(1))
	if e != nil {
		return nil, e
	}
	off := m.num(2)
	if off >= 1024 || off+uint64(length) > uint64(count) {
		return nil, fmt.Errorf("invalid delta slice")
	}
	lanes := 1024 / (t.width() * 8)
	chunks := count / 1024
	basesCount := chunks * lanes
	if count%1024 != 0 {
		basesCount++
	}
	bases, e := d.child(n, 0, t, basesCount)
	if e != nil {
		return nil, e
	}
	deltas, e := d.child(n, 1, t, count)
	if e != nil {
		return nil, e
	}
	out, e := d.empty(t, length)
	if e != nil {
		return nil, e
	}
	if deltas.valid != nil {
		if e = d.mask(out); e != nil {
			return nil, e
		}
	}
	mask := bitMask(t.width() * 8)
	order := [8]int{0, 4, 2, 6, 1, 5, 3, 7}
	var block [1024]uint64
	for chunk := 0; chunk < chunks; chunk++ {
		for lane := 0; lane < lanes; lane++ {
			prev := bases.bits[chunk*lanes+lane]
			for row := 0; row < t.width()*8; row++ {
				idx := order[row/8]*16 + (row%8)*128 + lane
				prev = (prev + deltas.bits[chunk*1024+idx]) & mask
				target := (idx%16)*64 + order[(idx/16)%8]*8 + idx/128
				block[target] = prev
			}
		}
		start := max(chunk*1024, int(off))
		end := min((chunk+1)*1024, int(off)+length)
		for i := start; i < end; i++ {
			out.bits[i-int(off)] = block[i-chunk*1024]
			if out.valid != nil {
				// FastLanes 0.6.1 scatters logical validity bits through the
				// transpose; numeric values use the inverse permutation.
				logical := i - chunk*1024
				physical := (logical%16)*64 + order[(logical/16)%8]*8 + logical/128
				out.valid[i-int(off)] = deltas.valid[chunk*1024+physical]
			}
		}
	}
	if count%1024 != 0 {
		prev := bases.bits[basesCount-1]
		for i := chunks * 1024; i < count; i++ {
			prev = (prev + deltas.bits[i]) & mask
			if i >= int(off) && i < int(off)+length {
				out.bits[i-int(off)] = prev
				if out.valid != nil {
					out.valid[i-int(off)] = deltas.valid[i]
				}
			}
		}
	}
	return out, nil
}
