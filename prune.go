// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/mprammer/vortex-go/internal/encoding"
	"google.golang.org/protobuf/encoding/protowire"
)

// Optional statistics never make a readable data layout unreadable. The schema is
// reconstructed only when every aggregate is understood; otherwise pruning stops.
type zoneRef struct {
	start, end, zoneLen                                       int64
	node                                                      *layout
	dtype                                                     encoding.Type
	minField, maxField, nullField, minTruncated, maxTruncated int
	loaded                                                    bool
	values                                                    *array.Struct
}

func (z *zoneRef) release() {
	if z.values != nil {
		z.values.Release()
		z.values = nil
	}
}
func newZoneRef(l *layout, t encoding.Type, start int64) *zoneRef {
	z := &zoneRef{start: start, end: start + l.rows, node: l.children[1], minField: -1, maxField: -1, nullField: -1, minTruncated: -1, maxTruncated: -1}
	zoneLen, dt, err := zoneSchema(l, t)
	if err != nil || zoneLen == 0 {
		return z
	}
	expected := l.rows / int64(zoneLen)
	if l.rows%int64(zoneLen) != 0 {
		expected++
	}
	if z.node.rows != expected {
		return z
	}
	z.zoneLen = int64(zoneLen)
	z.dtype = dt
	for i, f := range dt.Fields {
		switch {
		case f.Name == "min" || strings.HasPrefix(f.Name, "vortex.min("):
			z.minField = i
		case f.Name == "max" || strings.HasPrefix(f.Name, "vortex.max("):
			z.maxField = i
		case f.Name == "null_count" || f.Name == "vortex.null_count()":
			z.nullField = i
		case f.Name == "min_is_truncated":
			z.minTruncated = i
		case f.Name == "max_is_truncated":
			z.maxTruncated = i
		}
	}
	return z
}
func (z *zoneRef) evaluate(ctx context.Context, f *File, pos int64, p Predicate) (end int64, skip bool, err error) {
	end = z.end
	if z.zoneLen == 0 {
		return end, false, nil
	}
	end = pos + min(z.zoneLen-(pos-z.start)%z.zoneLen, z.end-pos)
	if !z.loaded {
		z.loaded = true
		a, e := f.decodeLayout(ctx, z.node, z.dtype)
		if e != nil {
			if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
				return end, false, e
			}
			z.zoneLen = 0
			return z.end, false, nil
		}
		st, ok := a.(*array.Struct)
		if !ok || int64(a.Len()) != z.node.rows {
			a.Release()
			z.zoneLen = 0
			return z.end, false, nil
		}
		z.values = st
	}
	if z.values == nil {
		return end, false, nil
	}
	idx := int((pos - z.start) / z.zoneLen)
	if z.values.IsNull(idx) {
		return end, false, nil
	}
	zoneRows := min(z.zoneLen, z.end-(z.start+int64(idx)*z.zoneLen))
	knownNonNull := false
	if z.nullField >= 0 {
		a, ok := z.values.Field(z.nullField).(*array.Uint64)
		if ok && !a.IsNull(idx) {
			if a.Value(idx) > uint64(zoneRows) {
				return end, false, nil
			}
			knownNonNull = a.Value(idx) == 0
			if a.Value(idx) == uint64(zoneRows) && p.Op != OpNEQ {
				return end, true, nil
			}
		}
	}
	if z.minField < 0 || z.maxField < 0 {
		return end, false, nil
	}
	for _, index := range []int{z.minTruncated, z.maxTruncated} {
		if index >= 0 {
			a, ok := z.values.Field(index).(*array.Boolean)
			if !ok || a.IsNull(idx) || a.Value(idx) {
				return end, false, nil
			}
		}
	}
	lo, okLo := signedValue(z.values.Field(z.minField), idx)
	hi, okHi := signedValue(z.values.Field(z.maxField), idx)
	if !okLo || !okHi || lo > hi {
		return end, false, nil
	}
	switch p.Op {
	case OpEQ:
		skip = p.Value < lo || p.Value > hi
	case OpNEQ:
		skip = knownNonNull && lo == hi && lo == p.Value
	case OpGT:
		skip = hi <= p.Value
	case OpGTE:
		skip = hi < p.Value
	case OpLT:
		skip = lo >= p.Value
	case OpLTE:
		skip = lo > p.Value
	}
	return end, skip, nil
}
func signedValue(a arrow.Array, index int) (int64, bool) {
	if index < 0 || index >= a.Len() || a.IsNull(index) {
		return 0, false
	}
	switch v := a.(type) {
	case *array.Int8:
		return int64(v.Value(index)), true
	case *array.Int16:
		return int64(v.Value(index)), true
	case *array.Int32:
		return int64(v.Value(index)), true
	case *array.Int64:
		return v.Value(index), true
	}
	return 0, false
}
func zoneSchema(l *layout, t encoding.Type) (uint32, encoding.Type, error) {
	dt := encoding.Type{Kind: "struct"}
	if t.Kind != "i8" && t.Kind != "i16" && t.Kind != "i32" && t.Kind != "i64" {
		return 0, dt, fmt.Errorf("non-integer zone map")
	}
	column := t
	column.Nullable = true
	add := func(name string, typ encoding.Type) {
		dt.Fields = append(dt.Fields, encoding.Field{Name: name, Type: typ})
	}
	count := encoding.Type{Kind: "u64", Nullable: true}
	switch l.id {
	case "vortex.stats":
		if len(l.metadata) < 4 {
			return 0, dt, fmt.Errorf("truncated legacy zone metadata")
		}
		bits := l.metadata[4:]
		if len(bits) > 2 {
			return 0, dt, fmt.Errorf("unknown legacy zone statistics")
		}
		for i := 0; i < len(bits)*8; i++ {
			if bits[i/8]&(1<<uint(i%8)) == 0 {
				continue
			}
			switch i {
			case 0:
				add("is_constant", encoding.Type{Kind: "bool", Nullable: true})
			case 1:
				add("is_sorted", encoding.Type{Kind: "bool", Nullable: true})
			case 2:
				add("is_strict_sorted", encoding.Type{Kind: "bool", Nullable: true})
			case 3:
				add("max", column)
				add("max_is_truncated", encoding.Type{Kind: "bool"})
			case 4:
				add("min", column)
				add("min_is_truncated", encoding.Type{Kind: "bool"})
			case 5:
				add("sum", encoding.Type{Kind: "i64", Nullable: true})
			case 6:
				add("null_count", count)
			case 7:
				add("uncompressed_size_in_bytes", count)
			case 8: // NaN count is absent for integers.
			default:
				return 0, dt, fmt.Errorf("unknown legacy statistic")
			}
		}
		return binary.LittleEndian.Uint32(l.metadata), dt, nil
	case "vortex.zoned":
		if len(l.metadata) < 2 || l.metadata[0] != 1 {
			return 0, dt, fmt.Errorf("unknown zoned metadata version")
		}
		m, err := protoFields(l.metadata[1:])
		if err != nil {
			return 0, dt, err
		}
		for field := range m {
			if field != 1 && field != 2 {
				return 0, dt, fmt.Errorf("unknown zoned metadata field")
			}
		}
		if len(m[1]) != 1 || m[1][0].kind != protowire.VarintType || m[1][0].number > 1<<32-1 {
			return 0, dt, fmt.Errorf("invalid zone length")
		}
		seen := map[string]bool{}
		for _, spec := range m[2] {
			if spec.kind != protowire.BytesType {
				return 0, dt, fmt.Errorf("invalid aggregate")
			}
			fields, e := protoFields(spec.bytes)
			if e != nil {
				return 0, dt, e
			}
			for field := range fields {
				if field != 1 && field != 2 {
					return 0, dt, fmt.Errorf("unknown aggregate metadata field")
				}
			}
			if len(fields[1]) != 1 || fields[1][0].kind != protowire.BytesType {
				return 0, dt, fmt.Errorf("missing aggregate id")
			}
			id := string(fields[1][0].bytes)
			opts := []byte(nil)
			if len(fields[2]) > 0 {
				v := fields[2][len(fields[2])-1]
				if v.kind != protowire.BytesType {
					return 0, dt, fmt.Errorf("invalid aggregate options")
				}
				opts = v.bytes
			}
			name := id + "()"
			switch id {
			case "vortex.min", "vortex.max", "vortex.sum":
				options, e := protoFields(opts)
				if e != nil {
					return 0, dt, e
				}
				skip := false
				for field, values := range options {
					if field != 1 || len(values) != 1 || values[0].kind != protowire.VarintType || values[0].number > 1 {
						return 0, dt, fmt.Errorf("unknown aggregate options")
					}
					skip = values[0].number != 0
				}
				if !skip {
					name = id + "(skip_nans=false)"
				}
				typ := column
				if id == "vortex.sum" {
					typ = encoding.Type{Kind: "i64", Nullable: true}
				}
				add(name, typ)
			case "vortex.null_count", "vortex.count", "vortex.uncompressed_size_in_bytes":
				if len(opts) != 0 {
					return 0, dt, fmt.Errorf("unknown empty aggregate options")
				}
				add(name, count)
			case "vortex.nan_count":
				if len(opts) != 0 {
					return 0, dt, fmt.Errorf("unknown nan_count options")
				}
				continue
			default:
				return 0, dt, fmt.Errorf("unknown aggregate %s", id)
			}
			if seen[name] {
				return 0, dt, fmt.Errorf("duplicate zone aggregate")
			}
			seen[name] = true
		}
		return uint32(m[1][0].number), dt, nil
	}
	return 0, dt, fmt.Errorf("unknown zone layout")
}
