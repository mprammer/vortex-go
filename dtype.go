// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"fmt"
	"github.com/google/flatbuffers/go"
	"github.com/mprammer/vortex-go/internal/encoding"
	"github.com/mprammer/vortex-go/internal/wire/fb"
)

func parseType(v *fb.DType, depth int, budget *int) (encoding.Type, error) {
	var out encoding.Type
	if v == nil {
		return out, fmt.Errorf("%w: missing dtype", ErrInvalid)
	}
	if err := nodeBudget(depth, budget); err != nil {
		return out, err
	}
	var tab flatbuffers.Table
	if !v.Type(&tab) {
		return out, fmt.Errorf("%w: missing dtype union", ErrInvalid)
	}
	switch v.TypeType() {
	case fb.TypeNull:
		out.Kind = "null"
		out.Nullable = true
	case fb.TypeBool:
		var t fb.Bool
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "bool"
		out.Nullable = t.Nullable()
	case fb.TypePrimitive:
		var t fb.Primitive
		t.Init(tab.Bytes, tab.Pos)
		kinds := []encoding.Kind{"u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f16", "f32", "f64"}
		if int(t.Ptype()) >= len(kinds) {
			return out, fmt.Errorf("%w: primitive dtype", ErrInvalid)
		}
		out.Kind = kinds[t.Ptype()]
		out.Nullable = t.Nullable()
	case fb.TypeUtf8:
		var t fb.Utf8
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "utf8"
		out.Nullable = t.Nullable()
	case fb.TypeBinary:
		var t fb.Binary
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "binary"
		out.Nullable = t.Nullable()
	case fb.TypeDecimal:
		var t fb.Decimal
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "decimal"
		out.Nullable = t.Nullable()
		out.Precision = int32(t.Precision())
		out.Scale = int32(t.Scale())
	case fb.TypeStruct_:
		var t fb.Struct_
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "struct"
		out.Nullable = t.Nullable()
		if t.NamesLength() != t.DtypesLength() {
			return out, fmt.Errorf("%w: struct fields", ErrInvalid)
		}
		if err := checkCount(t.NamesLength(), tab.Bytes, 4); err != nil {
			return out, err
		}
		for i := 0; i < t.NamesLength(); i++ {
			var c fb.DType
			t.Dtypes(&c, i)
			dtype, err := parseType(&c, depth+1, budget)
			if err != nil {
				return out, err
			}
			name, err := copyText(t.Names(i), budget)
			if err != nil {
				return out, err
			}
			out.Fields = append(out.Fields, encoding.Field{Name: name, Type: dtype})
		}
	case fb.TypeExtension:
		var t fb.Extension
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "extension"
		var err error
		out.Extension, err = copyText(t.Id(), budget)
		if err != nil {
			return out, err
		}
		out.Metadata = t.MetadataBytes()
		storage, err := parseType(t.StorageDtype(nil), depth+1, budget)
		if err != nil {
			return out, err
		}
		out.Storage = &storage
		out.Nullable = storage.Nullable
	case fb.TypeList:
		var t fb.List
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "list"
		out.Nullable = t.Nullable()
		storage, err := parseType(t.ElementType(nil), depth+1, budget)
		if err != nil {
			return out, err
		}
		out.Storage = &storage
	case fb.TypeFixedSizeList:
		var t fb.FixedSizeList
		t.Init(tab.Bytes, tab.Pos)
		out.Kind = "fixed_size_list"
		out.Nullable = t.Nullable()
		if t.Size() > 1<<31-1 {
			return out, fmt.Errorf("%w: list size", ErrInvalid)
		}
		out.ListSize = int32(t.Size())
		storage, err := parseType(t.ElementType(nil), depth+1, budget)
		if err != nil {
			return out, err
		}
		out.Storage = &storage
	default:
		return out, fmt.Errorf("%w: dtype %d", ErrUnsupported, v.TypeType())
	}
	return out, nil
}
