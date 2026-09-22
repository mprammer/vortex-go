// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.

// Package encoding decodes Vortex's recursive array encodings into Arrow arrays.
package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

type Kind string

type Type struct {
	Kind             Kind
	Nullable         bool
	Fields           []Field
	Storage          *Type
	Extension        string
	Metadata         []byte
	Precision, Scale int32
	ListSize         int32
}
type Field struct {
	Name string
	Type Type
}
type Node struct {
	Encoding string
	Metadata []byte
	Children []*Node
	Buffers  [][]byte
}

func (t Type) ArrowType() (arrow.DataType, error) {
	switch t.Kind {
	case "null":
		return arrow.Null, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "u8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "u16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "u32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "u64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "i8":
		return arrow.PrimitiveTypes.Int8, nil
	case "i16":
		return arrow.PrimitiveTypes.Int16, nil
	case "i32":
		return arrow.PrimitiveTypes.Int32, nil
	case "i64":
		return arrow.PrimitiveTypes.Int64, nil
	case "f16":
		return arrow.FixedWidthTypes.Float16, nil
	case "f32":
		return arrow.PrimitiveTypes.Float32, nil
	case "f64":
		return arrow.PrimitiveTypes.Float64, nil
	case "utf8":
		return arrow.BinaryTypes.String, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "decimal":
		if t.Precision < 1 || t.Precision > 76 || t.Scale < 0 || t.Scale > t.Precision {
			return nil, fmt.Errorf("invalid decimal precision %d scale %d", t.Precision, t.Scale)
		}
		if t.Precision > 38 {
			return &arrow.Decimal256Type{Precision: t.Precision, Scale: t.Scale}, nil
		}
		return &arrow.Decimal128Type{Precision: t.Precision, Scale: t.Scale}, nil
	case "struct":
		fields := make([]arrow.Field, len(t.Fields))
		for i, f := range t.Fields {
			dt, e := f.Type.ArrowType()
			if e != nil {
				return nil, e
			}
			fields[i] = arrow.Field{Name: f.Name, Type: dt, Nullable: f.Type.Nullable}
		}
		return arrow.StructOf(fields...), nil
	case "list", "fixed_size_list":
		if t.Storage == nil {
			return nil, fmt.Errorf("list has no element type")
		}
		dt, e := t.Storage.ArrowType()
		if e != nil {
			return nil, e
		}
		f := arrow.Field{Name: "item", Type: dt, Nullable: t.Storage.Nullable}
		if t.Kind == "list" {
			return arrow.ListOfField(f), nil
		}
		if t.ListSize < 0 {
			return nil, fmt.Errorf("negative list size")
		}
		return arrow.FixedSizeListOfField(t.ListSize, f), nil
	case "extension":
		if t.Storage == nil {
			return nil, fmt.Errorf("extension %q has no storage type", t.Extension)
		}
		if len(t.Metadata) < 1 {
			return nil, fmt.Errorf("extension %q has no metadata", t.Extension)
		}
		tag := t.Metadata[0]
		unit := arrow.Nanosecond
		switch tag {
		case 0:
			unit = arrow.Nanosecond
		case 1:
			unit = arrow.Microsecond
		case 2:
			unit = arrow.Millisecond
		case 3:
			unit = arrow.Second
		case 4:
		default:
			return nil, fmt.Errorf("invalid temporal unit %d", tag)
		}
		switch t.Extension {
		case "vortex.date":
			if len(t.Metadata) != 1 {
				return nil, fmt.Errorf("invalid date metadata length")
			}
			if tag == 4 && t.Storage.Kind == "i32" {
				return arrow.FixedWidthTypes.Date32, nil
			}
			if tag == 2 && t.Storage.Kind == "i64" {
				return arrow.FixedWidthTypes.Date64, nil
			}
		case "vortex.time":
			if len(t.Metadata) != 1 {
				return nil, fmt.Errorf("invalid time metadata length")
			}
			if tag >= 2 && tag <= 3 && t.Storage.Kind == "i32" {
				return &arrow.Time32Type{Unit: unit}, nil
			}
			if tag <= 1 && t.Storage.Kind == "i64" {
				return &arrow.Time64Type{Unit: unit}, nil
			}
		case "vortex.timestamp":
			if len(t.Metadata) < 3 || tag > 3 || t.Storage.Kind != "i64" {
				return nil, fmt.Errorf("invalid timestamp metadata/storage")
			}
			n := int(binary.LittleEndian.Uint16(t.Metadata[1:]))
			if len(t.Metadata) != 3+n {
				return nil, fmt.Errorf("truncated timestamp timezone")
			}
			tz := string(t.Metadata[3:])
			return &arrow.TimestampType{Unit: unit, TimeZone: tz}, nil
		default:
			return nil, fmt.Errorf("unsupported extension %q", t.Extension)
		}
		return nil, fmt.Errorf("invalid storage type or unit for %s", t.Extension)
	}
	return nil, fmt.Errorf("unsupported Vortex type %q", t.Kind)
}

func primitiveType(p uint64, nullable bool) (Type, error) {
	kinds := [...]Kind{"u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f16", "f32", "f64"}
	if p >= uint64(len(kinds)) {
		return Type{}, fmt.Errorf("invalid primitive type %d", p)
	}
	return Type{Kind: kinds[p], Nullable: nullable}, nil
}
func (t Type) width() int {
	switch t.Kind {
	case "u8", "i8", "bool":
		return 1
	case "u16", "i16", "f16":
		return 2
	case "u32", "i32", "f32":
		return 4
	case "u64", "i64", "f64":
		return 8
	}
	return 0
}
func (t Type) integer() bool {
	return len(t.Kind) > 0 && (t.Kind[0] == 'i' || t.Kind[0] == 'u') && t.width() != 0
}
func (t Type) signed() bool { return t.integer() && t.Kind[0] == 'i' }
func unsigned(t Type) Type {
	if t.signed() {
		t.Kind = "u" + t.Kind[1:]
	}
	return t
}
func signed(t Type) Type {
	if t.integer() {
		t.Kind = "i" + t.Kind[1:]
	}
	return t
}
