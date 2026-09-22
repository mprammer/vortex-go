// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package vortex

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// These input formulas were authored independently for the owned Rust corpus.
// Unsigned expectations stay unsigned, including values above MaxInt64.
func TestOwnedDeltaAndTimestampSourceValues(t *testing.T) {
	f, _ := openFixture(t, "owned_temporal_delta")
	fields := []arrow.Field{
		{Name: "u16_delta", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "u32_delta", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "u64_delta", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "clock_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{Name: "clock_ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
	}
	if f.NumRows() != 3079 || !f.Schema().Equal(arrow.NewSchema(fields, nil)) {
		t.Fatalf("unexpected temporal fixture shape: %d rows, %s", f.NumRows(), f.Schema())
	}
	for _, batchSize := range []int{127, 1024} {
		t.Run(fmt.Sprint(batchSize), func(t *testing.T) {
			r, err := f.NewRecordReader(context.Background(), ScanOptions{BatchSize: batchSize})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			var row int64
			for r.Next() {
				batch := r.RecordBatch()
				if r.RowOffset() != row {
					t.Fatalf("row offset %d, want %d", r.RowOffset(), row)
				}
				for i := range int(batch.NumRows()) {
					n := row + int64(i)
					for j, field := range batch.Schema().Fields() {
						a := batch.Column(j)
						if a.IsNull(i) {
							t.Fatalf("%s row %d unexpected null", field.Name, n)
						}
						var got, want any
						switch field.Name {
						case "u16_delta":
							got, want = a.(*array.Uint16).Value(i), uint16(1024+n*3)
						case "u32_delta":
							got, want = a.(*array.Uint32).Value(i), uint32(4000000+n*17)
						case "u64_delta":
							got, want = a.(*array.Uint64).Value(i), uint64(1<<63)+37+uint64(n)*1009
						case "clock_ns":
							got, want = a.(*array.Timestamp).Value(i), arrow.Timestamp(1653456789012345678+n*7000003)
						case "clock_ms":
							got, want = a.(*array.Timestamp).Value(i), arrow.Timestamp(-987654321+n*13003)
						default:
							t.Fatalf("unexpected field %s", field.Name)
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("%s row %d: got %v want %v", field.Name, n, got, want)
						}
					}
				}
				row += batch.NumRows()
			}
			if r.Err() != nil {
				t.Fatal(r.Err())
			}
			if row != 3079 {
				t.Fatalf("read %d rows, want 3079", row)
			}
		})
	}
}

// Compare raw IEEE bits so NaN payloads, signed zero, and subnormals remain
// covered when this module is tested without the Iceberg integration suite.
func TestOwnedALPFlatSourceValues(t *testing.T) {
	f, _ := openFixture(t, "owned_alp_flat")
	fields := []arrow.Field{
		{Name: "single", Type: arrow.PrimitiveTypes.Float32},
		{Name: "double", Type: arrow.PrimitiveTypes.Float64},
		{Name: "nullable_single", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "nullable_double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	if f.NumRows() != 3091 || !f.Schema().Equal(arrow.NewSchema(fields, nil)) {
		t.Fatalf("unexpected ALP fixture shape: %d rows, %s", f.NumRows(), f.Schema())
	}
	r, err := f.NewRecordReader(t.Context(), ScanOptions{BatchSize: 127})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	row := 0
	for r.Next() {
		batch := r.RecordBatch()
		if r.RowOffset() != int64(row) {
			t.Fatalf("row offset %d, want %d", r.RowOffset(), row)
		}
		for col, field := range fields {
			a := batch.Column(col)
			for i := range a.Len() {
				n := row + i
				valid := !field.Nullable || (n%23 != 11 && (n < 1800 || n >= 1831))
				if a.IsValid(i) != valid {
					t.Fatalf("%s row %d validity", field.Name, n)
				}
				if !valid {
					continue
				}
				var got uint64
				switch a := a.(type) {
				case *array.Float32:
					got = uint64(math.Float32bits(a.Value(i)))
				case *array.Float64:
					got = math.Float64bits(a.Value(i))
				default:
					t.Fatalf("unexpected ALP column %s: %T", field.Name, a)
				}
				want := ownedALPSourceBits(n, field.Type.ID() == arrow.FLOAT32)
				if got != want {
					t.Fatalf("%s row %d: bits %x, want %x", field.Name, n, got, want)
				}
			}
		}
		row += int(batch.NumRows())
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if row != 3091 {
		t.Fatalf("read %d rows, want 3091", row)
	}
}

func ownedALPSourceBits(row int, f32 bool) uint64 {
	positions := [...]int{2, 7, 19, 509, 1022, 1024, 1537, 2047, 2048, 3089}
	bits32 := [...]uint32{0x80000000, 0x7f800000, 0xff800000, 0x7fc02468, 0xffc01357, 0x00000001, 0x80000001, 0x7f7fffff, 0x00800000, 0x3e4ccccd}
	bits64 := [...]uint64{0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000002468, 0xfff8000000001357, 0x0000000000000001, 0x8000000000000001, 0x7fefffffffffffff, 0x0010000000000000, 0x3fc999999999999a}
	for i, position := range positions {
		if row == position {
			if f32 {
				return uint64(bits32[i])
			}
			return bits64[i]
		}
	}
	if f32 {
		return uint64(math.Float32bits(float32(row*3 - 4600)))
	}
	return math.Float64bits(float64(row*3 - 4600))
}
