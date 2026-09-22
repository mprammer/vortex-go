// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/mprammer/vortex-go/internal/encoding"
)

func TestLayoutDictionaryNullability(t *testing.T) {
	for _, tc := range []struct {
		name, codes string
		nullable    bool
	}{
		{"required_null_codes", "v32", false},
		{"required_all_valid_codes", "all_valid", false},
		{"nullable_null_codes", "v32", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := openFixture(t, "delta_nullable")
			if f.root.id != "vortex.struct" {
				t.Fatal("expected struct fixture")
			}
			values, codes := f.schema.FieldIndices("row_id"), f.schema.FieldIndices(tc.codes)
			if len(values) != 1 || len(codes) != 1 {
				t.Fatal("missing fixture columns")
			}
			typ := encoding.Type{Kind: "i64", Nullable: tc.nullable}
			// Rust row IDs supply identity dictionary values. Both code columns
			// have nullable i32 dtype, but only v32 has nulls (every third row).
			dict := &layout{id: "vortex.dict", rows: f.NumRows(), metadata: []byte{8, 6, 16, 1}, children: []*layout{f.root.children[values[0]], f.root.children[codes[0]]}}
			f.root = &layout{id: "vortex.struct", rows: dict.rows, children: []*layout{dict}}
			f.dtype = encoding.Type{Kind: "struct", Fields: []encoding.Field{{Name: "value", Type: typ}}}
			f.schema = arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: tc.nullable}}, nil)
			invalid := !tc.nullable && tc.codes == "v32"
			check := func(t *testing.T, a arrow.Array, offset int) {
				t.Helper()
				for i := 0; i < a.Len(); i++ {
					row := offset + i
					valid := tc.codes != "v32" || row%3 != 0
					if a.IsValid(i) != valid || valid && a.(*array.Int64).Value(i) != int64(row) {
						t.Fatalf("row %d: validity or identity value changed", row)
					}
				}
			}
			for _, mode := range []string{"streaming", "auxiliary"} {
				t.Run(mode, func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer mem.AssertSize(t, 0)
					ctx := compute.WithAllocator(t.Context(), mem)
					if mode == "auxiliary" {
						a, err := f.decodeLayout(ctx, dict, typ)
						if a != nil {
							defer a.Release()
						}
						if invalid {
							if a != nil || !errors.Is(err, ErrInvalid) {
								t.Fatalf("required dictionary: returned array=%v, error=%v", a != nil, err)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						if int64(a.Len()) != f.NumRows() {
							t.Fatal("wrong row count")
						}
						check(t, a, 0)
						return
					}
					r, err := f.NewRecordReader(ctx, ScanOptions{BatchSize: 113})
					if err != nil {
						t.Fatal(err)
					}
					defer r.Release()
					if invalid {
						if r.Next() || r.RecordBatch() != nil || !errors.Is(r.Err(), ErrInvalid) {
							t.Fatalf("required dictionary: emitted=%v, error=%v", r.RecordBatch() != nil, r.Err())
						}
						if r.Next() || !errors.Is(r.Err(), ErrInvalid) {
							t.Fatal("invalid dictionary reader resumed")
						}
						mem.AssertSize(t, 0) // Failure must release caches before Release.
						return
					}
					rows := 0
					for r.Next() {
						check(t, r.RecordBatch().Column(0), rows)
						rows += int(r.RecordBatch().NumRows())
					}
					if r.Err() != nil || int64(rows) != f.NumRows() {
						t.Fatalf("rows=%d, error=%v", rows, r.Err())
					}
				})
			}
		})
	}
}

type cancelAllocator struct {
	memory.Allocator
	cancel    context.CancelFunc
	remaining int
}

func (a *cancelAllocator) allocating() {
	a.remaining--
	if a.remaining == 0 {
		a.cancel()
	}
}
func (a *cancelAllocator) Allocate(n int) []byte {
	a.allocating()
	return a.Allocator.Allocate(n)
}
func (a *cancelAllocator) Reallocate(n int, b []byte) []byte {
	a.allocating()
	return a.Allocator.Reallocate(n, b)
}

func TestCancellationDuringMaterialization(t *testing.T) {
	for _, columns := range [][]string{{"row_id"}, {"row_id", "required"}} {
		t.Run(columns[len(columns)-1], func(t *testing.T) {
			f, _ := openFixture(t, "delta_nullable")
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Required columns each materialize one Arrow values buffer. With
			// two columns, cancellation must also release the first column.
			alloc := &cancelAllocator{Allocator: mem, cancel: cancel, remaining: len(columns)}
			ctx = compute.WithAllocator(ctx, alloc)
			r, err := f.NewRecordReader(ctx, ScanOptions{Columns: columns})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			if r.Next() || r.RecordBatch() != nil || !errors.Is(r.Err(), context.Canceled) {
				t.Fatalf("canceled materialization: emitted=%v, error=%v, context=%v", r.RecordBatch() != nil, r.Err(), ctx.Err())
			}
			if r.Next() || r.RecordBatch() != nil || !errors.Is(r.Err(), context.Canceled) {
				t.Fatal("canceled reader resumed")
			}
			mem.AssertSize(t, 0) // Failure must release arrays and caches immediately.
		})
	}
}
