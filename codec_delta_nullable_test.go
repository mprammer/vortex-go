// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package vortex

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/mprammer/vortex-go/internal/encoding"
)

func TestRustNullableDeltaSourceValues(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end int
	}{
		{"delta_nullable", 0, 2500},
		{"delta_nullable_slice", 1000, 1050},
		{"delta_nullable_late_slice", 1023, 2051},
		{"delta_nullable_tail", 2400, 2500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := openFixture(t, tc.name)
			if f.NumRows() != int64(tc.end-tc.start) {
				t.Fatalf("row count %d", f.NumRows())
			}
			names := []string{"row_id", "v32", "v64", "required", "all_valid", "all_null"}
			if f.Schema().NumFields() != len(names) {
				t.Fatalf("column count %d", f.Schema().NumFields())
			}
			for i, name := range names {
				if f.Schema().Field(i).Name != name {
					t.Fatalf("column %d: expected %s", i, name)
				}
			}
			// Keep the fixture encoded and physically sliced. Canonicalized
			// replacements would no longer exercise transposed delta validity.
			for column := 1; column < f.Schema().NumFields(); column++ {
				node := deltaFixtureArray(t, f, column)
				if node.Encoding != "fastlanes.delta" {
					t.Fatalf("column %d: expected delta, got %s", column, node.Encoding)
				}
				metadata := wireUnsignedFields(t, node.Metadata)
				count := ((tc.end+1023)/1024 - tc.start/1024) * 1024
				if metadata[1] != uint64(count) || metadata[2] != uint64(tc.start%1024) {
					t.Fatalf("column %d: unexpected delta storage metadata %v", column, metadata)
				}
			}
			r, err := f.NewRecordReader(t.Context(), ScanOptions{BatchSize: 127})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			rows := 0
			for r.Next() {
				batch := r.RecordBatch()
				if r.RowOffset() != int64(rows) {
					t.Fatalf("physical offset %d, expected %d", r.RowOffset(), rows)
				}
				for column, field := range batch.Schema().Fields() {
					a := batch.Column(column)
					for i := 0; i < a.Len(); i++ {
						row := tc.start + rows + i
						valid := true
						switch field.Name {
						case "v32":
							valid = row%3 != 0
						case "v64":
							valid = row%5 != 2 && (row < 1001 || row >= 1041)
						case "all_null":
							valid = false
						}
						if a.IsValid(i) != valid {
							t.Fatalf("%s source row %d: validity %v, expected %v", field.Name, row, a.IsValid(i), valid)
						}
						if !valid {
							continue
						}
						var got, want int64
						switch field.Name {
						case "row_id":
							got, want = a.(*array.Int64).Value(i), int64(row)
						case "v64":
							got, want = a.(*array.Int64).Value(i), math.MinInt64+int64(row)*19
						case "v32", "required", "all_valid":
							got, want = int64(a.(*array.Int32).Value(i)), int64(row)
						default:
							t.Fatalf("unexpected valid column %s", field.Name)
						}
						if got != want {
							t.Fatalf("%s source row %d: got %d, expected %d", field.Name, row, got, want)
						}
					}
				}
				rows += int(batch.NumRows())
			}
			if r.Err() != nil {
				t.Fatal(r.Err())
			}
			if rows != tc.end-tc.start {
				t.Fatalf("read %d rows, expected %d", rows, tc.end-tc.start)
			}
		})
	}
}

func deltaFixtureArray(t *testing.T, f *File, column int) *encoding.Node {
	t.Helper()
	plan := &columnPlan{}
	if err := buildFieldPlan(f.root, f.dtype, column, 0, plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.pieces) != 1 {
		t.Fatal("expected one physical piece")
	}
	leaf := plan.pieces[0].node
	if leaf.id != "vortex.flat" || len(leaf.segments) != 1 {
		t.Fatal("expected one flat segment")
	}
	return physicalArrayNode(t, f, leaf)
}
