// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/encoding/protowire"
	"testing"
)

func findLayout(root *layout, predicate func(*layout) bool) *layout {
	if predicate(root) {
		return root
	}
	for _, c := range root.children {
		if l := findLayout(c, predicate); l != nil {
			return l
		}
	}
	return nil
}
func collectColumn(t *testing.T, f *File, column string, predicates []Predicate) ([]int64, []bool, []int64) {
	t.Helper()
	rr, err := f.NewRecordReader(context.Background(), ScanOptions{Columns: []string{column}, BatchSize: 113, Predicates: predicates})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	var values []int64
	var valid []bool
	var physical []int64
	for rr.Next() {
		a := rr.RecordBatch().Column(0)
		for i := 0; i < a.Len(); i++ {
			v, ok := signedValue(a, i)
			values = append(values, v)
			valid = append(valid, ok)
			physical = append(physical, rr.RowOffset()+int64(i))
		}
	}
	if rr.Err() != nil {
		t.Fatal(rr.Err())
	}
	return values, valid, physical
}
func TestOptionalZoneMetadataFallsBack(t *testing.T) {
	mutations := map[string]func(*File, *layout){
		"version":                func(_ *File, z *layout) { z.metadata = []byte{2, 0} },
		"unknown_metadata_field": func(_ *File, z *layout) { z.metadata = append(z.metadata, 24, 1) },
		"truncated_proto":        func(_ *File, z *layout) { z.metadata = []byte{1, 8} },
		"unknown_aggregate":      func(_ *File, z *layout) { z.metadata = zoneMetadata(1024, "vortex.unknown", nil) },
		"unknown_options":        func(_ *File, z *layout) { z.metadata = zoneMetadata(1024, "vortex.min", []byte{16, 1}) },
		"zone_count":             func(_ *File, z *layout) { z.children[1].rows++ },
		"unknown_stats_layout":   func(_ *File, z *layout) { z.children[1].id = "vortex.unknown" },
		"truncated_stats_array": func(f *File, z *layout) {
			leaf := findLayout(z.children[1], func(l *layout) bool { return l.id == "vortex.flat" })
			if leaf == nil {
				t.Fatal("no flat stats leaf")
			}
			f.segments[leaf.segments[0]].length = 0
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f, _ := openFixture(t, "zoned_ints")
			column := ""
			var target *layout
			for i, field := range f.dtype.Fields {
				if field.Type.Kind != "i32" && field.Type.Kind != "i64" {
					continue
				}
				plan := &columnPlan{}
				if err := buildFieldPlan(f.root, f.dtype, i, 0, plan); err != nil {
					t.Fatal(err)
				}
				if len(plan.zones) > 0 {
					column = field.Name
					stats := plan.zones[0].node
					target = findLayout(f.root, func(l *layout) bool {
						return len(l.children) == 2 && l.children[1] == stats && (l.id == "vortex.zoned" || l.id == "vortex.stats")
					})
					break
				}
			}
			if target == nil {
				t.Fatal("fixture missing integer zone map")
			}
			values, valid, physical := collectColumn(t, f, column, nil)
			mutate(f, target)
			got, gvalid, gphysical := collectColumn(t, f, column, []Predicate{{Column: column, Op: OpEQ, Value: 1 << 62}})
			if len(got) != len(values) {
				t.Fatalf("invalid optional statistics pruned rows: got %d expected %d", len(got), len(values))
			}
			for i := range got {
				if got[i] != values[i] || gvalid[i] != valid[i] || gphysical[i] != physical[i] {
					t.Fatalf("fallback changed row %d", i)
				}
			}
		})
	}
}
func zoneMetadata(length uint64, id string, options []byte) []byte {
	spec := protowire.AppendTag(nil, 1, protowire.BytesType)
	spec = protowire.AppendString(spec, id)
	spec = protowire.AppendTag(spec, 2, protowire.BytesType)
	spec = protowire.AppendBytes(spec, options)
	out := protowire.AppendTag([]byte{1}, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, length)
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	return protowire.AppendBytes(out, spec)
}
func TestPruningPreservesNullsForNotEqual(t *testing.T) {
	f, _ := openFixture(t, "zoned_ints")
	for i, field := range f.dtype.Fields {
		if !field.Type.Nullable || (field.Type.Kind != "i32" && field.Type.Kind != "i64") {
			continue
		}
		plan := &columnPlan{}
		if err := buildFieldPlan(f.root, f.dtype, i, 0, plan); err != nil {
			t.Fatal(err)
		}
		if len(plan.zones) == 0 {
			continue
		}
		_, valid, physical := collectColumn(t, f, field.Name, nil)
		_, filtered, positions := collectColumn(t, f, field.Name, []Predicate{{Column: field.Name, Op: OpNEQ, Value: 0}})
		seen := map[int64]bool{}
		for j, pos := range positions {
			if !filtered[j] {
				seen[pos] = true
			}
		}
		for j, pos := range physical {
			if !valid[j] && !seen[pos] {
				t.Fatalf("null at physical row %d lost", pos)
			}
		}
		return
	}
	t.Fatal("fixture missing nullable integer zones")
}
func TestZonePredicatesAreConservative(t *testing.T) {
	f, _ := openFixture(t, "zoned_ints")
	column := f.Schema().Field(0).Name
	values, valid, _ := collectColumn(t, f, column, nil)
	for _, op := range []CompareOp{OpEQ, OpNEQ, OpGT, OpGTE, OpLT, OpLTE} {
		_, _, positions := collectColumn(t, f, column, []Predicate{{Column: column, Op: op, Value: 2048}})
		seen := make(map[int64]bool, len(positions))
		for _, pos := range positions {
			seen[pos] = true
		}
		for row, value := range values {
			matches := false
			if !valid[row] {
				matches = op == OpNEQ
			} else {
				switch op {
				case OpEQ:
					matches = value == 2048
				case OpNEQ:
					matches = value != 2048
				case OpGT:
					matches = value > 2048
				case OpGTE:
					matches = value >= 2048
				case OpLT:
					matches = value < 2048
				case OpLTE:
					matches = value <= 2048
				}
			}
			if matches && !seen[int64(row)] {
				t.Fatalf("operation %d lost matching row %d", op, row)
			}
		}
	}
	// Verify that returned arrays use Arrow's canonical scalar representation.
	rr, err := f.NewRecordReader(context.Background(), ScanOptions{Columns: []string{column}})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	if !rr.Next() {
		t.Fatal(rr.Err())
	}
	switch rr.RecordBatch().Column(0).(type) {
	case *array.Int32, *array.Int64:
	default:
		t.Fatal("integer fixture type")
	}
}
