// SPDX-License-Identifier: Apache-2.0

package vortex

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/mprammer/vortex-go/internal/encoding"
)

func TestDictionaryPruningUsesLogicalValueZones(t *testing.T) {
	for _, outerZones := range []bool{false, true} {
		name := "code_zones_only"
		if outerZones {
			name = "enclosing_value_zones"
		}
		t.Run(name, func(t *testing.T) {
			f, _ := openFixture(t, "zoned_ints")
			want, wantValid, _ := collectColumn(t, f, "v64", nil)
			codesIndex, valuesIndex := -1, -1
			for i, field := range f.dtype.Fields {
				switch field.Name {
				case "row_id":
					codesIndex = i
				case "v64":
					valuesIndex = i
				}
			}
			if f.root.id != "vortex.struct" || codesIndex < 0 || valuesIndex < 0 {
				t.Fatal("unexpected source fixture layout")
			}
			typ := f.dtype.Fields[valuesIndex].Type
			values := f.root.children[valuesIndex]
			// The Rust fixture's row IDs provide signed identity codes. Its v64 values
			// contain nulls, negative extremes, repeated 7s, and positive extremes, so
			// code statistics cannot describe the dictionary's logical values.
			dict := &layout{id: "vortex.dict", rows: f.NumRows(), metadata: []byte{8, 7, 16, 0}, children: []*layout{values, f.root.children[codesIndex]}}
			data := dict
			if outerZones {
				if values.id != "vortex.zoned" || len(values.children) != 2 {
					t.Fatal("fixture missing logical value zones")
				}
				outer := *values
				outer.children = []*layout{dict, values.children[1]}
				data = &outer
			}
			f.root = &layout{id: "vortex.struct", rows: dict.rows, children: []*layout{data}}
			f.dtype = encoding.Type{Kind: "struct", Fields: []encoding.Field{{Name: "value", Type: typ}}}
			dt, err := typ.ArrowType()
			if err != nil {
				t.Fatal(err)
			}
			f.schema = arrow.NewSchema([]arrow.Field{{Name: "value", Type: dt, Nullable: typ.Nullable}}, nil)

			for _, tc := range []struct {
				name string
				op   CompareOp
			}{
				{"eq", OpEQ}, {"neq", OpNEQ}, {"gt", OpGT}, {"gte", OpGTE}, {"lt", OpLT}, {"lte", OpLTE},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, valid, positions := collectColumn(t, f, "value", []Predicate{{Column: "value", Op: tc.op, Value: 7}})
					seen := make(map[int64]bool, len(positions))
					for i, pos := range positions {
						if pos < 0 || pos >= int64(len(want)) || seen[pos] {
							t.Fatalf("invalid physical position %d", pos)
						}
						seen[pos] = true
						if valid[i] != wantValid[pos] || valid[i] && got[i] != want[pos] {
							t.Fatalf("dictionary changed physical row %d", pos)
						}
					}
					matches := 0
					for row, value := range want {
						match := tc.op == OpNEQ
						if wantValid[row] {
							switch tc.op {
							case OpEQ:
								match = value == 7
							case OpNEQ:
								match = value != 7
							case OpGT:
								match = value > 7
							case OpGTE:
								match = value >= 7
							case OpLT:
								match = value < 7
							case OpLTE:
								match = value <= 7
							}
						}
						if match {
							matches++
							if !seen[int64(row)] {
								t.Errorf("lost matching physical row %d (value=%d, valid=%t)", row, value, wantValid[row])
								break
							}
						}
					}
					if matches == 0 {
						t.Fatal("fixture has no matches")
					}
					if outerZones && tc.op == OpEQ && len(got) >= len(want) {
						t.Fatal("enclosing logical value zones no longer prune")
					}
					if !outerZones && len(got) != len(want) {
						t.Errorf("code-only statistics pruned %d rows", len(want)-len(got))
					}
				})
			}
		})
	}
}
