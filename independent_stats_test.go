// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 the vortex-go contributors.
package vortex

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

const ownedStatsZone = 257
const ownedStatsRows = 2*ownedStatsZone + 7

func ownedStatsOriginal(row int) (int64, bool) {
	if row < ownedStatsZone {
		return -900 + int64(row)*3, row%17 != 4
	}
	if row < 2*ownedStatsZone {
		return 0, false
	}
	return (1 << 53) + 17 + int64(row-2*ownedStatsZone)*11, row != ownedStatsRows-2
}

func TestOwnedLegacyStatsPhysicalRepresentation(t *testing.T) {
	f, _ := openFixture(t, "owned_legacy_stats")
	if f.NumRows() != ownedStatsRows || f.root.id != "vortex.struct" || len(f.dtype.Fields) != 3 {
		t.Fatal("legacy stats fixture shape changed")
	}
	for column, name := range []string{"row_id", "exact", "inexact"} {
		if f.dtype.Fields[column].Name != name || f.dtype.Fields[column].Type.Kind != "i64" || f.dtype.Fields[column].Type.Nullable != (column != 0) {
			t.Fatalf("unexpected schema field %d: %+v", column, f.dtype.Fields[column])
		}
		if column == 0 {
			continue
		}
		l := f.root.children[column]
		if l.id != "vortex.stats" || !bytes.Equal(l.metadata, []byte{1, 1, 0, 0, 0x58, 0}) || len(l.children) != 2 || l.children[1].rows != 3 {
			t.Fatalf("%s lost legacy statistics metadata", name)
		}
		plan := &columnPlan{}
		if err := buildFieldPlan(f.root, f.dtype, column, 0, plan); err != nil {
			t.Fatal(err)
		}
		if len(plan.zones) != 1 || len(plan.pieces) != 3 {
			t.Fatalf("%s expected one zone map and three data chunks", name)
		}
		for zone, piece := range plan.pieces {
			if piece.start != int64(zone*ownedStatsZone) || piece.end != int64(min((zone+1)*ownedStatsZone, ownedStatsRows)) {
				t.Fatalf("%s zone %d physical boundaries changed", name, zone)
			}
		}
		z := plan.zones[0]
		if z.zoneLen != ownedStatsZone || z.maxField != 0 || z.maxTruncated != 1 || z.minField != 2 || z.minTruncated != 3 || z.nullField != 4 {
			t.Fatalf("%s legacy statistics schema changed", name)
		}
		a, err := f.decodeLayout(t.Context(), z.node, z.dtype)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Release()
		s := a.(*array.Struct)
		for zone := range 3 {
			var lo, hi int64
			nulls, nonnulls := uint64(0), 0
			for row := zone * ownedStatsZone; row < min((zone+1)*ownedStatsZone, ownedStatsRows); row++ {
				value, valid := ownedStatsOriginal(row)
				if !valid {
					nulls++
					continue
				}
				if nonnulls == 0 || value < lo {
					lo = value
				}
				if nonnulls == 0 || value > hi {
					hi = value
				}
				nonnulls++
			}
			for _, bound := range []struct {
				field int
				want  int64
			}{{z.minField, lo}, {z.maxField, hi}} {
				v := s.Field(bound.field).(*array.Int64)
				if v.IsNull(zone) != (nonnulls == 0) || (nonnulls != 0 && v.Value(zone) != bound.want) {
					t.Fatalf("%s zone %d bound %d", name, zone, bound.field)
				}
			}
			for _, index := range []int{z.minTruncated, z.maxTruncated} {
				v := s.Field(index).(*array.Boolean)
				if v.IsNull(zone) || v.Value(zone) != (name == "inexact") {
					t.Fatalf("%s zone %d truncation flag", name, zone)
				}
			}
			v := s.Field(z.nullField).(*array.Uint64)
			if v.IsNull(zone) || v.Value(zone) != nulls {
				t.Fatalf("%s zone %d null count", name, zone)
			}
		}
	}
}

// Every emitted value is checked against the independent source formula. The
// predicate selects whole zones, so returned offsets can jump across skipped data.
func scanOwnedStats(t *testing.T, f *File, column string, predicates []Predicate, zones []int) {
	t.Helper()
	wantRows := make([]int, 0, ownedStatsRows)
	for _, zone := range zones {
		for row := zone * ownedStatsZone; row < min((zone+1)*ownedStatsZone, ownedStatsRows); row++ {
			wantRows = append(wantRows, row)
		}
	}
	r, err := f.NewRecordReader(context.Background(), ScanOptions{Columns: []string{"row_id", column}, BatchSize: 31, Predicates: predicates})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	rows := 0
	for r.Next() {
		b := r.RecordBatch()
		ids := b.Column(0).(*array.Int64)
		values := b.Column(1).(*array.Int64)
		for i := range values.Len() {
			if rows >= len(wantRows) {
				t.Fatal("unexpected extra row")
			}
			row := wantRows[rows]
			if r.RowOffset()+int64(i) != int64(row) || ids.IsNull(i) || ids.Value(i) != int64(row) {
				t.Fatalf("emitted row %d physical offset/id, expected %d", rows, row)
			}
			want, valid := ownedStatsOriginal(row)
			if values.IsValid(i) != valid || (valid && values.Value(i) != want) {
				t.Fatalf("%s row %d original value/validity", column, row)
			}
			rows++
		}
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != len(wantRows) {
		t.Fatalf("emitted %d rows, expected %d", rows, len(wantRows))
	}
}

func TestOwnedLegacyStatsValuesAndPruning(t *testing.T) {
	cases := []struct {
		name, column string
		predicates   []Predicate
		zones        []int
	}{
		{"full_exact", "exact", nil, []int{0, 1, 2}},
		{"full_inexact", "inexact", nil, []int{0, 1, 2}},
		{"equal_above_float_precision", "exact", []Predicate{{Column: "exact", Op: OpEQ, Value: (1 << 53) + 17}}, []int{2}},
		{"less_than_zero", "exact", []Predicate{{Column: "exact", Op: OpLT, Value: 0}}, []int{0}},
		{"greater_than_zero", "exact", []Predicate{{Column: "exact", Op: OpGT, Value: 0}}, []int{2}},
		{"all_pruned", "exact", []Predicate{{Column: "exact", Op: OpEQ, Value: math.MaxInt64}}, nil},
		{"not_equal_preserves_nulls", "exact", []Predicate{{Column: "exact", Op: OpNEQ, Value: 0}}, []int{0, 1, 2}},
		{"inexact_bounds_fall_back", "inexact", []Predicate{{Column: "inexact", Op: OpEQ, Value: math.MaxInt64}}, []int{0, 2}},
		{"inexact_not_equal_preserves_nulls", "inexact", []Predicate{{Column: "inexact", Op: OpNEQ, Value: 0}}, []int{0, 1, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := openFixture(t, "owned_legacy_stats")
			scanOwnedStats(t, f, tc.column, tc.predicates, tc.zones)
		})
	}
	// Corrupt optional legacy metadata while retaining valid data segments.
	t.Run("truncated_metadata_falls_back", func(t *testing.T) {
		f, _ := openFixture(t, "owned_legacy_stats")
		f.root.children[1].metadata = []byte{1, 1, 0}
		scanOwnedStats(t, f, "exact", []Predicate{{Column: "exact", Op: OpEQ, Value: math.MaxInt64}}, []int{0, 1, 2})
	})
}

func TestOwnedLegacyStatsPruningAvoidsDataReads(t *testing.T) {
	_, src := openFixture(t, "owned_legacy_stats")
	stat, err := src.Stat()
	if err != nil {
		t.Fatal(err)
	}
	measure := func(predicates []Predicate, zones []int) (int, int) {
		tracked := &trackedSource{src: src}
		f, err := Open(t.Context(), tracked, stat.Size())
		if err != nil {
			t.Fatal(err)
		}
		beforeReads, beforeBytes := tracked.reads, tracked.bytes
		scanOwnedStats(t, f, "exact", predicates, zones)
		return tracked.reads - beforeReads, tracked.bytes - beforeBytes
	}
	fullReads, fullBytes := measure(nil, []int{0, 1, 2})
	prunedReads, prunedBytes := measure([]Predicate{{Column: "exact", Op: OpGT, Value: 0}}, []int{2})
	if prunedReads >= fullReads || prunedBytes >= fullBytes {
		t.Fatalf("legacy pruning did not avoid source reads: full=%d/%d pruned=%d/%d", fullReads, fullBytes, prunedReads, prunedBytes)
	}
}
